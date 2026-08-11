//! `eth_getTransactions`, served from the ExEx-maintained index.
//!
//! The types mirror tempo's `crates/node/src/rpc/eth_ext` field for field: tempo declares
//! the method and answers `unimplemented`, so its schema is the entire specification.
//! Renaming a field here forks the API silently -- which is what tempo-e2e's wire tests
//! in `test_indexer.py` exist to catch.

use alloy_primitives::Address;
use futures::future::try_join_all;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_node_core::rpc::result::internal_rpc_err;
use reth_rpc_eth_api::{helpers::EthTransactions, EthApiTypes, RpcTransaction};
use serde::{Deserialize, Serialize};

use crate::store::{Filter, Order, Position, Reader};

/// Page size when the caller does not ask for one.
const DEFAULT_LIMIT: usize = 10;
/// Hard ceiling on a page, so one request cannot ask the node to serialize the world.
const MAX_LIMIT: usize = 100;

/// Sort direction. `sort.on` is accepted but not honoured -- see [`PaginationParams`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SortOrder {
    Asc,
    #[default]
    Desc,
}

impl From<SortOrder> for Order {
    fn from(order: SortOrder) -> Self {
        match order {
            SortOrder::Asc => Order::Ascending,
            SortOrder::Desc => Order::Descending,
        }
    }
}

/// Field sorting parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sort {
    /// The field to order by. Accepted and ignored: transactions have one total order,
    /// their chain position, and `blockNumber` only coarsens it. Unknown values fall back
    /// to chain order rather than erroring, so a client built for a newer node still
    /// works against an older one.
    pub on: String,
    /// The ordering direction.
    pub order: SortOrder,
}

/// Cursor-paginated request envelope, shared by every indexer endpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginationParams<Filters> {
    /// Opaque cursor from a previous `nextCursor`; absent starts at the first entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Which items to yield.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Filters>,
    /// Maximum items to return. Defaults to 10; clamped to 1..=100.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Ordering of the yielded items.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Sort>,
}

/// Which transactions to return. Unset fields do not constrain the result.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionsFilter {
    /// Filter by sender address.
    pub from: Option<Address>,
    /// Filter by recipient address.
    pub to: Option<Address>,
    /// Filter by transaction type.
    #[serde(rename = "type", default, with = "tx_type")]
    pub type_: Option<u8>,
}

/// `type` accepts a JSON number (`118`) or a quantity string (`"0x76"`), because tempo's
/// `TempoTxType` takes either and the e2e wire tests send the hex form.
mod tx_type {
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Option<u8>, s: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(v) => s.serialize_str(&format!("0x{v:x}")),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u8>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Num(u8),
            Text(String),
        }
        Ok(match Option::<Repr>::deserialize(d)? {
            None => None,
            Some(Repr::Num(v)) => Some(v),
            Some(Repr::Text(text)) => {
                let digits = text.strip_prefix("0x").unwrap_or(&text);
                Some(u8::from_str_radix(digits, 16).map_err(de::Error::custom)?)
            }
        })
    }
}

/// A page of transactions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionsResponse<T> {
    /// Cursor for the next page, or null when this is the last one.
    pub next_cursor: Option<String>,
    /// The transactions matching the query.
    pub transactions: Vec<T>,
}

#[rpc(server, namespace = "eth")]
pub trait IndexerApi<T: serde::Serialize + Clone> {
    /// Gets paginated transactions with flexible filtering and sorting.
    ///
    /// Uses cursor-based pagination for stable iteration through transactions.
    #[method(name = "getTransactions")]
    async fn transactions(
        &self,
        params: PaginationParams<TransactionsFilter>,
    ) -> RpcResult<TransactionsResponse<T>>;
}

/// The `eth_getTransactions` handler: index for selection, reth for the bodies.
#[derive(Debug, Clone)]
pub struct IndexerRpc<EthApi> {
    eth_api: EthApi,
    store: Reader,
}

impl<EthApi> IndexerRpc<EthApi> {
    pub const fn new(eth_api: EthApi, store: Reader) -> Self {
        Self { eth_api, store }
    }
}

#[async_trait::async_trait]
impl<EthApi> IndexerApiServer<RpcTransaction<EthApi::NetworkTypes>> for IndexerRpc<EthApi>
where
    EthApi: EthTransactions + EthApiTypes + 'static,
{
    async fn transactions(
        &self,
        params: PaginationParams<TransactionsFilter>,
    ) -> RpcResult<TransactionsResponse<RpcTransaction<EthApi::NetworkTypes>>> {
        let filters = params.filters.unwrap_or_default();
        let filter = Filter {
            from: filters.from,
            to: filters.to,
            tx_type: filters.type_,
        };
        let order: Order = params.sort.unwrap_or_default().order.into();
        // Floor of 1, not just a ceiling: a zero limit returns no rows, so it has no
        // last row to cut a cursor from, and the caller is told the page is final while
        // `has_more` says otherwise -- a walk that ends one page in.
        let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

        let after = params
            .cursor
            .as_deref()
            .map(|cursor| {
                Position::decode(cursor)
                    .ok_or_else(|| internal_rpc_err(format!("malformed cursor: {cursor}")))
            })
            .transpose()?;

        // One extra row tells us whether a further page exists. Counting instead would
        // promise a next page that turns out empty at an exact multiple of the limit.
        let mut found = self
            .store
            .query(&filter, after, order, limit.saturating_add(1))
            .map_err(|e| internal_rpc_err(format!("index query failed: {e}")))?;

        let has_more = found.len() > limit;
        found.truncate(limit);
        // The cursor names the last row returned, not the extra one peeked at.
        let next_cursor = found
            .last()
            .filter(|_| has_more)
            .map(|entry| entry.position.encode());

        // The lookups are independent, so overlap them instead of awaiting one at a
        // time -- a full page is up to 100. `try_join_all` keeps the rows in order.
        let sources = try_join_all(
            found
                .iter()
                .map(|entry| EthTransactions::transaction_by_hash(&self.eth_api, entry.hash)),
        )
        .await
        .map_err(|e| internal_rpc_err(format!("failed to load transaction: {e}")))?;

        let mut transactions = Vec::with_capacity(found.len());
        // The index can name a transaction reth has since pruned; skip it rather than
        // failing the whole page.
        for source in sources.into_iter().flatten() {
            let tx = source
                .into_transaction(self.eth_api.converter())
                .map_err(|e| internal_rpc_err(format!("failed to convert transaction: {e}")))?;
            transactions.push(tx);
        }

        Ok(TransactionsResponse {
            next_cursor,
            transactions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_accept_tempos_wire_shape() {
        // Exactly the envelope tempo-e2e's wire tests send.
        let json = serde_json::json!({
            "cursor": "00000000000000000001:0000000000",
            "filters": {
                "from": "0x0000000000000000000000000000000000000001",
                "to": "0x0000000000000000000000000000000000000002",
                "type": "0x76",
            },
            "limit": 25,
            "sort": {"on": "blockNumber", "order": "asc"},
        });
        let params: PaginationParams<TransactionsFilter> = serde_json::from_value(json).unwrap();
        let filters = params.filters.unwrap();
        assert_eq!(filters.type_, Some(0x76));
        assert_eq!(params.limit, Some(25));
        assert_eq!(params.sort.unwrap().order, SortOrder::Asc);
    }

    #[test]
    fn tx_type_accepts_both_spellings() {
        for (json, want) in [
            (serde_json::json!({"filters": {"type": "0x76"}}), 0x76),
            (serde_json::json!({"filters": {"type": 118}}), 118),
            (serde_json::json!({"filters": {"type": "0x2"}}), 2),
        ] {
            let params: PaginationParams<TransactionsFilter> =
                serde_json::from_value(json).unwrap();
            assert_eq!(params.filters.unwrap().type_, Some(want));
        }
    }

    #[test]
    fn tx_type_rejects_nonsense() {
        let json = serde_json::json!({"filters": {"type": "not-a-type"}});
        assert!(serde_json::from_value::<PaginationParams<TransactionsFilter>>(json).is_err());
    }

    #[test]
    fn empty_params_are_valid() {
        let params: PaginationParams<TransactionsFilter> =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(params.cursor.is_none());
        assert!(params.filters.is_none());
    }

    #[test]
    fn unknown_filter_fields_are_ignored() {
        // Forward compatibility: a client built against a newer schema must keep working.
        let json = serde_json::json!({"filters": {"fieldFromAFutureRelease": 1}});
        let params: PaginationParams<TransactionsFilter> = serde_json::from_value(json).unwrap();
        assert_eq!(params.filters.unwrap(), TransactionsFilter::default());
    }

    #[test]
    fn unknown_sort_field_is_accepted() {
        let json = serde_json::json!({"sort": {"on": "id", "order": "asc"}});
        let params: PaginationParams<TransactionsFilter> = serde_json::from_value(json).unwrap();
        assert_eq!(params.sort.unwrap().on, "id");
    }

    #[test]
    fn malformed_params_are_rejected() {
        for bad in [
            serde_json::json!({"limit": "ten"}),
            serde_json::json!({"filters": {"from": "notanaddress"}}),
            serde_json::json!({"sort": {"on": "blockNumber", "order": "sideways"}}),
        ] {
            assert!(
                serde_json::from_value::<PaginationParams<TransactionsFilter>>(bad.clone())
                    .is_err(),
                "should have rejected {bad}",
            );
        }
    }

    #[test]
    fn response_uses_camel_case_next_cursor() {
        let response = TransactionsResponse::<u8> {
            next_cursor: None,
            transactions: vec![],
        };
        let json = serde_json::to_value(&response).unwrap();
        assert!(json.get("nextCursor").is_some());
        assert!(json["nextCursor"].is_null());
    }
}
