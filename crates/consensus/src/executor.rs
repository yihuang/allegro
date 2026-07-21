//! Forwards finalized blocks to the execution layer via fork-choice updates.
//!
//! In the full implementation, this actor receives finalized block digests
//! from the consensus engine and calls Reth's engine API to advance the
//! canonical chain.

use futures::channel::mpsc;
use tracing::info;

/// Commands sent to the executor actor.
#[derive(Debug)]
pub(crate) enum Command {
    /// A block was finalized at the given height.
    Finalized { height: u64, hash: alloy_primitives::B256 },
}

/// Mailbox for the executor actor.
#[derive(Clone)]
pub(crate) struct Mailbox {
    _sender: mpsc::UnboundedSender<Command>,
}

impl Mailbox {
    pub(crate) fn send(&self, cmd: Command) -> Result<(), mpsc::TrySendError<Command>> {
        self._sender.unbounded_send(cmd)
    }
}

/// Spawn a new executor actor and return its mailbox.
pub(crate) fn spawn_executor() -> Mailbox {
    let (tx, _rx) = mpsc::unbounded();
    info!("executor actor spawned (not yet processing messages)");
    Mailbox { _sender: tx }
}
