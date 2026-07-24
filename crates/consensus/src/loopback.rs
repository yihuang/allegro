//! In-process channel pair for single-node consensus testing.
//!
//! Uses `tokio::sync::mpsc` under the hood. Works on the tokio runtime.

use std::convert::Infallible;
use std::sync::Arc;

use commonware_cryptography::PublicKey as PublicKeyTrait;
use commonware_p2p::{CheckedSender, LimitedSender, Receiver, Recipients};
use commonware_runtime::{IoBuf, IoBufs};

/// Create a loopback channel pair for single-node testing.
pub fn loopback_channel<P: PublicKeyTrait + Clone + Send + 'static>(
    self_key: P,
    capacity: usize,
) -> (LoopbackSender<P>, LoopbackReceiver<P>) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    (
        LoopbackSender {
            self_key,
            tx: Arc::new(tokio::sync::Mutex::new(tx)),
        },
        LoopbackReceiver {
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
        },
    )
}

// ── Sender ─────────────────────────────────────────────────

#[derive(Debug)]
pub struct LoopbackSender<P> {
    self_key: P,
    tx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Sender<(P, IoBuf)>>>,
}

impl<P: PublicKeyTrait + Clone + Send + 'static> LimitedSender for LoopbackSender<P> {
    type PublicKey = P;
    type Checked<'a>
        = LoopbackCheckedSender<P>
    where
        Self: 'a;

    async fn check(
        &mut self,
        _recipients: Recipients<P>,
    ) -> Result<Self::Checked<'_>, std::time::SystemTime> {
        Ok(LoopbackCheckedSender {
            tx: self.tx.clone(),
            sent_to: self.self_key.clone(),
        })
    }
}

impl<P: PublicKeyTrait + Clone + Send + 'static> Clone for LoopbackSender<P> {
    fn clone(&self) -> Self {
        Self {
            self_key: self.self_key.clone(),
            tx: self.tx.clone(),
        }
    }
}

// ── CheckedSender ─────────────────────────────────────────

#[derive(Debug)]
pub struct LoopbackCheckedSender<P> {
    tx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Sender<(P, IoBuf)>>>,
    sent_to: P,
}

impl<P: PublicKeyTrait + Clone + Send + 'static> CheckedSender for LoopbackCheckedSender<P> {
    type PublicKey = P;
    type Error = Infallible;

    async fn send(
        self,
        message: impl Into<IoBufs> + Send,
        _priority: bool,
    ) -> Result<Vec<P>, Self::Error> {
        let bufs: IoBufs = message.into();
        let msg = bufs.coalesce();
        let tx = self.tx.lock().await;
        let _ = tx.send((self.sent_to.clone(), msg)).await;
        Ok(vec![self.sent_to])
    }
}

// ── Receiver ───────────────────────────────────────────────

#[derive(Debug)]
pub struct LoopbackReceiver<P> {
    rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<(P, IoBuf)>>>,
}

impl<P> Clone for LoopbackReceiver<P> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
        }
    }
}

impl<P: PublicKeyTrait + Clone + Send + 'static> Receiver for LoopbackReceiver<P> {
    type PublicKey = P;
    type Error = Infallible;

    async fn recv(&mut self) -> Result<(P, IoBuf), Self::Error> {
        let mut rx = self.rx.lock().await;
        Ok(rx.recv().await.expect("channel should not close"))
    }
}
