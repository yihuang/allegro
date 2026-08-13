//! In-process channel pair for single-node consensus testing.
//!
//! Uses `tokio::sync::mpsc` under the hood. Works on the tokio runtime.

use std::convert::Infallible;

use commonware_actor::{Feedback, Unreliable};
use commonware_cryptography::PublicKey as PublicKeyTrait;
use commonware_p2p::{CheckedSender, LimitedSender, Receiver, Recipients};
use commonware_runtime::{IoBuf, IoBufs};
use tokio::sync::mpsc::error::TrySendError;

/// Create a loopback channel pair for single-node testing.
pub fn loopback_channel<P: PublicKeyTrait + Clone + Send + 'static>(
    self_key: P,
    capacity: usize,
) -> (LoopbackSender<P>, LoopbackReceiver<P>) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    (LoopbackSender { self_key, tx }, LoopbackReceiver { rx })
}

// ── Sender ─────────────────────────────────────────────────

#[derive(Debug)]
pub struct LoopbackSender<P> {
    self_key: P,
    tx: tokio::sync::mpsc::Sender<(P, IoBuf)>,
}

impl<P: PublicKeyTrait + Clone + Send + 'static> LimitedSender for LoopbackSender<P> {
    type PublicKey = P;
    type Checked<'a>
        = LoopbackCheckedSender<P>
    where
        Self: 'a;

    fn check(
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
    tx: tokio::sync::mpsc::Sender<(P, IoBuf)>,
    sent_to: P,
}

impl<P: PublicKeyTrait + Clone + Send + 'static> CheckedSender for LoopbackCheckedSender<P> {
    type PublicKey = P;

    fn recipients(&self) -> Vec<P> {
        vec![self.sent_to.clone()]
    }

    /// Submission is non-blocking: `send` is synchronous now, so a full
    /// loopback buffer drops the message rather than applying backpressure.
    fn send(self, message: impl Into<IoBufs> + Send, _priority: bool) -> Unreliable<Feedback> {
        let bufs: IoBufs = message.into();
        let msg = bufs.coalesce();
        match self.tx.try_send((self.sent_to, msg)) {
            Ok(()) => Unreliable::new(Feedback::Ok),
            Err(TrySendError::Full(_)) => Unreliable::rejected(),
            Err(TrySendError::Closed(_)) => Unreliable::new(Feedback::Closed),
        }
    }
}

// ── Receiver ───────────────────────────────────────────────

#[derive(Debug)]
pub struct LoopbackReceiver<P> {
    rx: tokio::sync::mpsc::Receiver<(P, IoBuf)>,
}

impl<P: PublicKeyTrait + Clone + Send + 'static> Receiver for LoopbackReceiver<P> {
    type PublicKey = P;
    type Error = Infallible;

    async fn recv(&mut self) -> Result<(P, IoBuf), Self::Error> {
        Ok(self.rx.recv().await.expect("channel should not close"))
    }
}
