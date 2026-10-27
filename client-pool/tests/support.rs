use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;

use crate::{ClientManager, PoolStats};

#[derive(Default)]
pub(crate) struct Control {
    pub(crate) create_pending: AtomicBool,
    pub(crate) recycle_pending: AtomicBool,
    pub(crate) create_error: AtomicBool,
    pub(crate) recycle_error: AtomicBool,
    pub(crate) live: AtomicUsize,
    pub(crate) created: AtomicUsize,
    pub(crate) dropped: AtomicUsize,
}

pub(crate) struct Manager(pub(crate) Arc<Control>);
pub(crate) struct Client(Arc<Control>);

impl Drop for Client {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl ClientManager for Manager {
    type Client = Client;
    type Error = std::io::Error;

    async fn create(&self) -> Result<Client, Self::Error> {
        if self.0.create_pending.load(Ordering::SeqCst) {
            pending::<()>().await;
        }
        if self.0.create_error.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("test creation rejected"));
        }
        self.0.live.fetch_add(1, Ordering::SeqCst);
        self.0.created.fetch_add(1, Ordering::SeqCst);
        Ok(Client(self.0.clone()))
    }

    async fn recycle(
        &self,
        _: &mut Client,
    ) -> Result<(), Self::Error> {
        if self.0.recycle_pending.load(Ordering::SeqCst) {
            pending::<()>().await;
        }
        if self.0.recycle_error.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("test recycle rejected"));
        }
        Ok(())
    }
}

pub(crate) fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

pub(crate) fn ready<F: Future>(mut future: Pin<&mut F>) -> F::Output {
    match poll_once(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("operation unexpectedly needs another wake"),
    }
}

pub(crate) fn empty(stats: PoolStats) {
    assert_eq!(
        (
            stats.live,
            stats.idle,
            stats.leased,
            stats.creating,
            stats.in_flight,
            stats.waiting
        ),
        (0, 0, 0, 0, 0, 0)
    );
}
