//! Tokio `AsyncRead` / `AsyncWrite` over the halves.
//!
//! Each poll asks the half whether it is ready (`poll_readable` /
//! `poll_writable`, which register the waker on `Pending`) and then tries
//! the operation once. Nothing here blocks: the wake arrives from the peer
//! directly (standalone) or from this process's doorbell driver (fleet).
//!
//! Every poll also spends one unit of Tokio's cooperative budget, so a
//! task draining a ring that is never empty (or filling one that is never
//! full) still yields to its neighbours on the worker.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{Endpoint, Error, ReadHalf, WriteHalf};

impl AsyncRead for ReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let coop = std::task::ready!(tokio::task::coop::poll_proceed(cx));
        loop {
            if let Err(error) = std::task::ready!(self.poll_readable(cx)) {
                return Poll::Ready(Err(error.into()));
            }
            match self.try_read(buf.initialize_unfilled()) {
                Ok(n) => {
                    coop.made_progress();
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                // Ready said yes and the ring said no: somebody consumed the
                // change in between. Ask again.
                Err(Error::WouldBlock) => continue,
                Err(error) => return Poll::Ready(Err(error.into())),
            }
        }
    }
}

impl AsyncWrite for WriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let coop = std::task::ready!(tokio::task::coop::poll_proceed(cx));
        loop {
            if let Err(error) = std::task::ready!(self.poll_writable(cx)) {
                return Poll::Ready(Err(error.into()));
            }
            match self.try_write(buf) {
                Err(Error::WouldBlock) => continue,
                other => {
                    coop.made_progress();
                    return Poll::Ready(other.map_err(io::Error::from));
                }
            }
        }
    }

    /// Writes commit as they happen; there is nothing buffered to push.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.finish().map_err(io::Error::from))
    }
}

impl AsyncRead for Endpoint {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl AsyncWrite for Endpoint {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.write).poll_shutdown(cx)
    }
}
