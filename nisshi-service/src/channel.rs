// Copyright ⓒ Solace Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use nisshi_sans_io::Frame;
use rama::{Layer, Service};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::Error;

/// A [`Layer`] receiving [`Frame`]s from a [`FrameReceiver`] channel
#[derive(Clone, Debug, Default)]
pub struct ChannelFrameLayer {
    cancellation: CancellationToken,
}

impl ChannelFrameLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for ChannelFrameLayer {
    type Service = ChannelFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            cancellation: self.cancellation.clone(),
        }
    }
}

/// A [`Service`] receiving [`Frame`]s from a [`FrameReceiver`] channel
#[derive(Clone, Debug, Default)]
pub struct ChannelFrameService<S> {
    inner: S,
    cancellation: CancellationToken,
}

/// A channel frame receiver
pub type FrameReceiver = mpsc::Receiver<(Frame, oneshot::Sender<Frame>)>;

impl<S> Service<FrameReceiver> for ChannelFrameService<S>
where
    S: Service<Frame, Output = Frame>,
    S::Error: From<Error>,
{
    type Output = ();
    type Error = S::Error;

    async fn serve(&self, mut req: FrameReceiver) -> Result<Self::Output, Self::Error> {
        loop {
            tokio::select! {
                Some((frame, tx)) = req.recv() => {
                    debug!(?frame, ?tx);

                    self.inner
                        .serve(frame)
                        .await
                        .and_then(|response| {
                            tx.send(response)
                                .map_err(|unsent| Error::UnableToSend(Box::new(unsent)))
                                .map_err(Into::into)
                        })?
                }

                cancelled = self.cancellation.cancelled() => {
                    debug!(?cancelled);
                    break;
                }
            }
        }

        Ok(())
    }
}

/// A channel frame sender
pub type FrameSender = mpsc::Sender<(Frame, oneshot::Sender<Frame>)>;

/// A [`Service`] sending [`Frame`]s over a [`FrameSender`] channel
#[derive(Clone, Debug)]
pub struct FrameChannelService {
    tx: FrameSender,
}

impl FrameChannelService {
    pub fn new(tx: FrameSender) -> Self {
        Self { tx }
    }
}

impl Service<Frame> for FrameChannelService {
    type Output = Frame;

    type Error = Error;

    async fn serve(&self, req: Frame) -> Result<Self::Output, Self::Error> {
        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx
            .send((req, resp_tx))
            .await
            .map_err(|send_error| Error::UnableToSend(Box::new(send_error.0.0)))?;

        resp_rx.await.map_err(Error::OneshotRecv)
    }
}

/// A bounded channel for sending and receiving frames
pub fn bounded_channel(buffer: usize) -> (FrameSender, FrameReceiver) {
    mpsc::channel::<(Frame, oneshot::Sender<Frame>)>(buffer)
}
