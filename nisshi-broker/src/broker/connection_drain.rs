// Copyright (c) Solace Corporation
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

use tokio::sync::watch;

/// The handle that drains a broker's client connections while the broker keeps running.
/// [`Broker::connection_drain`](super::Broker::connection_drain) returns it.
///
/// A drained connection answers the request that it has started to read, and then closes. The
/// listener stays open, so the broker serves a client that connects again.
///
/// Apache Kafka has no such operation. A Metadata response leaves out a fenced Kafka broker
/// ([`getAliveBrokerNodes`]), and its clients connect to the brokers that remain. A Nisshi
/// broker names only itself in a Metadata response, at its advertised listener, so the response
/// cannot send a client to another broker, and a client keeps its connection until the broker
/// closes it. A program that runs several brokers behind one address drains a broker to send
/// its clients back through that address.
///
/// A client sees the connection end between two responses, as it does when a Kafka broker
/// closes an idle connection ([`maybeCloseOldestConnection`]). The broker does not read a
/// request that the client sent behind the one in progress, so that request has no effect and
/// gets no answer. The client sends it again on its next connection, except a Produce with
/// `acks=0`: the client expects no answer to it, so it does not learn that the broker never
/// read it.
///
/// [`getAliveBrokerNodes`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala#L379-L382
/// [`maybeCloseOldestConnection`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/network/Selector.java#L809-L825
#[derive(Clone, Debug)]
pub struct ConnectionDrain {
    /// How many drains have been requested.
    requested: watch::Sender<u64>,
}

impl ConnectionDrain {
    pub(super) fn new() -> Self {
        Self {
            requested: watch::Sender::new(0),
        }
    }

    /// Drains the connections that the broker has open, and returns before they close.
    ///
    /// The broker's listener starts the drain when it next waits for a connection, so the drain
    /// also closes a connection that the broker accepts until then. A connection that the broker
    /// accepts later stays open until the next drain. Drains that are requested before the
    /// listener starts one count as one drain, and a drain of a broker that does not listen does
    /// nothing.
    ///
    /// A connection closes only after it has answered the request that it holds, so a slow
    /// request keeps its connection open for as long as the request takes.
    pub fn drain(&self) {
        self.requested
            .send_modify(|requested| *requested = requested.wrapping_add(1));
    }

    /// Returns a receiver that changes at each drain that is requested from now on.
    pub(super) fn requests(&self) -> watch::Receiver<u64> {
        self.requested.subscribe()
    }
}
