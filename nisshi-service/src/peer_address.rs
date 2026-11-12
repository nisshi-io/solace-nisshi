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

use std::net::{IpAddr, SocketAddr};

use rama::extensions::Extension;

/// The address of the client at the other end of a connection, which the broker inserts into
/// the connection's extensions when it accepts the connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Extension)]
pub struct PeerAddressExtension(SocketAddr);

impl PeerAddressExtension {
    pub fn new(peer_address: SocketAddr) -> Self {
        Self(peer_address)
    }

    /// Returns the client's host as Kafka reports it in `DescribeGroups`: `/` and the IP
    /// address, written as Java writes an `InetAddress` without a host name.
    ///
    /// A listener on an IPv6 address sees an IPv4 client at an IPv4-mapped IPv6 address. Java
    /// reports such an address as the IPv4 address, and so does this method. This method does not
    /// write the zone of a link-local IPv6 address, which Java writes after a `%`.
    pub fn client_host(&self) -> String {
        match self.0.ip().to_canonical() {
            IpAddr::V4(address) => format!("/{address}"),

            // Java writes each of the eight groups of an IPv6 address in hexadecimal, and does not
            // shorten a run of zero groups to `::` as Rust does.
            IpAddr::V6(address) => format!(
                "/{}",
                address
                    .segments()
                    .map(|segment| format!("{segment:x}"))
                    .join(":")
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn client_host_is_a_slash_and_the_ip_address_as_java_writes_it() {
        assert_eq!(
            "/192.0.2.7",
            PeerAddressExtension::new(SocketAddr::from((Ipv4Addr::new(192, 0, 2, 7), 50_000)))
                .client_host()
        );
        assert_eq!(
            "/2001:db8:0:0:0:0:0:1",
            PeerAddressExtension::new(SocketAddr::from((
                Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1),
                50_000
            )))
            .client_host()
        );
    }

    #[test]
    fn client_host_of_an_ipv4_mapped_address_is_the_ipv4_address() {
        let mapped = Ipv4Addr::new(127, 0, 0, 1).to_ipv6_mapped();

        assert_eq!(
            "/127.0.0.1",
            PeerAddressExtension::new(SocketAddr::from((mapped, 50_000))).client_host()
        );
    }
}
