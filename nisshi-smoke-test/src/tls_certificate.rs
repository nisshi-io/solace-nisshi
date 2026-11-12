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

/// A TLS certificate and its private key, in PEM, that a broker serves TLS with. The tools trust
/// this certificate directly, so the harness does not create a certificate authority.
#[derive(Clone, Debug)]
pub struct TlsCertificate {
    pub certificate_pem: String,
    pub private_key_pem: String,
}

impl TlsCertificate {
    /// Generates a self-signed certificate for `localhost` and `127.0.0.1`.
    ///
    /// The tools connect to a launched broker at `127.0.0.1`, and check that the certificate names
    /// that address.
    pub fn self_signed_for_localhost() -> Self {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(["localhost".to_owned(), "127.0.0.1".to_owned()])
                .expect("rcgen generates a self-signed certificate");

        Self {
            certificate_pem: cert.pem(),
            private_key_pem: signing_key.serialize_pem(),
        }
    }
}
