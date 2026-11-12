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

use std::{
    fs::{self, File, OpenOptions},
    net::TcpListener,
    sync::atomic::{AtomicU16, Ordering},
};

use crate::settings;

/// A port that one broker or PostgreSQL server owns. While the `ReservedPort` exists,
/// `ReservedPort::reserve` skips its port for every other test of every smoke run of the same user,
/// so the owner can stop and start again on it.
///
/// A port that its stopped owner keeps looks free to a check that something listens on it. So the
/// reservation is an exclusive `flock` on a file named after the port, in
/// `settings::port_lock_dir`, which every test process of the user sees. nextest runs each test in
/// a process of its own, so a lock inside one process would not reach the other tests. The
/// operating system releases the lock when the process exits, so a test that nextest kills leaves
/// no reservation behind.
#[derive(Debug)]
pub struct ReservedPort {
    number: u16,
    _lock: File,
}

impl ReservedPort {
    /// Reserves a port that is free, and that no other test on this machine has reserved.
    ///
    /// Panics if every port in the range is in use or reserved.
    #[track_caller]
    pub(crate) fn reserve() -> Self {
        for port in candidate_ports() {
            let Some(lock) = try_lock_port(port) else {
                continue;
            };

            // A process outside the smoke runs can still listen on a reserved port.
            if port_is_free(port) {
                return Self {
                    number: port,
                    _lock: lock,
                };
            }
        }

        panic!("no free port")
    }

    pub fn number(&self) -> u16 {
        self.number
    }

    /// Panics if something already listens on the port, because a broker's readiness check would
    /// then pass against it.
    #[track_caller]
    pub(crate) fn assert_free(&self) {
        let port = self.number;

        assert!(
            port_is_free(port),
            "port {port} is already in use: stop what listens there (`docker ps --filter \
             label=nisshi-smoke` lists containers an earlier run left behind), or choose another port"
        );
    }
}

/// Takes the lock file of `port`, or returns `None` if another `ReservedPort` holds it.
#[track_caller]
fn try_lock_port(port: u16) -> Option<File> {
    let dir = settings::port_lock_dir();

    if let Err(err) = fs::create_dir_all(&dir) {
        panic!("{}: {err}", dir.display());
    }

    let path = dir.join(port.to_string());
    let file = match OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(err) => panic!("{}: {err}", path.display()),
    };

    file.try_lock().ok().map(|()| file)
}

/// Returns the ports that [`ReservedPort::reserve`] tries, in order.
fn candidate_ports() -> impl Iterator<Item = u16> {
    /// Reserved ports start here, below the ports that the operating system picks for outgoing
    /// connections and port-0 binds: 32768 up on Linux, 49152 up on macOS. The operating system
    /// then can't take a reserved port before its owner binds it.
    const FIRST_BROKER_PORT: u16 = 20000;
    const BROKER_PORT_COUNT: u16 = 10000;

    static PORTS_TRIED: AtomicU16 = AtomicU16::new(0);

    // Each process starts at its own offset, so parallel tests try different ports.
    let start = (std::process::id() % u32::from(BROKER_PORT_COUNT)) as u16;

    (0..BROKER_PORT_COUNT).map(move |_| {
        let offset = start.wrapping_add(PORTS_TRIED.fetch_add(1, Ordering::Relaxed));
        FIRST_BROKER_PORT + offset % BROKER_PORT_COUNT
    })
}

fn port_is_free(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}
