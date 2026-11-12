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

use std::{
    fs, io,
    path::{Path, PathBuf},
    thread,
};

use crate::{
    StorageUrl, TlsCertificate,
    common::{docker, unique},
    settings,
};

/// The directory, in the broker's own directory, that holds a TLS broker's
/// [`TLS_CERTIFICATE_FILE`] and [`TLS_PRIVATE_KEY_FILE`].
pub(super) const TLS_DIRECTORY: &str = "tls";
/// The file that holds a TLS broker's certificate, in PEM.
pub(super) const TLS_CERTIFICATE_FILE: &str = "certificate.pem";
/// The file that holds a TLS broker's private key, in PEM.
pub(super) const TLS_PRIVATE_KEY_FILE: &str = "private-key.pem";

/// A launched broker's files, which it keeps when it restarts. Dropping them removes the volume,
/// and also the directory unless [`BrokerFiles::keep_dir`] is set or the test is failing,
/// because the directory then holds the logs that show why.
#[derive(Debug)]
pub(super) struct BrokerFiles {
    /// The broker's own directory: a log per start, a process's SQLite database, and a TLS
    /// broker's [`TLS_DIRECTORY`].
    dir: PathBuf,
    /// A container's SQLite database volume, mounted as its working directory.
    volume: Option<String>,
    /// How many times the broker has started, which numbers its logs.
    starts: u32,
    /// Keeps the directory after the files are dropped, for a broker that failed its checks.
    keep_dir: bool,
}

impl BrokerFiles {
    /// Creates a directory under [`settings::broker_work_dir`], and a volume for a SQLite broker in
    /// a container.
    #[track_caller]
    pub(super) fn new(storage: &StorageUrl) -> Self {
        let dir = settings::broker_work_dir().join(unique::name("nisshi-smoke"));

        if let Err(err) = fs::create_dir_all(&dir) {
            panic!("broker directory {}: {err}", dir.display());
        }

        let mut files = Self {
            dir,
            volume: None,
            starts: 0,
            keep_dir: false,
        };
        files.create_volume_for(storage);

        files
    }

    /// Creates a volume for a SQLite broker in a container, if the files don't have one yet.
    ///
    /// The volume is the container's working directory, where the broker resolves a relative
    /// `sqlite://` path. A database outside the volume is removed with the container, at the next
    /// stop.
    pub(super) fn create_volume_for(&mut self, storage: &StorageUrl) {
        if self.volume.is_none() && settings::broker_image().is_some() && storage.is_sqlite() {
            let volume = unique::name("nisshi-smoke-sqlite");

            _ = docker::execute(&["volume", "create", &docker::label(), &volume]);

            self.volume = Some(volume);
        }
    }

    /// Deletes each of `paths`, relative to the broker's working directory, while the broker is
    /// stopped. Skips a path that doesn't exist, and panics if a deletion fails.
    ///
    /// A broker container without a volume keeps its files in the container, which the harness
    /// removed when the broker stopped, so there is nothing to delete.
    #[track_caller]
    pub(super) fn delete(&self, paths: &[impl AsRef<str>]) {
        if paths.is_empty() {
            return;
        }

        match (&self.volume, settings::broker_image()) {
            (_, None) => {
                for path in paths {
                    let path = path.as_ref();
                    match fs::remove_file(self.dir.join(path)) {
                        Ok(()) => {}
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                        Err(err) => panic!("{path}: {err}"),
                    }
                }
            }

            (Some(volume), Some(_)) => {
                // The broker's image holds only the broker, without `rm`, so a container from the
                // Kafka tools' image deletes the files from the volume.
                let tools_image =
                    match docker::inspect(&settings::kafka_container(), "{{.Config.Image}}") {
                        Ok(tools_image) => tools_image,
                        Err(reason) => panic!("{reason}"),
                    };
                let mount = format!("{volume}:/data");
                let label = docker::label();
                let mut args = vec![
                    "run",
                    "--rm",
                    &label,
                    // The broker writes the volume as root. The tools' image runs as a user that
                    // can't delete root's files.
                    "--user=0",
                    "--volume",
                    &mount,
                    "--entrypoint",
                    "rm",
                    &tools_image,
                    "-f",
                ];
                let paths_in_volume = paths
                    .iter()
                    .map(|path| format!("/data/{}", path.as_ref()))
                    .collect::<Vec<_>>();
                args.extend(paths_in_volume.iter().map(String::as_str));

                match docker::execute(&args) {
                    Ok(deleted) if deleted.code == Some(0) => {}
                    deleted => {
                        panic!("couldn't delete {paths_in_volume:?} from {volume}: {deleted:?}")
                    }
                }
            }

            (None, Some(_)) => {}
        }
    }

    /// Writes `certificate` and its private key to [`TLS_CERTIFICATE_FILE`] and
    /// [`TLS_PRIVATE_KEY_FILE`] in [`BrokerFiles::tls_directory`].
    pub(super) fn write_tls_certificate(&self, certificate: &TlsCertificate) {
        let tls_directory = self.tls_directory();

        if let Err(err) = fs::create_dir_all(&tls_directory) {
            panic!("{}: {err}", tls_directory.display());
        }

        for (file, pem) in [
            (TLS_CERTIFICATE_FILE, &certificate.certificate_pem),
            (TLS_PRIVATE_KEY_FILE, &certificate.private_key_pem),
        ] {
            let path = tls_directory.join(file);

            if let Err(err) = fs::write(&path, pem) {
                panic!("{}: {err}", path.display());
            }
        }
    }

    /// Returns the directory on this machine that holds a TLS broker's certificate and private
    /// key: [`TLS_DIRECTORY`] in the broker's own directory.
    pub(super) fn tls_directory(&self) -> PathBuf {
        self.dir.join(TLS_DIRECTORY)
    }

    /// Returns the broker's own directory, which is also a broker process's working directory.
    pub(super) fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the volume that a SQLite broker container keeps its database in.
    pub(super) fn volume(&self) -> Option<&str> {
        self.volume.as_deref()
    }

    /// Keeps the directory after the files are dropped, for a broker that failed its checks.
    pub(super) fn keep_directory(&mut self) {
        self.keep_dir = true;
    }

    /// The log for the broker's next start, so a restart keeps the log from before it.
    pub(super) fn next_log(&mut self) -> PathBuf {
        self.starts += 1;
        self.dir.join(format!("broker-{}.log", self.starts))
    }
}

impl Drop for BrokerFiles {
    fn drop(&mut self) {
        if let Some(volume) = &self.volume {
            _ = docker::execute(&["volume", "rm", "--force", volume]);
        }

        if !self.keep_dir && !thread::panicking() {
            _ = fs::remove_dir_all(&self.dir);
        }
    }
}
