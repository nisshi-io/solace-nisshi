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

use object_store::{ObjectMeta, PutResult, UpdateVersion};

use crate::Version;

impl From<Version> for UpdateVersion {
    fn from(value: Version) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<ObjectMeta> for Version {
    fn from(value: ObjectMeta) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<UpdateVersion> for Version {
    fn from(value: UpdateVersion) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<PutResult> for Version {
    fn from(value: PutResult) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}
