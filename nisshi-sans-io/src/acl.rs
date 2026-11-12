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

/// Represents whether an ACL grants or denies permissions
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum Permission {
    /// Represents any permission which this client cannot understand,
    /// perhaps because this client is too old.
    #[default]
    Unknown = 0,

    /// In a filter, matches any permission.
    Any = 1,

    /// Disallows access
    Deny = 2,

    /// Grants access
    Allow = 3,
}

impl From<i8> for Permission {
    fn from(value: i8) -> Self {
        match value {
            perm if perm == Permission::Allow as i8 => Permission::Allow,
            perm if perm == Permission::Any as i8 => Permission::Any,
            perm if perm == Permission::Deny as i8 => Permission::Deny,

            _ => Permission::Unknown,
        }
    }
}

/// Represents an operation which an ACL grants or denies permission to perform.
///
/// Some operations imply other operations:
///
/// - [`Allow`] [`All`] implies [`Allow`] everything
/// - [`Deny`] [`All`] implies [`Deny`] everything
/// - [`Allow`] [`Read`] implies [`Allow`] [`Describe`]
/// - [`Allow`] [`Write`] implies [`Allow`] [`Describe`]
/// - [`Allow`] [`Delete`] implies [`Allow`] [`Describe`]
/// - [`Allow`] [`Alter`] implies [`Allow`] [`Describe`]
/// - [`Allow`] [`AlterConfigs`] implies [`Allow`] [`DescribeConfigs`]
///
/// [`Allow`]: Permission::Allow
/// [`Deny`]: Permission::Deny
/// [`All`]: Self::All
/// [`Read`]: Self::Read
/// [`Write`]: Self::Write
/// [`Delete`]: Self::Delete
/// [`Alter`]: Self::Alter
/// [`Describe`]: Self::Describe
/// [`AlterConfigs`]: Self::AlterConfigs
/// [`DescribeConfigs`]: Self::DescribeConfigs
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum Operation {
    #[default]
    Unknown = 0,

    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
    CreateTokens = 13,
    DescribeTokens = 14,
    TwoPhaseCommit = 15,
}

impl From<i8> for Operation {
    fn from(value: i8) -> Self {
        match value {
            op if op == Operation::All as i8 => Operation::All,
            op if op == Operation::Alter as i8 => Operation::Alter,
            op if op == Operation::AlterConfigs as i8 => Operation::AlterConfigs,
            op if op == Operation::Any as i8 => Operation::Any,
            op if op == Operation::ClusterAction as i8 => Operation::ClusterAction,
            op if op == Operation::Create as i8 => Operation::Create,
            op if op == Operation::CreateTokens as i8 => Operation::CreateTokens,
            op if op == Operation::Delete as i8 => Operation::Delete,
            op if op == Operation::Describe as i8 => Operation::Describe,
            op if op == Operation::DescribeConfigs as i8 => Operation::DescribeConfigs,
            op if op == Operation::DescribeTokens as i8 => Operation::DescribeTokens,
            op if op == Operation::IdempotentWrite as i8 => Operation::IdempotentWrite,
            op if op == Operation::Read as i8 => Operation::Read,
            op if op == Operation::TwoPhaseCommit as i8 => Operation::TwoPhaseCommit,
            op if op == Operation::Write as i8 => Operation::Write,

            _ => Operation::Unknown,
        }
    }
}

/// Represents a type of resource which an ACL can be applied to.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum Resource {
    /// Represents any ResourceType which this client cannot understand,
    /// perhaps because this client is too old.
    #[default]
    Unknown = 0,

    /// In a filter, matches any ResourceType
    Any = 1,

    /// A topic
    Topic = 2,

    /// A consumer group
    Group = 3,

    /// The cluster as a whole
    Cluster = 4,

    /// A transactional ID
    TransactionalId = 5,

    /// A token ID
    DelegationToken = 6,

    /// A user principal
    User = 7,
}

impl From<i8> for Resource {
    fn from(value: i8) -> Self {
        match value {
            r if r == Resource::Any as i8 => Resource::Any,
            r if r == Resource::Cluster as i8 => Resource::Cluster,
            r if r == Resource::DelegationToken as i8 => Resource::DelegationToken,
            r if r == Resource::Group as i8 => Resource::Group,
            r if r == Resource::Topic as i8 => Resource::Topic,
            r if r == Resource::TransactionalId as i8 => Resource::TransactionalId,
            r if r == Resource::User as i8 => Resource::User,

            _ => Resource::Unknown,
        }
    }
}
