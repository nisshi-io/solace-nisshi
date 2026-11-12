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

pub mod offset;
pub mod time;

use crate::{Result, TopitionOffset};
use std::fmt::Debug;

pub trait OffsetProvider: Debug + Send {
    fn provide_offset(&self, tpo: &TopitionOffset) -> Result<Box<dyn Offset>>;
}

impl<T: OffsetProvider + ?Sized> OffsetProvider for Box<T> {
    fn provide_offset(&self, tpo: &TopitionOffset) -> Result<Box<dyn Offset>> {
        (**self).provide_offset(tpo)
    }
}

pub trait Offset: Debug + Send {
    fn append(&mut self, offset: i64, position: u64) -> Result<()>;
    fn position_for_offset(&mut self, offset: i64) -> Result<u64>;
}

impl<T: Offset + ?Sized> Offset for Box<T> {
    fn append(&mut self, offset: i64, position: u64) -> Result<()> {
        (**self).append(offset, position)
    }

    fn position_for_offset(&mut self, offset: i64) -> Result<u64> {
        (**self).position_for_offset(offset)
    }
}

pub trait TimeProvider: Debug + Send {
    fn provide_time(&self, tpo: &TopitionOffset) -> Result<Box<dyn Time>>;
}

impl<T: TimeProvider + ?Sized> TimeProvider for Box<T> {
    fn provide_time(&self, tpo: &TopitionOffset) -> Result<Box<dyn Time>> {
        (**self).provide_time(tpo)
    }
}

pub trait Time: Debug + Send {
    fn append(&mut self, time: i64, offset: i64) -> Result<()>;
    fn offset_for_time(&mut self, time: i64) -> Result<i64>;
}

impl<T: Time + ?Sized> Time for Box<T> {
    fn append(&mut self, time: i64, offset: i64) -> Result<()> {
        (**self).append(time, offset)
    }

    fn offset_for_time(&mut self, time: i64) -> Result<i64> {
        (**self).offset_for_time(time)
    }
}
