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

use crate::{Error, Kind, MessageKind, Result, VersionRange};
use serde_json::Value;
use std::str::FromStr;

// A Wrapped JSON type
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Wv<'a>(&'a Value);

impl<'a, 'b: 'a> From<&'b Value> for Wv<'a> {
    fn from(value: &'b Value) -> Self {
        Self(value)
    }
}

pub trait As<'a, T>
where
    T: 'a,
{
    fn as_a(&'a self, name: &str) -> Result<T>;
}

impl<'a> As<'a, &'a str> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<&'a str> {
        self.as_option(name)
            .and_then(|maybe| maybe.ok_or(Error::Message(name.into())))
    }
}

impl<'a> As<'a, String> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<String> {
        self.as_option(name)
            .and_then(|maybe| maybe.ok_or(Error::Message(name.into())))
    }
}

impl<'a> As<'a, &'a [Value]> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<&'a [Value]> {
        self.0[name]
            .as_array()
            .map(|v| &v[..])
            .ok_or(Error::Message(String::from(name)))
    }
}

impl<'a> As<'a, u64> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u64> {
        self.0[name]
            .as_u64()
            .ok_or(Error::Message(String::from(name)))
    }
}

impl<'a> As<'a, u32> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u32> {
        As::<'a, u64>::as_a(self, name).and_then(|u| u32::try_from(u).map_err(Into::into))
    }
}

impl<'a> As<'a, u16> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u16> {
        As::<'a, u64>::as_a(self, name).and_then(|u| u16::try_from(u).map_err(Into::into))
    }
}

impl<'a> As<'a, i64> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<i64> {
        self.0[name]
            .as_i64()
            .ok_or(Error::Message(String::from(name)))
    }
}

impl<'a> As<'a, i16> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<i16> {
        As::<'a, i64>::as_a(self, name).and_then(|u| i16::try_from(u).map_err(Into::into))
    }
}

impl<'a> As<'a, MessageKind> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<MessageKind> {
        self.as_a(name).and_then(MessageKind::from_str)
    }
}

impl<'a> As<'a, VersionRange> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<VersionRange> {
        self.as_a(name).and_then(VersionRange::from_str)
    }
}

impl<'a> As<'a, Kind> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<Kind> {
        self.as_a(name).and_then(Kind::from_str)
    }
}

pub trait AsOption<'a, T>
where
    T: 'a,
{
    fn as_option(&'a self, name: &str) -> Result<Option<T>>;
}

impl<'a> AsOption<'a, bool> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<bool>> {
        Ok(self.0[name].as_bool())
    }
}

impl<'a> AsOption<'a, u64> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u64>> {
        Ok(self.0[name].as_u64())
    }
}

impl<'a> AsOption<'a, u32> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u32>> {
        self.0[name]
            .as_u64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, u16> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u16>> {
        self.0[name]
            .as_u64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, u8> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u8>> {
        self.0[name]
            .as_u64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, i64> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i64>> {
        Ok(self.0[name].as_i64())
    }
}

impl<'a> AsOption<'a, i32> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i32>> {
        self.0[name]
            .as_i64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, i16> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i16>> {
        self.0[name]
            .as_i64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, i8> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i8>> {
        self.0[name]
            .as_i64()
            .map_or(Ok(None), |v| v.try_into().map_err(Into::into).map(Some))
    }
}

impl<'a> AsOption<'a, &'a str> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<&'a str>> {
        Ok(self.0[name].as_str())
    }
}

impl<'a> AsOption<'a, String> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<String>> {
        Ok(self.0[name].as_str().map(String::from))
    }
}

impl<'a> AsOption<'a, &'a [Value]> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<&'a [Value]>> {
        Ok(self.0[name].as_array().map(|v| &v[..]))
    }
}

impl<'a> AsOption<'a, VersionRange> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<VersionRange>> {
        self.0[name]
            .as_str()
            .map_or(Ok(None), |s| VersionRange::from_str(s).map(Some))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn as_a_str() -> Result<()> {
        let v = serde_json::from_str::<Value>(
            r#"
            {
                "hello": "world"
            }
            "#,
        )?;

        let wv = Wv::from(&v);
        let s: &str = wv.as_a("hello")?;
        assert_eq!("world", s);

        Ok(())
    }

    #[test]
    fn as_a_string() -> Result<()> {
        let v = serde_json::from_str::<Value>(
            r#"
            {
                "hello": "world"
            }
            "#,
        )?;

        let wv = Wv::from(&v);
        let s: String = wv.as_a("hello")?;
        assert_eq!("world", s);

        Ok(())
    }

    #[test]
    fn as_a_kind() -> Result<()> {
        let v = serde_json::from_str::<Value>(
            r#"
            {
                "type": "request"
            }
            "#,
        )?;

        let wv = Wv::from(&v);
        let s: MessageKind = wv.as_a("type")?;
        assert_eq!(MessageKind::Request, s);

        Ok(())
    }

    #[test]
    fn as_option() -> Result<()> {
        fn as_option<'a, 'b, T>(
            instance: &'a Wv<'b>,
            operation: impl Fn(&'a Wv<'b>, &'static str) -> Result<Option<T>>,
            tests: &[(&'static str, Option<T>)],
        ) -> Result<()>
        where
            T: 'a + PartialEq + std::fmt::Debug,
        {
            for (name, expected) in tests {
                assert_eq!(*expected, operation(instance, name)?, "name: {name}");
            }
            Ok(())
        }

        let v = serde_json::from_str::<Value>(
            r#"
            {
                "u64": 18446744073709551615,
                "u32": 4294967295,
                "u16": 65535,
                "u8": 255,

                "i64_max": 9223372036854775807,
                "i64_min": -9223372036854775808,
                "i32_max": 2147483647,
                "i32_min": -2147483648,
                "i16_max": 32767,
                "i16_min": -32768,
                "i8_max": 127,
                "i8_min": -128
            }
            "#,
        )?;

        let wv = Wv::from(&v);

        as_option(
            &wv,
            <Wv<'_> as AsOption<u64>>::as_option,
            &[
                ("u64", Some(u64::MAX)),
                ("u32", Some(u32::MAX.into())),
                ("u16", Some(u16::MAX.into())),
                ("u8", Some(u8::MAX.into())),
                ("i64_max", Some(i64::MAX.try_into()?)),
                ("i64_min", None),
                ("i32_max", Some(i32::MAX.try_into()?)),
                ("i32_min", None),
                ("i16_max", Some(i16::MAX.try_into()?)),
                ("i16_min", None),
                ("i8_max", Some(i8::MAX.try_into()?)),
                ("i8_min", None),
            ],
        )?;

        assert!(matches!(
            <Wv<'_> as AsOption<u32>>::as_option(&wv, "u64"),
            Err(Error::TryFromInt(_)),
        ));
        assert!(matches!(
            <Wv<'_> as AsOption<u32>>::as_option(&wv, "i64_max"),
            Err(Error::TryFromInt(_)),
        ));

        as_option(
            &wv,
            <Wv<'_> as AsOption<u32>>::as_option,
            &[
                // ("u64", None),
                ("u32", Some(u32::MAX)),
                ("u16", Some(u16::MAX.into())),
                ("u8", Some(u8::MAX.into())),
                // ("i64_max", Some(i64::MAX.try_into()?)),
                ("i64_min", None),
                ("i32_max", Some(i32::MAX.try_into()?)),
                ("i32_min", None),
                ("i16_max", Some(i16::MAX.try_into()?)),
                ("i16_min", None),
                ("i8_max", Some(i8::MAX.try_into()?)),
                ("i8_min", None),
            ],
        )?;

        Ok(())
    }

    #[test]
    fn as_a_u64() -> Result<()> {
        let v = serde_json::from_str::<Value>(
            r#"
            {
                "value": 32123
            }
            "#,
        )?;

        let wv = Wv::from(&v);
        let s: u64 = wv.as_a("value")?;
        assert_eq!(32123, s);

        Ok(())
    }

    #[test]
    fn as_a_version_range() -> Result<()> {
        let v = serde_json::from_str::<Value>(
            r#"
            {
                "flexibleVersions": "2+"
            }
            "#,
        )?;

        let wv = Wv::from(&v);
        let flexible: VersionRange = wv.as_a("flexibleVersions")?;

        assert_eq!(
            VersionRange {
                start: 2,
                end: i16::MAX
            },
            flexible
        );

        Ok(())
    }

    #[test]
    fn as_array_value() -> Result<()> {
        let v = serde_json::from_str::<Value>(
            r#"
            {
                "x": [1,2,3]
            }
            "#,
        )?;

        let wv = Wv::from(&v);
        let array: &[Value] = wv.as_a("x")?;
        assert_eq!([json!(1), json!(2), json!(3)], array);

        Ok(())
    }
}
