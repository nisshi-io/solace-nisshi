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

//! A Kafka record (headers, key and value) and the one-line text form that the Kafka console
//! producer reads and the console consumer prints.

/// A Kafka record with headers, a key and a value.
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    pub headers: &'a [(&'a str, &'a str)],
    pub key: &'a str,
    pub value: &'a str,
}

impl Record<'_> {
    /// Returns the record as the console tools write it: the headers as `name:value` pairs
    /// separated by commas, then the key, then the value, separated by tabs.
    ///
    /// The console producer reads this line with `parse.headers=true` and `parse.key=true`.
    /// The console consumer prints the same fields in the same format with `print.headers`,
    /// `print.key` and `print.value`.
    pub fn console_line(&self) -> String {
        let headers = self
            .headers
            .iter()
            .map(|(name, value)| format!("{name}:{value}"))
            .collect::<Vec<_>>()
            .join(",");

        format!("{headers}\t{}\t{}", self.key, self.value)
    }

    /// Returns `records` as console producer input, one line per record.
    pub fn console_input(records: &[Record<'_>]) -> String {
        records
            .iter()
            .map(|record| record.console_line() + "\n")
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Record;

    #[test]
    fn console_line_puts_headers_key_and_value_in_order() {
        let record = Record {
            headers: &[("h1", "pqr"), ("h2", "jkl")],
            key: "qwerty",
            value: "poiuy",
        };

        assert_eq!(record.console_line(), "h1:pqr,h2:jkl\tqwerty\tpoiuy");
    }
}
