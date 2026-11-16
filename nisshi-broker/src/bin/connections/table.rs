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

use std::fmt;

/// A Markdown table whose columns are as wide as their widest cell.
#[derive(Clone, Debug)]
pub(crate) struct Table {
    header: Vec<&'static str>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub(crate) fn new(header: &[&'static str]) -> Self {
        Self {
            header: header.to_vec(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn row(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    fn widths(&self) -> Vec<usize> {
        let mut widths = self
            .header
            .iter()
            .map(|cell| cell.chars().count())
            .collect::<Vec<_>>();

        for row in &self.rows {
            for (width, cell) in widths.iter_mut().zip(row) {
                *width = (*width).max(cell.chars().count());
            }
        }

        widths
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let widths = self.widths();

        let line = |cells: &mut dyn Iterator<Item = &str>| {
            let cells = cells
                .zip(&widths)
                .map(|(cell, width)| format!("{cell:<width$}"))
                .collect::<Vec<_>>();

            format!("| {} |", cells.join(" | "))
        };

        write!(f, "{}", line(&mut self.header.iter().copied()))?;

        let rule = widths
            .iter()
            .map(|width| "-".repeat(width + 2))
            .collect::<Vec<_>>();
        write!(f, "\n|{}|", rule.join("|"))?;

        for row in &self.rows {
            write!(f, "\n{}", line(&mut row.iter().map(String::as_str)))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_are_as_wide_as_their_widest_cell() {
        let mut table = Table::new(&["kind", "n"]);
        table.row(vec!["producer".into(), "2".into()]);
        table.row(vec!["c".into(), "10".into()]);

        assert_eq!(
            "| kind     | n  |\n\
             |----------|----|\n\
             | producer | 2  |\n\
             | c        | 10 |",
            table.to_string()
        );
    }

    #[test]
    fn a_table_without_rows_is_its_header_and_rule() {
        assert_eq!("| a |\n|---|", Table::new(&["a"]).to_string());
    }
}
