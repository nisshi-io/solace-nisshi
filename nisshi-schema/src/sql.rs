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

use std::sync::LazyLock;

use crate::{Error, Result};
use arrow::datatypes::DataType;
use datafusion::sql::sqlparser::{
    ast::{CastKind, DataType as SqlDataType, Expr, Ident},
    dialect::GenericDialect,
    parser::Parser,
};
use regex::Regex;
use tracing::debug;

/// A safe, unquoted SQL identifier: this is the entire allowlist for what
/// may appear as a path segment inside a `nisshi.lake.generate.*` value, or
/// as the generated column name taken from the config key itself. Neither
/// of these ever originates from us: topic configuration can currently be
/// set by any connected client (a separate, known authorization gap), so
/// both strings must be treated as untrusted input.
#[allow(clippy::unwrap_used)]
static SAFE_IDENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap());

/// Validates that `name` is safe to use verbatim as a generated column
/// name (the suffix of a `nisshi.lake.generate.<name>` config key).
pub(crate) fn validate_generated_column_name(name: &str) -> Result<()> {
    if SAFE_IDENT.is_match(name) {
        Ok(())
    } else {
        Err(Error::Message(format!(
            "unsafe generated column name: {name:?}"
        )))
    }
}

/// A `nisshi.lake.generate.<name>` value, once validated: a target Arrow
/// type and the dotted path of the (existing) column it is derived from.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GeneratedExpr {
    pub(crate) data_type: DataType,
    pub(crate) path: Vec<String>,
}

/// Parses and validates a `nisshi.lake.generate.<name>` value.
///
/// The only expression shape ever used by (or documented for) this
/// feature is `CAST(<column path> AS DATE|INT|INTEGER)`, where `<column
/// path>` is a plain, dotted reference to an existing column (e.g.
/// `meta.timestamp`). This function enforces exactly that grammar and
/// nothing more: any other SQL expression, however syntactically valid,
/// is rejected with `Err`, never accepted, and never used to build SQL
/// text.
///
/// Because topic configuration can currently be set by any connected
/// client, `expr` is untrusted input. This function must never panic on
/// malformed or hostile input: doing so would turn a rejected value into
/// a broker-wide denial of service on every write to the topic.
pub(crate) fn parse_generated_expr(expr: &str) -> Result<GeneratedExpr> {
    let dialect = GenericDialect {};

    let ast = Parser::new(&dialect)
        .try_with_sql(expr)?
        .parse_expr()
        .inspect(|ast| debug!(?ast))?;

    let Expr::Cast {
        kind: CastKind::Cast,
        expr: inner,
        data_type,
        // `array: true` is MySQL's `CAST(... AS type ARRAY)` functional
        // index form, not a data type nisshi supports here: reject it
        // explicitly rather than silently accepting a new syntax shape.
        array: false,
        format: None,
    } = ast
    else {
        return Err(Error::Message(format!(
            "unsupported generated column expression: {expr}"
        )));
    };

    let data_type = delta_sql_type(data_type)?;
    let path = identifier_path(*inner)?;

    Ok(GeneratedExpr { data_type, path })
}

/// Requires `expr` to be a plain, unquoted column reference (optionally
/// dotted, e.g. `meta.timestamp`), returning its path segments. Rejects
/// everything else: function calls, subqueries, operators, literals, and
/// quoted/exotic identifiers.
fn identifier_path(expr: Expr) -> Result<Vec<String>> {
    let idents = match expr {
        Expr::Identifier(ident) => vec![ident],
        Expr::CompoundIdentifier(idents) => idents,
        otherwise => {
            return Err(Error::Message(format!(
                "unsupported generated column reference: {otherwise}"
            )));
        }
    };

    idents.into_iter().map(validate_ident).collect()
}

fn validate_ident(ident: Ident) -> Result<String> {
    if ident.quote_style.is_none() && SAFE_IDENT.is_match(&ident.value) {
        Ok(ident.value)
    } else {
        Err(Error::Message(format!(
            "unsupported identifier in generated column expression: {ident}"
        )))
    }
}

fn delta_sql_type(data_type: SqlDataType) -> Result<DataType> {
    match data_type {
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int32),

        otherwise => Err(Error::Message(format!(
            "unsupported generated column type: {otherwise}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::Error;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn simple_cast() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(
            parse_generated_expr("cast(meta.timestamp as date)")?,
            GeneratedExpr {
                data_type: DataType::Date32,
                path: vec![String::from("meta"), String::from("timestamp")],
            }
        );

        Ok(())
    }

    #[test]
    fn cast_to_integer_of_single_segment_column() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(
            parse_generated_expr("cast(vendor_id as integer)")?,
            GeneratedExpr {
                data_type: DataType::Int32,
                path: vec![String::from("vendor_id")],
            }
        );

        Ok(())
    }

    #[test]
    fn rejects_non_cast_expression() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(parse_generated_expr("meta.timestamp").is_err());

        Ok(())
    }

    #[test]
    fn rejects_function_call_inside_cast() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(parse_generated_expr("cast(upper(meta.timestamp) as date)").is_err());

        Ok(())
    }

    #[test]
    fn rejects_subquery_inside_cast() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(parse_generated_expr("cast((select 1) as integer)").is_err());

        Ok(())
    }

    #[test]
    fn rejects_arithmetic_inside_cast() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(parse_generated_expr("cast(1/0 as integer)").is_err());

        Ok(())
    }

    #[test]
    fn rejects_literal_inside_cast() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(parse_generated_expr("cast(1 as integer)").is_err());

        Ok(())
    }

    #[test]
    fn rejects_quoted_identifier() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(parse_generated_expr(r#"cast(meta."ti""; drop table t; --" as date)"#).is_err());

        Ok(())
    }

    #[test]
    fn rejects_unsupported_target_type_without_panicking() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(parse_generated_expr("cast(meta.timestamp as varchar)").is_err());

        Ok(())
    }

    #[test]
    fn rejects_mysql_array_cast_form() -> Result<()> {
        let _guard = init_tracing()?;

        // sqlparser 0.61 accepts a trailing ARRAY in every dialect, not
        // just MySQL's: `cast(... as date array)`. This isn't a data type
        // nisshi supports for this feature, so it must still be rejected.
        assert!(parse_generated_expr("cast(meta.timestamp as date array)").is_err());

        Ok(())
    }

    #[test]
    fn rejects_garbage_without_panicking() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(parse_generated_expr("); drop table t; --").is_err());

        Ok(())
    }

    #[test]
    fn accepts_safe_generated_column_name() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(validate_generated_column_name("date").is_ok());
        assert!(validate_generated_column_name("vendor_id").is_ok());

        Ok(())
    }

    #[test]
    fn rejects_unsafe_generated_column_name() -> Result<()> {
        let _guard = init_tracing()?;

        assert!(validate_generated_column_name(r#"x" AS y --"#).is_err());
        assert!(validate_generated_column_name("meta.timestamp").is_err());
        assert!(validate_generated_column_name("").is_err());

        Ok(())
    }
}
