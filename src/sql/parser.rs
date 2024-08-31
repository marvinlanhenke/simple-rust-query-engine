use crate::error::Result;
use sqlparser::ast::Statement;
use sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Tokenizer};

pub struct WrappedParser<'a> {
    inner: Parser<'a>,
}

/// A thin wrapper around the `Parser` from the `sqlparser` crate
/// to provide easier initialization and parsing of SQL statements.
impl<'a> WrappedParser<'a> {
    /// Creates a new instance of `WrappedParser` by tokenizing the provided SQL string.
    pub fn try_new(sql: &str) -> Result<Self> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(Self {
            inner: Parser::new(dialect).with_tokens(tokens),
        })
    }

    /// Parses a single SQL statement from the tokenized input.
    pub fn try_parse(&mut self) -> Result<Statement> {
        Ok(self.inner.parse_statement()?)
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use sqlparser::ast::{SetExpr, Statement};

    use super::WrappedParser;

    #[test]
    fn test_sql_parser_parse() {
        let sql = "SELECT * FROM simple";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        if let Statement::Query(query) = statement {
            if let SetExpr::Select(select) = *query.body {
                return assert_eq!(format!("{select}"), "SELECT * FROM simple");
            };
        }
        panic!()
    }
}
