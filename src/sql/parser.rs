use crate::error::Result;
use sqlparser::ast::Statement;
use sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Tokenizer};

pub struct WrappedParser<'a> {
    inner: Parser<'a>,
}

impl<'a> WrappedParser<'a> {
    pub fn try_new(sql: &str) -> Result<Self> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(Self {
            inner: Parser::new(dialect).with_tokens(tokens),
        })
    }

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
