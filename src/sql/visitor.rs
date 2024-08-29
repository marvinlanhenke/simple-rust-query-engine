use std::{collections::BTreeSet, ops::ControlFlow};

use crate::error::{Error, Result};

use snafu::location;
use sqlparser::ast::{ObjectName, Statement, Visit, Visitor};

struct RelationVisitor {
    relations: BTreeSet<ObjectName>,
}

impl RelationVisitor {
    fn insert_relation(&mut self, relation: &ObjectName) {
        if !self.relations.contains(relation) {
            self.relations.insert(relation.clone());
        };
    }
}

impl Visitor for RelationVisitor {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> std::ops::ControlFlow<Self::Break> {
        self.insert_relation(relation);
        ControlFlow::Continue(())
    }
}

pub fn resolve_table_references(statement: &Statement) -> Result<Vec<String>> {
    let mut visitor = RelationVisitor {
        relations: BTreeSet::new(),
    };

    statement.visit(&mut visitor);

    visitor
        .relations
        .into_iter()
        .map(|ObjectName(idents)| match idents.len() {
            1 => Ok(idents[0].value.clone()),
            _ => Err(Error::InvalidOperation {
                message: "Only single, bare table references are allowed.".to_string(),
                location: location!(),
            }),
        })
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::WrappedParser;

    use super::resolve_table_references;

    #[test]
    fn test_sql_visitor_resolve_table_references_multiple_tables() {
        let sql = "SELECT * FROM one LEFT JOIN two ON one.a = two.b";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = resolve_table_references(&statement).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "one");
        assert_eq!(result[1], "two");
    }

    #[test]
    fn test_sql_visitor_resolve_table_references_single_table() {
        let sql = "SELECT * FROM simple";
        let mut parser = WrappedParser::try_new(sql).unwrap();
        let statement = parser.try_parse().unwrap();

        let result = resolve_table_references(&statement).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "simple");
    }
}
