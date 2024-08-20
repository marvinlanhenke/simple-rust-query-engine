use std::{fmt::Display, sync::Arc};

use arrow_schema::{FieldRef, Schema, SchemaRef};

use crate::expression::logical::expr::Expression;

use super::plan::LogicalPlan;

/// Enum representing the type of join operation.
#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Left => write!(f, "LEFT"),
        }
    }
}

/// Represents a join operation in a logical query plan.
///
/// The `Join` struct defines the left and right inputs of the join, the join condition,
/// the type of join, and any additional filters applied during the join. It also
/// includes the schema of the output after the join is performed.
#[derive(Debug, Clone)]
pub struct Join {
    /// Left input [`LogicalPlan`].
    lhs: Arc<LogicalPlan>,
    /// Right input [`LogicalPlan`].
    rhs: Arc<LogicalPlan>,
    /// Equijoin expression, as a pair of left and right.
    on: Vec<(Expression, Expression)>,
    /// The type of join.
    join_type: JoinType,
    /// The filter applied during join (non-equi conditions).
    filter: Option<Expression>,
    /// The output schema.
    schema: SchemaRef,
}

impl Join {
    /// Creates a new [`Join`] instance.
    pub fn new(
        lhs: Arc<LogicalPlan>,
        rhs: Arc<LogicalPlan>,
        on: Vec<(Expression, Expression)>,
        join_type: JoinType,
        filter: Option<Expression>,
    ) -> Self {
        let schema = Self::create_join_schema(lhs.schema(), rhs.schema(), &join_type);
        Self {
            lhs,
            rhs,
            on,
            join_type,
            filter,
            schema,
        }
    }

    /// Retrieves the left input `LogicalPlan`.
    pub fn lhs(&self) -> &LogicalPlan {
        &self.lhs
    }

    /// Retrieves the right input `LogicalPlan`.
    pub fn rhs(&self) -> &LogicalPlan {
        &self.rhs
    }

    /// Retrieves the equijoin expression.
    pub fn on(&self) -> &[(Expression, Expression)] {
        &self.on
    }

    /// Retrieves the type of join.
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Retrieves the optional join filter expression.
    pub fn filter(&self) -> Option<&Expression> {
        self.filter.as_ref()
    }

    /// Retrieves the output schema, after the join operation.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Retrieves the child logical plans.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        vec![&self.lhs, &self.rhs]
    }

    /// Creates the output schema for a join operation.
    /// The fields from the left-hand-side are created first.
    fn create_join_schema(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        join_type: &JoinType,
    ) -> SchemaRef {
        let qualified_fields: Vec<FieldRef> = match join_type {
            JoinType::Inner => left_schema
                .fields()
                .iter()
                .chain(right_schema.fields().iter())
                .cloned()
                .collect(),
            JoinType::Left => {
                let nullable_right_fields = right_schema
                    .fields()
                    .iter()
                    .map(|f| {
                        let field = f.as_ref().clone().with_nullable(true);
                        Arc::new(field)
                    })
                    .collect::<Vec<_>>();
                left_schema
                    .fields()
                    .iter()
                    .cloned()
                    .chain(nullable_right_fields)
                    .collect()
            }
        };

        let schema = Schema::new(qualified_fields);
        Arc::new(schema)
    }
}

impl Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Join: [type: {}, on: {:?}]", self.join_type, self.on)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::tests::create_schema;

    use super::{Join, JoinType};

    #[test]
    fn test_create_join_schema() {
        let left_schema = Arc::new(create_schema());
        let right_schema = Arc::new(create_schema());

        let inner =
            Join::create_join_schema(left_schema.clone(), right_schema.clone(), &JoinType::Inner);
        assert_eq!(inner.fields().len(), 6);

        let left =
            Join::create_join_schema(left_schema.clone(), right_schema.clone(), &JoinType::Left);
        assert_eq!(left.fields().len(), 6)
    }
}
