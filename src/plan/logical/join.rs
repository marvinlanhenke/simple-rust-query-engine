use std::{fmt::Display, sync::Arc};

use arrow_schema::{FieldRef, Schema, SchemaRef};
use snafu::location;

use crate::{
    error::{Error, Result},
    expression::logical::{column::Column, expr::Expression},
};

use super::plan::LogicalPlan;

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

#[derive(Debug)]
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
    pub fn try_new_with_projection(
        original: &LogicalPlan,
        lhs: Arc<LogicalPlan>,
        rhs: Arc<LogicalPlan>,
        on: (Vec<Column>, Vec<Column>),
    ) -> Result<Self> {
        let original_join = match original {
            LogicalPlan::Join(join) => join,
            _ => {
                return Err(Error::InvalidOperation {
                    message: "Cannot create 'Join' plan with projected input".to_string(),
                    location: location!(),
                })
            }
        };

        let on =
            on.0.into_iter()
                .zip(on.1)
                .map(|(l, r)| (Expression::Column(l), Expression::Column(r)))
                .collect::<Vec<_>>();
        let schema = Self::create_join_schema(lhs.schema(), rhs.schema(), &original_join.join_type);

        Ok(Self {
            lhs,
            rhs,
            on,
            filter: original_join.filter.clone(),
            join_type: original_join.join_type,
            schema,
        })
    }

    /// The output schema.
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
