use std::sync::Arc;

use arrow_schema::SchemaRef;

use crate::{error::Result, io::DataSource, plan::physical::plan::ExecutionPlan};

#[derive(Debug)]
pub struct ListingTable {
    name: String,
    path: String,
    schema: SchemaRef,
    source: Arc<dyn DataSource>,
}

impl ListingTable {
    pub fn new(
        name: impl Into<String>,
        path: impl Into<String>,
        schema: SchemaRef,
        source: Arc<dyn DataSource>,
    ) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            schema,
            source,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn source(&self) -> &dyn DataSource {
        self.source.as_ref()
    }
}

impl DataSource for ListingTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, _projection: Option<&Vec<String>>) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}
