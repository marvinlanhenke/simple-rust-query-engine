use std::collections::HashMap;

use snafu::location;

use ahash::RandomState;
use arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    row::{RowConverter, Rows, SortField},
};

use crate::{
    error::{Error, Result},
    plan::physical::joins::utils::create_hashes,
};

/// Manages the aggregation of group values within a dataset using a defined grouping schema.
///
/// This struct utilizes the `arrow::RowConverter` to transform an array into a row format, which is then hashed.
/// The resulting hash serves as a key in a hashmap where each key is associated with a unique group index.
/// Upon processing a new array, it either assigns a new group index for previously unseen values or
/// retrieves an existing index from the hashmap. These indices are stored in the provided `Vec<usize>`
/// (current_group_indices) to track the groups of each entry in the dataset.
#[derive(Debug)]
pub struct GroupValues {
    /// The grouping schema.
    schema: SchemaRef,
    /// Converts arrow arrays into row format.
    row_converter: RowConverter,
    /// Maps a hashed row to indices in the `group values` array.
    map: HashMap<u64, usize>,
    /// Optional storage of grouped rows.
    group_values: Option<Rows>,
    /// `RandomState` used for generating consistent hash values.
    random_state: RandomState,
    /// An internal buffer to store computed hash values
    /// for the current batch of rows.
    hashes_buffer: Vec<u64>,
}

impl GroupValues {
    /// Attempts to create a new [`GroupValues] instance.
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        Ok(Self {
            schema,
            row_converter,
            map: HashMap::new(),
            group_values: None,
            random_state: Default::default(),
            hashes_buffer: Default::default(),
        })
    }

    /// Retrieves a reference-counted `grouping schema`.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Checks if the group values are present and not empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of unique group entries stored.
    pub fn len(&self) -> usize {
        self.group_values
            .as_ref()
            .map(|r| r.num_rows())
            .unwrap_or(0)
    }

    /// Emits the aggregated group values as an array of `ArrayRef`,
    /// resetting the state for new aggregations.
    pub fn emit(&mut self) -> Result<Vec<ArrayRef>> {
        let mut group_values = self
            .group_values
            .take()
            .ok_or_else(|| Error::InvalidOperation {
                message: "Cannot emit from empty rows".to_string(),
                location: location!(),
            })?;
        let output = self.row_converter.convert_rows(&group_values)?;
        group_values.clear();
        self.group_values = Some(group_values);

        Ok(output)
    }

    /// Processes and interns group values from the provided columns,
    /// updating internal state and group indices.
    pub fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        groups.clear();

        let mut group_values = match self.group_values.take() {
            Some(rows) => rows,
            None => self.row_converter.empty_rows(0, 0),
        };

        let group_rows = self.row_converter.convert_columns(cols)?;

        let hashes_buffer = &mut self.hashes_buffer;
        hashes_buffer.clear();
        hashes_buffer.resize(group_rows.num_rows(), 0);
        create_hashes(cols, hashes_buffer, &self.random_state)?;

        for (row_idx, hash) in self.hashes_buffer.iter().enumerate() {
            let group_idx = match self.map.get_mut(hash) {
                Some(group_idx) => *group_idx,
                None => {
                    let group_idx = group_values.num_rows();
                    group_values.push(group_rows.row(row_idx));
                    self.map.insert(*hash, group_idx);
                    group_idx
                }
            };
            groups.push(group_idx);
        }

        self.group_values = Some(group_values);

        Ok(())
    }
}
