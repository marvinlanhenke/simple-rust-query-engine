use std::collections::HashMap;

use ahash::RandomState;
use arrow::{
    array::{downcast_array, ArrayAccessor, ArrayRef, BooleanArray, PrimitiveArray, StringArray},
    datatypes::{ArrowPrimitiveType, DataType, SchemaRef},
    downcast_primitive_array,
    row::{RowConverter, Rows, SortField},
};
use arrow_array::Array;
use snafu::location;

use crate::{
    error::{Error, Result},
    utils::HashValue,
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
        self.create_hashes(cols, group_rows.num_rows())?;

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

    /// Generates hashes for each row based on the provided columns,
    /// supporting various data types.
    fn create_hashes(&mut self, cols: &[ArrayRef], num_rows: usize) -> Result<()> {
        self.hashes_buffer.clear();
        self.hashes_buffer.resize(num_rows, 0);

        for col in cols.iter() {
            let array = col.as_ref();

            downcast_primitive_array!(
                array => self.hash_primitive_array(array),
                DataType::Null => self
                    .hashes_buffer
                    .iter_mut()
                    .for_each(|hash| *hash = self.random_state.hash_one(1)),
                DataType::Boolean => {
                    let array = downcast_array::<BooleanArray>(array);
                    self.hash_array(&array);
                }
                DataType::Utf8 => {
                    let array = downcast_array::<StringArray>(array);
                    self.hash_array(&array);
                }
                other => return Err(
                    Error::InvalidOperation {
                        message: format!("data type {} not supported in hasher", other),
                        location: location!()
                    })
            );
        }

        Ok(())
    }

    /// Hashes non-null values of an array, updating the hash buffer.
    fn hash_array<T>(&mut self, array: T)
    where
        T: ArrayAccessor,
        T::Item: HashValue,
    {
        for (idx, hash) in self.hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(idx) {
                let value = array.value(idx);
                *hash = value.hash_one(&self.random_state);
            }
        }
    }

    /// Specifically hashes values in a primitive array, considering nullability.
    fn hash_primitive_array<T>(&mut self, array: &PrimitiveArray<T>)
    where
        T: ArrowPrimitiveType,
        <T as arrow_array::ArrowPrimitiveType>::Native: HashValue,
    {
        for (idx, hash) in self.hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(idx) {
                let value = array.value(idx);
                *hash = value.hash_one(&self.random_state);
            }
        }
    }
}
