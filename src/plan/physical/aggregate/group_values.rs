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

#[derive(Debug)]
pub struct GroupValues {
    schema: SchemaRef,
    row_converter: RowConverter,
    map: HashMap<u64, usize>,
    group_values: Option<Rows>,
    random_state: RandomState,
    hashes_buffer: Vec<u64>,
}

impl GroupValues {
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

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

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
