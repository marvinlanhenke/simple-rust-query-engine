use ahash::RandomState;
use arrow::array::downcast_array;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::downcast_primitive_array;
use arrow_array::{ArrayAccessor, ArrayRef, BooleanArray, PrimitiveArray, StringArray};

use arrow_array::Array;
use arrow_schema::DataType;
use snafu::location;

use crate::{
    error::{Error, Result},
    utils::HashValue,
};

/// Computes the hash for each array and stores it inside a `hash_buffer`.
pub fn create_hashes<'a>(
    arrays: &[ArrayRef],
    hashes_buffer: &'a mut Vec<u64>,
    random_state: &RandomState,
) -> Result<&'a mut Vec<u64>> {
    for col in arrays.iter() {
        let array = col.as_ref();

        downcast_primitive_array!(
            array => hash_primitive_array(array, hashes_buffer, random_state),
            DataType::Null =>
                hashes_buffer
                .iter_mut()
                .for_each(|hash| *hash = random_state.hash_one(1)),
            DataType::Boolean => {
                let array = downcast_array::<BooleanArray>(array);
                hash_array(&array, hashes_buffer, random_state);
            }
            DataType::Utf8 => {
                let array = downcast_array::<StringArray>(array);
                hash_array(&array, hashes_buffer, random_state);
            }
            other => return Err(
                Error::InvalidOperation {
                    message: format!("data type {} not supported in hasher", other),
                    location: location!()
                })
        );
    }
    Ok(hashes_buffer)
}

/// Hashes non-null values of an array, updating the hash buffer.
fn hash_array<T>(array: T, hashes_buffer: &mut [u64], random_state: &RandomState)
where
    T: ArrayAccessor,
    T::Item: HashValue,
{
    for (idx, hash) in hashes_buffer.iter_mut().enumerate() {
        if !array.is_null(idx) {
            let value = array.value(idx);
            *hash = value.hash_one(random_state);
        }
    }
}

/// Specifically hashes values in a primitive array, considering nullability.
fn hash_primitive_array<T>(
    array: &PrimitiveArray<T>,
    hashes_buffer: &mut [u64],
    random_state: &RandomState,
) where
    T: ArrowPrimitiveType,
    <T as arrow_array::ArrowPrimitiveType>::Native: HashValue,
{
    for (idx, hash) in hashes_buffer.iter_mut().enumerate() {
        if !array.is_null(idx) {
            let value = array.value(idx);
            *hash = value.hash_one(random_state);
        }
    }
}