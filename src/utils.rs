use ahash::RandomState;
use arrow::datatypes::{i256, IntervalDayTime, IntervalMonthDayNano};

pub trait HashValue {
    fn hash_one(&self, state: &RandomState) -> u64;
}

macro_rules! hash_value {
    ($($t:ty),+) => {
       $(impl HashValue for $t {
        fn hash_one(&self, state: &RandomState) -> u64 {
        state.hash_one(self)
        }
        })+
    };
}

hash_value!(i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128);
hash_value!(bool, &str, str, [u8], IntervalDayTime, IntervalMonthDayNano);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(<$i>::from_ne_bytes(self.to_ne_bytes()))
            }
        })+
    };
}
hash_float_value!((half::f16, u16), (f32, u32), (f64, u64));
