pub mod hash_join;
pub mod nested_loop_join;
pub mod utils;

/// Default batch size for processing operations in a join operation.
/// Determines the number of rows processed in each batch, emitted by [`HashJoinExec`].
pub const DEFAULT_BATCH_SIZE: usize = 1024;
