pub mod opener;
pub mod options;
pub mod source;

/// How many records should be read in order to infer the CSV schema.
pub const MAX_INFER_RECORDS: usize = 100;

/// The default number of CSV records to read per batch.
pub const DEFAULT_BATCH_SIZE: usize = 1024;
