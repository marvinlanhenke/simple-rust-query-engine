use std::{fs::File, io::Read};

use async_stream::try_stream;
use futures::StreamExt;

use super::options::CsvFileOpenerConfig;
use crate::{
    error::Result,
    io::{FileOpener, RecordBatchStream},
};

/// Responsible for opening CSV files with provided configurations.
///
/// Implements the [`FileOpener`] trait, allowing for async reading of
/// CSV files, by wrapping the [`arrow::csv::Reader`] with a boxed stream,
/// yielding instances of [`arrow::array::RecordBatch`].
#[derive(Debug)]
pub struct CsvFileOpener {
    config: CsvFileOpenerConfig,
}

impl CsvFileOpener {
    /// Creates a [`CsvFileOpener`] with specified configuration.
    pub fn new(config: CsvFileOpenerConfig) -> Self {
        Self { config }
    }

    /// Creates a CSV reader from the given input source.
    fn reader<R: Read>(&self, reader: R) -> Result<arrow::csv::Reader<R>> {
        let mut builder = arrow::csv::ReaderBuilder::new(self.config.schema().clone())
            .with_batch_size(self.config.batch_size())
            .with_header(self.config.has_header())
            .with_delimiter(self.config.delimiter())
            .with_quote(self.config.quote());
        if let Some(projection) = self.config.projection() {
            builder = builder.with_projection(projection.clone());
        }

        Ok(builder.build(reader)?)
    }
}

impl FileOpener for CsvFileOpener {
    /// Opens the file at the specified path and creates a boxed stream
    /// of `RecordBatch` results from a CSV reader.
    ///
    /// Returns a pinned future that yields the boxed stream.
    fn open(&self, path: &str) -> Result<RecordBatchStream> {
        let file = File::open(path)?;
        let reader = self.reader(file)?;

        let stream = try_stream! {
            for batch in reader {
                yield batch?
            }
        };

        Ok(stream.boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::{
        io::{
            reader::csv::{opener::CsvFileOpener, options::CsvFileOpenerConfig},
            FileOpener,
        },
        tests::create_schema,
    };

    #[tokio::test]
    async fn test_csv_file_opener() {
        let schema = Arc::new(create_schema());
        let config = CsvFileOpenerConfig::new(schema);
        let opener = CsvFileOpener::new(config);

        let mut stream = opener.open("testdata/csv/simple.csv").unwrap();
        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 6);
            assert_eq!(batch.num_columns(), 3);
        }
    }

    #[tokio::test]
    async fn test_csv_file_opener_batch_size_one() {
        let schema = Arc::new(create_schema());
        let config = CsvFileOpenerConfig::builder(schema)
            .with_batch_size(1)
            .build();
        let opener = CsvFileOpener::new(config);

        let mut stream = opener.open("testdata/csv/simple.csv").unwrap();
        while let Some(Ok(batch)) = stream.next().await {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 3);
        }
    }
}
