use arrow::compute::interleave;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::Result;

#[derive(Debug, Clone, Default)]
struct BatchCursor {
    batch_index: usize,
    row_index: usize,
}

#[derive(Debug)]
pub struct BatchBuilder {
    schema: SchemaRef,
    batches: Vec<(usize, RecordBatch)>,
    cursors: Vec<BatchCursor>,
    indices: Vec<(usize, usize)>,
}

impl BatchBuilder {
    pub fn new(schema: SchemaRef, stream_count: usize, batch_size: usize) -> Self {
        Self {
            schema,
            batches: Vec::with_capacity(stream_count * 2),
            cursors: vec![BatchCursor::default(); stream_count],
            indices: Vec::with_capacity(batch_size),
        }
    }

    pub fn push_batch(&mut self, stream_idx: usize, batch: RecordBatch) {
        let batch_index = self.batches.len();
        self.batches.push((stream_idx, batch));
        self.cursors[stream_idx] = BatchCursor {
            batch_index,
            row_index: 0,
        };
    }

    pub fn push_row(&mut self, stream_idx: usize) {
        let cursor = &mut self.cursors[stream_idx];
        let row_index = cursor.row_index;
        cursor.row_index += 1;
        self.indices.push((cursor.batch_index, row_index));
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        let columns = (0..self.schema.fields().len())
            .map(|col_idx| {
                let arrays = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.column(col_idx).as_ref())
                    .collect::<Vec<_>>();
                Ok(interleave(&arrays, &self.indices)?)
            })
            .collect::<Result<Vec<_>>>()?;

        self.indices.clear();
        let mut batch_index = 0;
        let mut retained = 0;
        self.batches.retain(|(stream_idx, _)| {
            let stream_cursor = &mut self.cursors[*stream_idx];
            let retain = stream_cursor.batch_index == batch_index;
            batch_index += 1;

            if retain {
                stream_cursor.batch_index = retained;
                retained += 1;
            };
            retain
        });

        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}
