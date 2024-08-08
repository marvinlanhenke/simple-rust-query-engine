use arrow::row::Rows;

/// A comparable cursor, used by sort operations
///
/// A `Cursor` is a pointer into a collection of rows with a tracked offset.
#[derive(Debug)]
pub struct Cursor {
    /// The current offset within the rows.
    offset: usize,
    /// A row-oriented representation of arrow data, that is normalized for comparison.
    rows: Rows,
}

impl Cursor {
    /// Creates a new [`Cursor`] instance.
    pub fn new(rows: Rows) -> Self {
        Self { offset: 0, rows }
    }

    /// Checks if the cursor has finished iterating over all rows.
    pub fn is_finished(&self) -> bool {
        self.offset == self.rows.num_rows()
    }

    /// Advances the cursor to the next row and
    /// returns the previous offset.
    pub fn advance(&mut self) -> usize {
        let prev = self.offset;
        self.offset += 1;
        prev
    }
}

impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        let lhs = self.rows.row(self.offset);
        let rhs = other.rows.row(other.offset);
        lhs == rhs
    }
}

impl Eq for Cursor {}

impl PartialOrd for Cursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let lhs = self.rows.row(self.offset);
        let rhs = other.rows.row(other.offset);
        lhs.cmp(&rhs)
    }
}
