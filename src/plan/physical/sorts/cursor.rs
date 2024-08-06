use arrow::row::Rows;

#[derive(Debug)]
pub struct Cursor {
    offset: usize,
    rows: Rows,
}

impl Cursor {
    pub fn new(rows: Rows) -> Self {
        Self { offset: 0, rows }
    }

    pub fn is_finished(&self) -> bool {
        self.offset == self.rows.num_rows()
    }

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
