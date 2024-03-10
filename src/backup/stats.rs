use chrono::{DateTime, Utc};

pub struct Stats {
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub bytes_uploaded: u64,
    pub bytes_read: u64,
    pub blocks_uploaded: u64,
    pub blocks_used: u64,
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            start_time: Utc::now(),
            end_time: None,
            bytes_read: 0,
            bytes_uploaded: 0,
            blocks_used: 0,
            blocks_uploaded: 0,
        }
    }

    pub fn end(&mut self) {
        self.end_time = Some(Utc::now());
    }
}
