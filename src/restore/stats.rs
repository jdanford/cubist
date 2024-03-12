use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct Stats {
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub bytes_downloaded: u64,
    pub bytes_written: u64,
    pub blocks_downloaded: u64,
    pub blocks_used: u64,
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            start_time: Utc::now(),
            end_time: None,
            bytes_downloaded: 0,
            bytes_written: 0,
            blocks_downloaded: 0,
            blocks_used: 0,
        }
    }

    pub fn end(&mut self) {
        self.end_time = Some(Utc::now());
    }
}
