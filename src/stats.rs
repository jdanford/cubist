use std::time::Duration;

use chrono::{DateTime, Utc};
use humansize::{ToF64, Unsigned, DECIMAL};

#[derive(Debug)]
pub struct Stats {
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub files_read: u64,
    pub files_created: u64,
    pub blocks_downloaded: u64,
    pub blocks_uploaded: u64,
    pub blocks_used: u64,
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            start_time: Utc::now(),
            end_time: None,
            bytes_read: 0,
            bytes_written: 0,
            files_read: 0,
            files_created: 0,
            blocks_downloaded: 0,
            blocks_uploaded: 0,
            blocks_used: 0,
        }
    }

    pub fn end(&mut self) -> Duration {
        let end_time = Utc::now();
        self.end_time = Some(end_time);
        let delta = end_time - self.start_time;
        let ms = delta.num_milliseconds().try_into().unwrap();
        Duration::from_millis(ms)
    }
}

pub fn format_size<T: ToF64 + Unsigned>(input: T) -> String {
    humansize::format_size(input, DECIMAL)
}
