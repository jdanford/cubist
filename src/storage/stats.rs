use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct GetRequestStats {
    pub elapsed_time: Duration,
    pub bytes: u32,
}

#[derive(Debug, Clone)]
pub struct PutRequestStats {
    pub elapsed_time: Duration,
    pub bytes: u32,
}

#[derive(Debug, Clone)]
pub struct DeleteRequestStats {
    pub elapsed_time: Duration,
}

#[derive(Debug, Clone)]
pub struct StorageStats {
    pub bytes_downloaded: u64,
    pub bytes_uploaded: u64,
    pub get_requests: Vec<GetRequestStats>,
    pub put_requests: Vec<PutRequestStats>,
    pub delete_requests: Vec<DeleteRequestStats>,
}

impl StorageStats {
    pub fn new() -> Self {
        StorageStats {
            bytes_downloaded: 0,
            bytes_uploaded: 0,
            get_requests: Vec::new(),
            put_requests: Vec::new(),
            delete_requests: Vec::new(),
        }
    }

    pub fn add_get(&mut self, start_time: Instant, end_time: Instant, bytes: u32) {
        let elapsed_time = end_time - start_time;
        let stats = GetRequestStats {
            elapsed_time,
            bytes,
        };

        self.bytes_downloaded += u64::from(bytes);
        self.get_requests.push(stats);
    }

    pub fn add_put(&mut self, start_time: Instant, end_time: Instant, bytes: u32) {
        let elapsed_time = end_time - start_time;
        let stats = PutRequestStats {
            elapsed_time,
            bytes,
        };

        self.bytes_uploaded += u64::from(bytes);
        self.put_requests.push(stats);
    }

    pub fn add_delete(&mut self, start_time: Instant, end_time: Instant) {
        let elapsed_time = end_time - start_time;
        let stats = DeleteRequestStats { elapsed_time };
        self.delete_requests.push(stats);
    }
}
