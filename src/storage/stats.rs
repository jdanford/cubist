#[derive(Debug)]
pub struct StorageStats {
    pub bytes_downloaded: u64,
    pub bytes_uploaded: u64,
    pub get_requests: u64,
    pub put_requests: u64,
    pub delete_requests: u64,
}

impl StorageStats {
    pub fn new() -> Self {
        StorageStats {
            bytes_downloaded: 0,
            bytes_uploaded: 0,
            get_requests: 0,
            put_requests: 0,
            delete_requests: 0,
        }
    }
}
