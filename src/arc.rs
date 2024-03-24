use std::sync::Arc;

use tokio::sync::RwLock;

pub fn rwarc<T>(value: T) -> Arc<RwLock<T>> {
    Arc::new(RwLock::new(value))
}

pub fn unrwarc<T>(arc: Arc<RwLock<T>>) -> T {
    unarc(arc).into_inner()
}

pub fn unarc<T>(arc: Arc<T>) -> T {
    Arc::into_inner(arc).unwrap()
}
