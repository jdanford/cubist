use std::sync::Arc;

use tokio::sync::RwLock;

pub fn arc<T>(value: T) -> Arc<T> {
    Arc::new(value)
}

pub fn rwarc<T>(value: T) -> Arc<RwLock<T>> {
    arc(RwLock::new(value))
}

pub fn unarc<T>(arc: Arc<T>) -> T {
    Arc::into_inner(arc).unwrap()
}

pub fn unrwarc<T>(arc: Arc<RwLock<T>>) -> T {
    unarc(arc).into_inner()
}
