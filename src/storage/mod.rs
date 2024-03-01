mod cloud;
mod local;
mod core;

pub use {cloud::CloudStorage, local::LocalStorage, core::Storage};

pub type BoxedStorage = Box<dyn Storage + Sync + Send + 'static>;
