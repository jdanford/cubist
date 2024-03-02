mod cloud;
mod core;
mod local;

pub use {cloud::CloudStorage, core::Storage, local::LocalStorage};

pub type BoxedStorage = Box<dyn Storage + Sync + Send + 'static>;
