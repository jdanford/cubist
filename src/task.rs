use std::{future::Future, sync::Arc};

use tokio::{
    sync::{AcquireError, Semaphore},
    task::{AbortHandle, JoinError, JoinSet},
};

pub struct BoundedJoinSet<T> {
    semaphore: Arc<Semaphore>,
    join_set: JoinSet<T>,
}

impl<T: 'static> BoundedJoinSet<T> {
    pub fn new(max_tasks: usize) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_tasks));
        let join_set = JoinSet::new();
        BoundedJoinSet {
            semaphore,
            join_set,
        }
    }

    pub async fn spawn<F>(&mut self, task: F) -> Result<AbortHandle, AcquireError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send,
    {
        let permit = self.semaphore.clone().acquire_owned().await?;
        let handle = self.join_set.spawn(async move {
            let value = task.await;
            drop(permit);
            value
        });
        Ok(handle)
    }

    pub async fn spawn_blocking<F>(&mut self, task: F) -> Result<AbortHandle, AcquireError>
    where
        F: (FnOnce() -> T) + Send + 'static,
        T: Send,
    {
        let permit = self.semaphore.clone().acquire_owned().await?;
        let handle = self.join_set.spawn_blocking(move || {
            let value = task();
            drop(permit);
            value
        });
        Ok(handle)
    }

    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.join_set.join_next().await
    }

    pub fn try_join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.join_set.try_join_next()
    }
}
