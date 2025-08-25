// src/util/pool.rs
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub struct ObjectPool<T> {
    objects: Arc<Mutex<VecDeque<T>>>,
    creator: Box<dyn Fn() -> T + Send + Sync>,
    max_size: usize,
}

impl<T> ObjectPool<T> {
    pub fn new<F>(creator: F, max_size: usize) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        Self {
            objects: Arc::new(Mutex::new(VecDeque::new())),
            creator: Box::new(creator),
            max_size,
        }
    }

    pub fn get(&self) -> PooledObject<T> {
        let mut objects = self.objects.lock().unwrap();
        let object = objects.pop_front().unwrap_or_else(|| (self.creator)());

        PooledObject {
            object: Some(object),
            pool: self.objects.clone(),
        }
    }
}

pub struct PooledObject<T> {
    object: Option<T>,
    pool: Arc<Mutex<VecDeque<T>>>,
}

impl<T> std::ops::Deref for PooledObject<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.object.as_ref().unwrap()
    }
}

impl<T> std::ops::DerefMut for PooledObject<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.object.as_mut().unwrap()
    }
}

impl<T> Drop for PooledObject<T> {
    fn drop(&mut self) {
        if let Some(object) = self.object.take() {
            let mut pool = self.pool.lock().unwrap();
            if pool.len() < 10 {
                // 限制池大小
                pool.push_back(object);
            }
        }
    }
}
