use tracing::instrument;

use super::runtime_handle::RuntimeHandle;
use crate::config;
use crate::media::media_source::MediaSourceRegistry;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

// 线程局部变量 - 每个线程独立持有生成器
thread_local! {
    static RNG: RefCell<SmallRng> = RefCell::new(SmallRng::from_entropy());
}

/// RuntimePool - 管理多个 RuntimeHandle 的池
pub struct RuntimePool {
    /// 存储所有 RuntimeHandle 的向量
    handles: Vec<RuntimeHandle>,
    /// 存储每个运行时当前负载的原子计数器向量
    loads: Vec<AtomicUsize>,
}

impl RuntimePool {
    /// 创建一个新的 RuntimePool
    ///
    /// # Arguments
    /// * `count`: 运行时数量，None 表示使用 CPU 核心数
    ///
    /// # Returns
    /// * 'Self' - 新创建的 RuntimePool 实例
    pub fn new(count: Option<usize>) -> Self {
        let settings = config::get();
        let n = count
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or(settings.runtime.fallback_worker_count)
            })
            .max(1);

        tracing::info!("init RuntimePool with {} runtimes", n);

        MediaSourceRegistry::init(n);

        let handles = (0..n).map(RuntimeHandle::new).collect();
        let loads = (0..n).map(|_| AtomicUsize::new(0)).collect();

        Self { handles, loads }
    }

    /// 获取最空闲的 RuntimeHandle
    /// 根据推流+拉流连接数总和选择负载最小的运行时
    ///
    /// # Returns
    /// * `RuntimeHandle` - 负载最小的运行时句柄
    #[instrument(level = "trace", skip(self))]
    pub fn get_handle(&self) -> RuntimeHandle {
        let mut selected_idx = 0;
        let mut selected_load = usize::MAX;
        for (idx, load) in self.loads.iter().enumerate() {
            let load_val = load.load(Ordering::Relaxed);
            if load_val < selected_load {
                selected_load = load_val;
                selected_idx = idx;
            }
        }

        if selected_load != usize::MAX {
            return self.handles[selected_idx].clone();
        }

        let fallback = RNG.with(|rng| rng.borrow_mut().gen_range(0..self.handles.len()));

        tracing::debug!("随机选择 runtime-{}", fallback);
        self.handles[fallback].clone()
    }

    /// 返回所有 RuntimeHandle 的克隆
    pub fn handles(&self) -> Vec<RuntimeHandle> {
        self.handles.clone()
    }

    /// 增加推流连接计数
    pub fn inc_push(&self, handle: &RuntimeHandle) {
        self.loads[handle.idx()].fetch_add(1, Ordering::Relaxed);
    }

    /// 减少推流连接计数
    pub fn dec_push(&self, handle: &RuntimeHandle) {
        self.loads[handle.idx()]
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
            .ok();
    }

    /// 增加拉流连接计数
    pub fn inc_pull(&self, handle: &RuntimeHandle) {
        self.loads[handle.idx()].fetch_add(1, Ordering::Relaxed);
    }

    /// 减少拉流连接计数
    pub fn dec_pull(&self, handle: &RuntimeHandle) {
        self.loads[handle.idx()]
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
            .ok();
    }

    /// 获取运行时数量
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// 判断是否为空
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// 获取指定索引的 RuntimeHandle
    pub fn get_by_idx(&self, idx: usize) -> Option<RuntimeHandle> {
        self.handles.get(idx).cloned()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::Duration;

    #[tokio::test]
    async fn test_runtime_handle_scheduling() {
        let pool = RuntimePool::new(Some(1));
        let handle = Arc::new(pool.get_handle());
        let handle_clone = handle.clone();

        // 记录运行时线程 ID
        let runtime_thread_id = Arc::new(std::sync::Mutex::new(None));
        let runtime_thread_id_for_task = runtime_thread_id.clone();
        let runtime_thread_id_for_task_clone = runtime_thread_id_for_task.clone();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        handle.spawn_local(async move {
            let handle = handle_clone.clone();
            let runtime_thread_id_for_task = runtime_thread_id_for_task_clone.clone();

            let id = thread::current().id();
            *runtime_thread_id_for_task.lock().unwrap() = Some(id);

            let counter_inner = counter_clone.clone();
            handle.spawn_local_current(async move {
                let current_id = thread::current().id();
                let stored_id = runtime_thread_id_for_task.lock().unwrap().unwrap();
                assert_eq!(current_id, stored_id);
                counter_inner.fetch_add(1, Ordering::SeqCst);
            });

            let runtime_thread_id_for_task = runtime_thread_id_for_task_clone.clone();
            let counter_for_spawn = counter_clone.clone();
            let runtime_thread_id_for_spawn = runtime_thread_id_for_task.clone();
            handle.spawn(async move {
                let current_id = thread::current().id();
                let stored_id = runtime_thread_id_for_spawn.lock().unwrap().unwrap();
                assert_eq!(current_id, stored_id);
                counter_for_spawn.fetch_add(1, Ordering::SeqCst);
            });

            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        let counter_for_cross_thread = counter.clone();
        let runtime_thread_id_for_cross = runtime_thread_id.clone();
        handle.spawn_local(async move {
            let current_id = thread::current().id();
            let stored_id = runtime_thread_id_for_cross.lock().unwrap().unwrap();
            assert_eq!(current_id, stored_id);
            counter_for_cross_thread.fetch_add(1, Ordering::SeqCst);
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(counter.load(Ordering::SeqCst), 4);

        assert!(runtime_thread_id.lock().unwrap().is_some());
    }
}
