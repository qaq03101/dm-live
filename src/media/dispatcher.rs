use crate::config;
use crate::event::RuntimeHandle;
use crate::media::reader::FrameRef;
use crate::media::{GopData, Reader, StorageSnapshot};
use crate::rtp::FrameData;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// Dispatcher 事件类型
pub enum DispatcherEvent {
    Update {
        stream_key: Arc<String>,
        storage: Arc<StorageSnapshot>,
    },
}

/// 每个流的上下文
struct StreamContext {
    dispatcher: Rc<RefCell<Dispatcher>>,
}

/// 每个运行时的分片数据分发器
///
/// 解耦原本的Dispacher直接与RingBuffer通过watch进行数据更新通知
/// 降低大量流存在时,更新时channel造成的的惊群问题.
struct ShardDispatcher {
    /// 分片ID
    shard_id: usize,
    /// 流分发器映射表
    dispatchers: HashMap<Arc<String>, StreamContext>,
}

impl ShardDispatcher {
    fn new(shard_id: usize) -> Self {
        Self {
            shard_id,
            dispatchers: HashMap::new(),
        }
    }

    /// 获取或创建 Dispatcher
    fn get_or_create_dispatcher(
        &mut self,
        stream_key: Arc<String>,
        runtime_id: usize,
        storage: Arc<StorageSnapshot>,
    ) -> Rc<RefCell<Dispatcher>> {
        self.dispatchers
            .entry(stream_key.clone())
            .or_insert_with(|| {
                tracing::info!(
                    "创建Dispatcher: stream={}, runtime={}, shard={}",
                    stream_key,
                    runtime_id,
                    self.shard_id
                );
                StreamContext {
                    dispatcher: Rc::new(RefCell::new(Dispatcher::new(runtime_id, storage.clone()))),
                }
            })
            .dispatcher
            .clone()
    }

    fn remove(&mut self, stream_key: &str) {
        if self
            .dispatchers
            .remove(&Arc::new(stream_key.to_string()))
            .is_some()
        {
            tracing::info!(
                "移除Dispatcher: stream={}, shard={}",
                stream_key,
                self.shard_id
            );
        }
    }

    // 新数据更新
    fn on_update(&mut self, stream_key: Arc<String>, storage: Arc<StorageSnapshot>) {
        if let Some(ctx) = self.dispatchers.get(&stream_key) {
            ctx.dispatcher.borrow_mut().dispatch(storage);
        }
    }
}

// 每个runtime线程维护自己的分片集合
thread_local! {
    static LOCAL_SHARDS: RefCell<Option<Vec<Rc<RefCell<ShardDispatcher>>>>> = RefCell::new(None);
}

// 全局：runtime_id -> shard senders
pub static GLOBAL_SHARD_SENDERS: Lazy<DashMap<usize, Arc<Vec<UnboundedSender<DispatcherEvent>>>>> =
    Lazy::new(DashMap::new);

/// 每个运行时的数据分发器
pub struct Dispatcher {
    /// 运行时ID
    runtime_id: usize,
    /// 当前数据存储快照
    storage: Arc<StorageSnapshot>,
    /// 注册的Reader列表
    readers: HashMap<String, Reader>,
}

impl Dispatcher {
    /// 创建新的Dispatcher
    pub fn new(runtime_id: usize, storage: Arc<StorageSnapshot>) -> Self {
        Self {
            runtime_id,
            storage,
            readers: HashMap::new(),
        }
    }

    /// 初始化runtime的分片和router任务
    pub fn init_runtime(runtime: RuntimeHandle) {
        let runtime_id = runtime.idx();
        // 已初始化则跳过
        if GLOBAL_SHARD_SENDERS.contains_key(&runtime_id) {
            return;
        }
        let shard_count = config::get().dispatcher.shard_count;
        let runtime_for_task = runtime.clone();
        let init_local = move || {
            LOCAL_SHARDS.with(|local| {
                if local.borrow().is_some() {
                    return;
                }

                let mut senders = Vec::with_capacity(shard_count);
                let mut receivers = Vec::with_capacity(shard_count);
                for shard_id in 0..shard_count {
                    let (tx, rx) = unbounded_channel::<DispatcherEvent>();
                    senders.push(tx);
                    receivers.push((shard_id, rx));
                }
                let sender_arc = Arc::new(senders);
                GLOBAL_SHARD_SENDERS.insert(runtime_id, sender_arc);

                let mut shards = Vec::with_capacity(shard_count);

                for (shard_id, rx) in receivers {
                    let shard = Rc::new(RefCell::new(ShardDispatcher::new(shard_id)));
                    let shard_task = shard.clone();
                    runtime_for_task.spawn_local_current(async move {
                        Self::run_shard(shard_task, rx).await;
                    });
                    shards.push(shard);
                }

                local.replace(Some(shards));
            });
        };

        if runtime.is_current() {
            init_local();
        } else {
            runtime.spawn_local(async move {
                init_local();
            });
        }
    }

    async fn run_shard(
        shard: Rc<RefCell<ShardDispatcher>>,
        mut rx: UnboundedReceiver<DispatcherEvent>,
    ) {
        while let Some(event) = rx.recv().await {
            match event {
                DispatcherEvent::Update {
                    stream_key,
                    storage,
                } => {
                    shard.borrow_mut().on_update(stream_key, storage);
                }
            }
        }
    }

    /// 获取runtime ID
    pub fn runtime_id(&self) -> usize {
        self.runtime_id
    }

    /// 注册Reader
    pub fn register_reader(&mut self, reader: Reader) -> anyhow::Result<()> {
        let session_id = reader.session_id().to_string();
        tracing::debug!(
            "注册Reader到Dispatcher: runtime={}, session={}",
            self.runtime_id,
            session_id
        );
        self.readers.insert(session_id, reader);
        Ok(())
    }

    /// 注销Reader
    pub fn unregister_reader(&mut self, session_id: &str) {
        self.readers.remove(session_id);
        tracing::debug!(
            "从Dispatcher注销Reader: runtime={}, session={}",
            self.runtime_id,
            session_id
        );
    }

    /// 获取Reader数量
    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    /// 数据分发,被RingBuffer回调唤醒时调用
    /// 接收新的Storage快照，遍历所有Reader分发数据
    pub fn dispatch(&mut self, new_storage: Arc<StorageSnapshot>) {
        // 更新Storage引用
        self.storage = new_storage;

        // 遍历所有Reader分发数据
        for (_session_id, reader) in &self.readers {
            // 在新storage读取该reader游标之后的数据
            let mut cursor = reader.cursor();
            let window_start = self.storage.start_seq;
            let window_end = self.storage.end_seq;
            if cursor < window_start {
                reader.set_cursor(window_start);
                cursor = window_start;
            } else if cursor > window_end {
                reader.set_cursor(window_end);
                cursor = window_end;
            }

            if let Some((start, end)) = Self::read_frame_range(
                &self.storage,
                cursor,
                config::get().dispatcher.frame_batch_size,
            ) {
                reader.on_frames(FrameRef::Borrowed {
                    storage: self.storage.clone(),
                    start,
                    end,
                });
            }
        }
    }

    /// 从Storage读取指定游标之后的帧
    fn read_frame_range(
        storage: &Arc<StorageSnapshot>,
        cursor: u64,
        limit: usize,
    ) -> Option<(usize, usize)> {
        if storage.is_empty() || storage.frames.is_empty() {
            return None;
        }

        let mut effective_cursor = cursor.max(storage.start_seq);
        if effective_cursor > storage.end_seq {
            effective_cursor = storage.end_seq;
        }

        let offset = (effective_cursor.saturating_sub(storage.start_seq)) as usize;
        let frames: &[Arc<FrameData>] = &storage.frames;
        if offset >= frames.len() {
            return None;
        }

        let end = (offset + limit).min(frames.len());
        Some((offset, end))
    }

    /// 更新Storage, 但不分发
    pub fn update_storage(&mut self, storage: Arc<StorageSnapshot>) {
        self.storage = storage;
    }

    /// 获取最新完整GOP
    /// 返回倒数第二个GOP
    pub fn latest_complete_gop(&self) -> Option<Arc<GopData>> {
        if self.storage.len() >= 2 {
            self.storage.gops.get(self.storage.len() - 2).cloned()
        } else {
            None
        }
    }

    /// 通过hash计算分片索引
    fn shard_idx(stream_key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        stream_key.hash(&mut hasher);
        (hasher.finish() as usize) % config::get().dispatcher.shard_count
    }

    pub fn shard_index(stream_key: &str) -> usize {
        Self::shard_idx(stream_key)
    }

    pub fn shard_senders(runtime_id: usize) -> Option<Arc<Vec<UnboundedSender<DispatcherEvent>>>> {
        GLOBAL_SHARD_SENDERS
            .get(&runtime_id)
            .map(|v| v.value().clone())
    }

    /// 获取或创建Dispatcher
    pub fn get_or_create(
        stream_key: &str,
        runtime_id: usize,
        storage: Arc<StorageSnapshot>,
    ) -> Rc<RefCell<Self>> {
        // 计算分片索引
        let shard_idx = Self::shard_idx(stream_key);

        LOCAL_SHARDS.with(|local| {
            let borrowed = local.borrow();
            let shards = borrowed
                .as_ref()
                .expect("LOCAL_SHARDS not initialized for this runtime");
            let shard = shards
                .get(shard_idx)
                .unwrap_or_else(|| panic!("shard {} not found", shard_idx))
                .clone();
            shard.borrow_mut().get_or_create_dispatcher(
                Arc::new(stream_key.to_string()),
                runtime_id,
                storage,
            )
        })
    }

    /// 从thread_local分片中移除Dispatcher
    pub fn remove(stream_key: &str) {
        let shard_idx = Self::shard_idx(stream_key);
        LOCAL_SHARDS.with(|local| {
            if let Some(shards) = local.borrow_mut().as_mut() {
                if let Some(shard) = shards.get_mut(shard_idx) {
                    shard.borrow_mut().remove(stream_key);
                }
            }
        });
    }

    /// 获取当前线程的Dispatcher数量
    pub fn local_count() -> usize {
        LOCAL_SHARDS.with(|local| {
            local
                .borrow()
                .as_ref()
                .map(|shards| shards.iter().map(|s| s.borrow().dispatchers.len()).sum())
                .unwrap_or(0)
        })
    }
}
