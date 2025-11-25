use crossbeam_queue::SegQueue;
use std::cell::Cell;
use std::sync::Arc;
use std::thread::ThreadId;
use tokio::runtime::{Builder, Handle as TokioRuntimeHandle};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, LocalSet};

// 每个 runtime 线程写入自身的线程 `idx`.
thread_local! {
    static CURRENT_RUNTIME_ID: Cell<Option<usize>> = Cell::new(None);
}

/// 运行时句柄
#[derive(Clone, Debug)]
pub struct RuntimeHandle {
    runtime_context: Arc<RuntimeHandleContext>,
}

/// 运行时上下文
#[derive(Debug)]
struct RuntimeHandleContext {
    /// 运行时索引
    idx: usize,
    /// 运行此 runtime 的线程 ID
    thread_id: ThreadId,
    /// 跨线程提交的待执行任务队列
    task_queue: Arc<SegQueue<Box<dyn FnOnce() + Send>>>,
    /// 用于唤醒 runtime 线程处理队列的通知器
    notifier: Arc<Notify>,
    /// 内部 tokio runtime handle
    tokio_handle: TokioRuntimeHandle,
}

impl RuntimeHandle {
    /// 创建指定索引的 RuntimeHandle, 并启动对应线程, 循环消费投递的任务.
    ///
    /// # Arguments
    /// * `idx` - 运行时索引
    ///
    /// # Returns
    /// - `RuntimeHandle` 实例
    pub fn new(idx: usize) -> Self {
        let task_queue: Arc<SegQueue<Box<dyn FnOnce() + Send>>> = Arc::new(SegQueue::new());
        let queue_for_thread = task_queue.clone();
        let notifier = Arc::new(Notify::new());
        let notifier_for_thread = notifier.clone();

        // 在线程内获取线程信息返回给主线程记录
        let (thread_init_tx, thread_init_rx) = std::sync::mpsc::channel();

        std::thread::Builder::new()
            .name(format!("runtime-{}", idx))
            .spawn(move || {
                CURRENT_RUNTIME_ID.with(|id| id.set(Some(idx)));

                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create current_thread runtime");

                let local = LocalSet::new();
                let tokio_handle = rt.handle().clone();
                let thread_id = std::thread::current().id();
                thread_init_tx.send((thread_id, tokio_handle)).ok();

                let queue = queue_for_thread;
                let notify = notifier_for_thread;
                local.block_on(&rt, async move {
                    loop {
                        while let Some(task) = queue.pop() {
                            // task()等价tokio::task::spawn_local(fut),
                            // 这里只会向localset中提交真正的fut任务
                            task();
                        }
                        notify.notified().await;
                    }
                })
            })
            .expect("Failed to spawn runtime thread");

        let (thread_id, tokio_handle) = thread_init_rx
            .recv()
            .expect("Failed to receive thread init data");

        Self {
            runtime_context: Arc::new(RuntimeHandleContext {
                idx,
                thread_id,
                task_queue,
                notifier,
                tokio_handle,
            }),
        }
    }

    /// 在当前运行时单线程上执行闭包
    /// 由其他线程进行跨线程投递任务.
    ///
    /// # Arguments
    /// * `fut` - 投递的future任务, 需要Send.
    pub fn spawn_local<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let task = Box::new(move || {
            tokio::task::spawn_local(fut);
        });

        self.runtime_context.task_queue.push(task);
        self.runtime_context.notifier.notify_one();
    }

    /// 进行跨线程提交监听任务, 在目标runtime线程内执行请求监听循环
    ///
    /// # Arguments
    /// * `factory` - 返回 !Send Future 的闭包.
    pub fn spawn_local_factory<F, Fut>(&self, factory: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        let task = Box::new(move || {
            let fut = factory();
            tokio::task::spawn_local(fut);
        });

        self.runtime_context.task_queue.push(task);
        self.runtime_context.notifier.notify_one();
    }

    /// 在当前运行时上执行本地任务,
    /// 由 runtime 所在线程调用.
    ///
    /// # Arguments
    /// * `fut` - 本地任务, 不需要 Send.
    pub fn spawn_local_current<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + 'static,
    {
        if self.is_current() {
            tokio::task::spawn_local(fut);
        } else {
            tracing::error!(
                "spawn_local_current called from non-runtime thread (runtime-{})",
                self.runtime_context.idx
            );
        }
    }

    /// 直接使用Tokio runtime提交Send任务，跳过了sequeue
    ///
    ///
    /// # Arguments
    /// * `fut` - 需要Send的Future任务
    ///
    /// # Returns
    /// - `JoinHandle<F::Output>` - 任务句柄
    pub fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime_context.tokio_handle.spawn(fut)
    }

    /// 判断是否在当前运行时线程中
    ///
    /// # Returns
    /// - `bool` - 是否在当前运行时线程中
    pub fn is_current(&self) -> bool {
        std::thread::current().id() == self.runtime_context.thread_id
    }

    pub fn thread_id(&self) -> ThreadId {
        self.runtime_context.thread_id
    }

    /// 获取运行时索引
    ///
    /// # Returns
    /// - `usize` - 运行时索引
    pub fn idx(&self) -> usize {
        self.runtime_context.idx
    }
}
