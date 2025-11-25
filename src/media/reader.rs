use crate::media::StorageSnapshot;
use crate::rtp::FrameData;
use std::cell::Cell;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::instrument;

/// 帧引用,借用 Storage 切片,或持有独立帧列表
pub enum FrameRef {
    Borrowed {
        storage: Arc<StorageSnapshot>,
        start: usize,
        end: usize,
    },
    Owned(Vec<Arc<FrameData>>),
}

impl FrameRef {
    pub fn len(&self) -> usize {
        match self {
            FrameRef::Borrowed { start, end, .. } => end.saturating_sub(*start),
            FrameRef::Owned(frames) => frames.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[Arc<FrameData>] {
        match self {
            FrameRef::Borrowed {
                storage,
                start,
                end,
            } => {
                let frames: &[Arc<FrameData>] = &storage.frames;
                &frames[*start..*end]
            }
            FrameRef::Owned(frames) => frames.as_slice(),
        }
    }
}

/// 拉流端数据读取器
pub struct Reader {
    /// 会话ID
    session_id: String,
    #[allow(dead_code)]
    /// 过滤的Track ID集合
    track_filter: HashSet<String>,
    /// 当前数据游标
    cursor: Cell<u64>,
    /// 数据回调闭包
    on_data: Box<dyn Fn(FrameRef)>,
}

impl Reader {
    /// 创建新Reader，直接传入回调闭包
    pub fn new<F>(session_id: String, track_filter: HashSet<String>, callback: F) -> Self
    where
        F: Fn(FrameRef) + 'static,
    {
        Self {
            session_id,
            track_filter,
            cursor: Cell::new(0),
            on_data: Box::new(callback),
        }
    }

    /// 获取session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// 获取当前游标
    pub fn cursor(&self) -> u64 {
        self.cursor.get()
    }

    /// 设置游标
    pub fn set_cursor(&self, cursor: u64) {
        self.cursor.set(cursor);
    }

    /// 调用数据闭包流转数据
    #[instrument(
        level = "trace",
        name = "reader_on_frames",
        skip(self, frames),
        fields(session = %self.session_id, frame_count = frames.len())
    )]
    pub fn on_frames(&self, frames: FrameRef) {
        if frames.is_empty() {
            return;
        }

        let frame_count = frames.len();
        let cursor_before = self.cursor();
        // 更新游标
        self.cursor.set(cursor_before + frame_count as u64);
        let cursor_after = self.cursor();

        tracing::debug!(
            target: "reader",
            session = %self.session_id,
            frames = frame_count,
            cursor_before = cursor_before,
            cursor_after = cursor_after,
            "reader consumed frames"
        );

        (self.on_data)(frames);
    }

    /// GOP flush能力 - 发送完整GOP实现秒开
    pub fn flush_gop(&self, gop: crate::media::GopData) {
        let frames = gop.frames;
        if !frames.is_empty() {
            tracing::debug!(
                "Reader执行GOP flush: session={}, frames={}",
                self.session_id,
                frames.len()
            );
            self.on_frames(FrameRef::Owned(frames));
        }
    }
}
