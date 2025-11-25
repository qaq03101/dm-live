use bytes::Bytes;
use std::cell::RefCell;
use std::io::{self, IoSlice};
use std::rc::Rc;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::task::yield_now;

use crate::config;
use crate::rtp::RtpPacket;
/// 持有 OwnedWriteHalf 的写入器
///
pub struct TcpWriter {
    /// Tokio Tcp socket 写入半部分
    writer: OwnedWriteHalf,
    /// 为RTP over TCP 预分配的头缓冲区
    headers_buf: Vec<[u8; 4]>,
}

impl TcpWriter {
    pub fn new(writer: OwnedWriteHalf) -> Self {
        Self {
            writer,
            headers_buf: Vec::with_capacity(256),
        }
    }

    /// 发送单个数据块
    pub async fn write(&mut self, data: &Bytes) -> std::io::Result<()> {
        self.writer.write_all(data).await
    }

    /// 批量发送(Tokio::write_vectored)
    ///
    pub async fn write_batch(&mut self, packet: &[Bytes]) -> std::io::Result<()> {
        if packet.is_empty() {
            return Ok(());
        }

        let iovecs: Vec<IoSlice> = packet.iter().map(|b| IoSlice::new(b)).collect();
        let expected: usize = iovecs.iter().map(|iov| iov.len()).sum();
        let written = write_vectored_maybe_mock(&mut self.writer, &iovecs, expected).await?;
        Self::ensure_full_write(written, expected)
    }

    /// 发送一个rtp packet
    pub async fn write_rtp_frame(
        &mut self,
        interleaved: u8,
        rtp_bytes: &Bytes,
    ) -> std::io::Result<()> {
        if self.headers_buf.is_empty() {
            self.headers_buf.push([0; 4]);
        }
        let header = &mut self.headers_buf[0];
        header[0] = b'$';
        header[1] = interleaved;
        header[2] = (rtp_bytes.len() >> 8) as u8;
        header[3] = (rtp_bytes.len() & 0xff) as u8;

        let iovecs = [IoSlice::new(&header[..]), IoSlice::new(rtp_bytes)];
        let expected: usize = iovecs.iter().map(|iov| iov.len()).sum();
        let written = write_vectored_maybe_mock(&mut self.writer, &iovecs, expected).await?;
        Self::ensure_full_write(written, expected)
    }

    /// 批量发送rtp packet
    pub async fn write_rtp_packets(
        &mut self,
        frames: &[(u8, Arc<RtpPacket>)],
    ) -> std::io::Result<()> {
        if frames.is_empty() {
            return Ok(());
        }

        self.headers_buf.resize(frames.len(), [0; 4]);

        // 先构建所有的rtp packet
        for (header, (interleaved, packet)) in self.headers_buf.iter_mut().zip(frames.iter()) {
            header[0] = b'$';
            header[1] = *interleaved;
            let len = packet.raw_packet().len();
            header[2] = (len >> 8) as u8;
            header[3] = (len & 0xff) as u8;
        }

        let total_slices = frames.len() * 2;
        let mut slice_idx = 0;
        let mut offset_in_slice = 0usize;

        // 循环写，直到全部写完
        while slice_idx < total_slices {
            let mut iovecs = Vec::with_capacity(total_slices - slice_idx);
            let mut expected = 0usize;
            for idx in slice_idx..total_slices {
                let is_header = idx % 2 == 0;
                let pair_idx = idx / 2;
                let raw_slice: &[u8] = if is_header {
                    &self.headers_buf[pair_idx]
                } else {
                    frames[pair_idx].1.raw_packet()
                };
                let slice = if idx == slice_idx && offset_in_slice > 0 {
                    &raw_slice[offset_in_slice..]
                } else {
                    raw_slice
                };
                if !slice.is_empty() {
                    expected += slice.len();
                    iovecs.push(IoSlice::new(slice));
                }
            }

            if expected == 0 {
                break;
            }

            let written = write_vectored_maybe_mock(&mut self.writer, &iovecs, expected).await?;
            if written == expected {
                // 全部写完
                break;
            }
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "partial write: wrote 0 bytes",
                ));
            }

            // 推进当前写入位置
            let mut remaining = written;
            while remaining > 0 && slice_idx < total_slices {
                let is_header = slice_idx % 2 == 0;
                let pair_idx = slice_idx / 2;
                let raw_slice: &[u8] = if is_header {
                    &self.headers_buf[pair_idx]
                } else {
                    frames[pair_idx].1.raw_packet()
                };
                let slice_len = raw_slice.len() - offset_in_slice;
                if remaining >= slice_len {
                    remaining -= slice_len;
                    slice_idx += 1;
                    offset_in_slice = 0;
                } else {
                    offset_in_slice += remaining;
                    remaining = 0;
                }
            }
        }

        Ok(())
    }

    /// 确认写入socket完整
    fn ensure_full_write(actual: usize, expected: usize) -> io::Result<()> {
        if actual == expected {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("partial write: expected {expected} bytes, wrote {actual}"),
            ))
        }
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.writer.shutdown().await
    }
}

/// 共享 TCP 写入器 
#[derive(Clone)]
pub struct SharedTcpWriter {
    /// 内部可变的写入器选项
    inner: Rc<RefCell<Option<TcpWriter>>>,
}

impl SharedTcpWriter {
    pub fn new(writer: TcpWriter) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Some(writer))),
        }
    }

    /// 检查写入器是否可用
    pub fn is_available(&self) -> bool {
        self.inner.borrow().is_some()
    }

    /// 获取写入器的独占访问权
    /// loop + yield_now 循环等待，直到获取到写入器
    pub async fn acquire(&self) -> TcpWriterGuard {
        loop {
            if let Some(writer) = { self.inner.borrow_mut().take() } {
                return TcpWriterGuard {
                    shared: self.clone(),
                    writer: Some(writer),
                };
            }
            yield_now().await;
        }
    }

    /// 尝试非阻塞获取写入器，若忙则返回 None。
    pub fn try_acquire(&self) -> Option<TcpWriterGuard> {
        if let Some(writer) = { self.inner.borrow_mut().take() } {
            Some(TcpWriterGuard {
                shared: self.clone(),
                writer: Some(writer),
            })
        } else {
            None
        }
    }
}

/// TCP 写入器守护者
/// 通过 Drop 实现自动归还写入器到共享池
pub struct TcpWriterGuard {
    shared: SharedTcpWriter,
    writer: Option<TcpWriter>,
}

impl Drop for TcpWriterGuard {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            let mut slot = self.shared.inner.borrow_mut();
            slot.replace(writer);
        }
    }
}

impl std::ops::Deref for TcpWriterGuard {
    type Target = TcpWriter;

    fn deref(&self) -> &Self::Target {
        self.writer
            .as_ref()
            .expect("SharedTcpWriter guard missing writer")
    }
}

impl std::ops::DerefMut for TcpWriterGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.writer
            .as_mut()
            .expect("SharedTcpWriter guard missing writer")
    }
}

#[inline]
fn mock_socket_io_enabled() -> bool {
    config::get().io.mock_socket_io
}

async fn write_vectored_maybe_mock(
    writer: &mut OwnedWriteHalf,
    iovecs: &[IoSlice<'_>],
    expected: usize,
) -> io::Result<usize> {
    if mock_socket_io_enabled() {
        Ok(expected)
    } else {
        writer.write_vectored(iovecs).await
    }
}
