use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tracing::instrument;

use crate::config;

#[cfg(target_os = "linux")]
use std::cell::RefCell;

#[cfg(target_os = "linux")]
struct SendBuffers {
    iovecs: Vec<libc::iovec>,
    addrs_v4: Vec<libc::sockaddr_in>,
    addrs_v6: Vec<libc::sockaddr_in6>,
    hdrs: Vec<libc::msghdr>,
    msgs: Vec<libc::mmsghdr>,
}

#[cfg(target_os = "linux")]
impl SendBuffers {
    fn new(capacity: usize) -> Self {
        Self {
            iovecs: Vec::with_capacity(capacity),
            addrs_v4: Vec::with_capacity(capacity),
            addrs_v6: Vec::with_capacity(capacity),
            hdrs: Vec::with_capacity(capacity),
            msgs: Vec::with_capacity(capacity),
        }
    }

    fn clear(&mut self) {
        self.iovecs.clear();
        self.addrs_v4.clear();
        self.addrs_v6.clear();
        self.hdrs.clear();
        self.msgs.clear();
    }
}

/// UDP 写入器
///
/// Linux 上使用 sendmmsg 批量发送，其他平台回退到循环发送
pub struct UdpWriter {
    socket: Arc<UdpSocket>,
    #[cfg(target_os = "linux")]
    max_batch: usize,
    #[cfg(target_os = "linux")]
    send_buffers: RefCell<SendBuffers>,
}

impl UdpWriter {
    pub fn new(socket: Arc<UdpSocket>) -> Self {
        #[cfg(target_os = "linux")]
        let max_batch = config::get().rtp.udp.linux_max_batch;

        Self {
            socket,
            #[cfg(target_os = "linux")]
            max_batch,
            #[cfg(target_os = "linux")]
            send_buffers: RefCell::new(SendBuffers::new(max_batch)),
        }
    }

    /// 发送单个 UDP 包
    pub async fn send_to(&self, data: &Bytes, dest: SocketAddr) -> std::io::Result<usize> {
        self.socket.send_to(data, dest).await
    }

    /// 批量发送多个 UDP 包
    ///
    /// packetcache 已提供缓冲
    pub async fn send_batch(
        &self,
        packets: &[(SocketAddr, Arc<crate::rtp::RtpPacket>)],
    ) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            // Linux：使用 sendmmsg 批量发送
            self.send_batch_mmsg(&packets).await
        }

        #[cfg(not(target_os = "linux"))]
        {
            // 其他平台：回退到循环发送
            for (dest, data) in packets {
                if mock_socket_io_enabled() {
                    continue;
                }
                let _ = self.socket.send_to(data.raw_packet(), dest).await;
            }
            Ok(())
        }
    }

    /// 使用 sendmmsg + AsyncFd 实现异步批量发送
    #[cfg(target_os = "linux")]
    #[instrument(
        level = "trace",
        name = "udp_sendmmsg",
        skip(self, packets),
        fields(count = packets.len())
    )]
    async fn send_batch_mmsg(
        &self,
        packets: &[(SocketAddr, Arc<crate::rtp::RtpPacket>)],
    ) -> std::io::Result<()> {
        use libc::{AF_INET, AF_INET6, socklen_t};
        use std::os::unix::io::AsRawFd;
        use std::{mem, ptr};

        let batch_size = packets.len().min(self.max_batch);

        let mut buffers = self.send_buffers.borrow_mut();
        buffers.clear();

        for (dest, pkt) in packets.iter().take(batch_size) {
            let bytes = pkt.raw_packet();
            // event!(
            //     Level::TRACE,
            //     kind = "mmsg_prepare",
            //     dest = %dest,
            //     payload = bytes.len()
            // );
            // 构造 iovec
            let iov = libc::iovec {
                iov_base: bytes.as_ptr() as *mut _,
                iov_len: bytes.len(),
            };
            buffers.iovecs.push(iov);

            // 构造 sockaddr
            let (addr_ptr, addr_len): (*mut libc::c_void, socklen_t) = match dest {
                SocketAddr::V4(v4) => {
                    // 构造 ipv4 地址
                    let addr = libc::sockaddr_in {
                        sin_family: AF_INET as u16,
                        sin_port: v4.port().to_be(),
                        sin_addr: libc::in_addr {
                            s_addr: u32::from_ne_bytes(v4.ip().octets()),
                        },
                        sin_zero: [0; 8],
                    };
                    buffers.addrs_v4.push(addr);
                    // 返回地址指针和长度
                    (
                        buffers.addrs_v4.last().unwrap() as *const _ as *mut libc::c_void,
                        mem::size_of::<libc::sockaddr_in>() as socklen_t,
                    )
                }
                SocketAddr::V6(v6) => {
                    let addr = libc::sockaddr_in6 {
                        sin6_family: AF_INET6 as u16,
                        sin6_port: v6.port().to_be(),
                        sin6_flowinfo: v6.flowinfo(),
                        sin6_addr: libc::in6_addr {
                            s6_addr: v6.ip().octets(),
                        },
                        sin6_scope_id: v6.scope_id(),
                    };
                    buffers.addrs_v6.push(addr);
                    (
                        buffers.addrs_v6.last().unwrap() as *const _ as *mut libc::c_void,
                        mem::size_of::<libc::sockaddr_in6>() as socklen_t,
                    )
                }
            };

            // 构造 msghdr
            let hdr = libc::msghdr {
                msg_name: addr_ptr,
                msg_namelen: addr_len,
                msg_iov: buffers.iovecs.last().unwrap() as *const _ as *mut libc::iovec,
                msg_iovlen: 1,
                msg_control: ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            };
            buffers.hdrs.push(hdr);
        }

        // 构造 mmsghdr
        for idx in 0..buffers.hdrs.len() {
            let hdr = buffers.hdrs[idx];
            let msg = libc::mmsghdr {
                msg_hdr: hdr,
                msg_len: 0,
            };
            buffers.msgs.push(msg);
        }

        if packets.is_empty() {
            return Ok(());
        }

        let fd = self.socket.as_raw_fd();
        // event!(
        //     Level::TRACE,
        //     kind = "mmsg_ready",
        //     batch = buffers.msgs.len()
        // );

        loop {
            // 等待 socket 可写
            self.socket.writable().await?;

            let send_result = self.socket.try_io(Interest::WRITABLE, || {
                if mock_socket_io_enabled() {
                    return Ok(batch_size as i32);
                }
                let ret = unsafe {
                    libc::sendmmsg(
                        fd,
                        buffers.msgs.as_mut_ptr(),
                        buffers.msgs.len() as u32,
                        libc::MSG_DONTWAIT,
                    )
                };

                if ret < 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(ret)
                }
            });

            match send_result {
                Ok(_sent) => {
                    // event!(
                    //     Level::TRACE,
                    //     kind = "sendmmsg_success",
                    //     sent = sent,
                    //     requested = batch_size
                    // );
                    return Ok(());
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(err)
                    if matches!(err.raw_os_error(), Some(code) if code == libc::EPERM
                        || code == libc::ENOSYS
                        || code == libc::EOPNOTSUPP) =>
                {
                    // 降级逻辑,如果批量发送失败,就一个一个发
                    for (dest, pkt) in packets.iter().take(batch_size) {
                        let _ = self.socket.send_to(pkt.raw_packet(), *dest).await;
                    }
                    return Ok(());
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }
}

#[inline]
fn mock_socket_io_enabled() -> bool {
    config::get().io.mock_socket_io
}
