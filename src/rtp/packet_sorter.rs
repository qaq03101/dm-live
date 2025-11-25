use crate::config;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{Level, event, instrument};

/// UDP收包排序器
pub struct PacketSorter<T> {
    /// 是否已开始排序
    started: bool,
    /// 下一个期望的序列号
    next_seq: u16,
    /// 最新接收到的序列号
    latest_seq: u16,
    /// 最大缓存大小
    max_buffer_size: usize,
    /// 最大缓存时间（毫秒）
    max_buffer_ms: u64,
    /// 最大距离
    max_distance: u16,
    /// 上次输出时间
    last_output_time: Instant,
    /// seq正序缓存
    pkt_sort_cache_map: BTreeMap<u16, T>,
    /// seq回退缓存
    pkt_drop_cache_map: BTreeMap<u16, T>,
}

/// 临时统计：抖动缓存强制flush/溢出次数
static PACKET_SORTER_OVERFLOW_COUNT: AtomicU64 = AtomicU64::new(0);

impl<T> PacketSorter<T> {
    const SEQ_MAX: u16 = u16::MAX;

    pub fn new() -> Self {
        let cfg = &config::get().rtp.sorter;
        Self::with_params(cfg.max_buffer_size, cfg.max_buffer_ms, cfg.max_distance)
    }

    pub fn with_params(max_buffer_size: usize, max_buffer_ms: u64, max_distance: u16) -> Self {
        Self {
            started: false,
            next_seq: 0,
            latest_seq: 0,
            max_buffer_size,
            max_buffer_ms,
            max_distance,
            last_output_time: Instant::now(),
            pkt_sort_cache_map: BTreeMap::new(),
            pkt_drop_cache_map: BTreeMap::new(),
        }
    }

    /// 设置参数
    pub fn set_params(&mut self, max_buffer_size: usize, max_buffer_ms: u64, max_distance: u16) {
        self.max_buffer_size = max_buffer_size;
        self.max_buffer_ms = max_buffer_ms;
        self.max_distance = max_distance;
    }

    /// 清空状态
    pub fn clear(&mut self) {
        self.started = false;
        self.last_output_time = Instant::now();
        self.pkt_sort_cache_map.clear();
        self.pkt_drop_cache_map.clear();
    }

    /// 获取排序缓存长度
    pub fn get_jitter_size(&self) -> usize {
        self.pkt_sort_cache_map.len()
    }

    /// 将包进行排序并收集输出
    #[instrument(
        level = "trace",
        name = "packet_sort_collect",
        skip(self, packet, output),
        fields(
            seq = seq,
            cache_size = self.pkt_sort_cache_map.len(),
            next_seq = self.next_seq
        )
    )]
    pub fn sort_packet_collect(&mut self, seq: u16, packet: T, output: &mut Vec<T>) {
        self.latest_seq = seq;

        if !self.started {
            self.started = true;
            self.next_seq = seq;
        }

        if seq == self.next_seq {
            self.output(seq, packet, output);
            self.flush_packet(output);
            self.pkt_drop_cache_map.clear();
            return;
        }

        if seq < self.next_seq && !self.may_looped(self.next_seq, seq) {
            self.pkt_drop_cache_map.insert(seq, packet);
            if self.pkt_drop_cache_map.len() > self.max_distance as usize
                || self.last_output_time.elapsed().as_millis() as u64 > self.max_buffer_ms
            {
                let total = PACKET_SORTER_OVERFLOW_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
                if total == 1 || total % 50 == 0 {
                    event!(
                        Level::WARN,
                        kind = "jitter_overflow",
                        size = self.pkt_drop_cache_map.len(),
                        total_overflow = total,
                        "packet sorter force flush due to overflow"
                    );
                    tracing::warn!(
                        target: "jitter_overflow",
                        size = self.pkt_drop_cache_map.len(),
                        total_overflow = total,
                        "packet sorter force flush due to overflow"
                    );
                }
                self.force_flush(self.next_seq, output);
                self.pkt_sort_cache_map = std::mem::take(&mut self.pkt_drop_cache_map);
                if let Some((&first_seq, _)) = self.pkt_sort_cache_map.iter().next() {
                    self.pop_iterator(first_seq, output);
                }
            }
            return;
        }

        self.pkt_sort_cache_map.insert(seq, packet);

        if self.need_force_flush(seq) {
            self.force_flush(self.next_seq, output);
        }

        if output.len() > 1 {
            event!(Level::TRACE, kind = "burst_output", count = output.len());
        }
    }

    /// 计算seq距离
    fn distance(&self, seq: u16) -> u16 {
        Self::calc_distance(seq, self.next_seq)
    }

    fn calc_distance(seq: u16, next_seq: u16) -> u16 {
        let ret = if seq > next_seq {
            seq - next_seq
        } else {
            next_seq - seq
        };

        if ret > Self::SEQ_MAX >> 1 {
            Self::SEQ_MAX - ret
        } else {
            ret
        }
    }

    /// 检查是否需要强制flush
    fn need_force_flush(&self, seq: u16) -> bool {
        self.pkt_sort_cache_map.len() > self.max_buffer_size
            || self.distance(seq) > self.max_distance
            || self.last_output_time.elapsed().as_millis() as u64 > self.max_buffer_ms
    }

    /// 检查是否可能回环
    fn may_looped(&self, last_seq: u16, now_seq: u16) -> bool {
        last_seq > Self::SEQ_MAX - self.max_distance || now_seq < self.max_distance
    }

    /// 输出包
    fn output(&mut self, seq: u16, packet: T, output: &mut Vec<T>) {
        if seq != self.next_seq {
            tracing::warn!(
                "packet dropped: {} -> {}, latest seq: {}, jitter buffer size: {}, jitter buffer ms: {}",
                self.next_seq,
                seq.wrapping_sub(1),
                self.latest_seq,
                self.pkt_sort_cache_map.len(),
                self.last_output_time.elapsed().as_millis()
            );
        }

        self.next_seq = seq.wrapping_add(1);
        output.push(packet);
        self.last_output_time = Instant::now();
    }

    /// 弹出迭代器
    fn pop_iterator(&mut self, seq: u16, output: &mut Vec<T>) {
        if let Some(packet) = self.pkt_sort_cache_map.remove(&seq) {
            self.output(seq, packet, output);
        }
    }

    /// flush连续包
    fn flush_packet(&mut self, output: &mut Vec<T>) {
        if self.pkt_sort_cache_map.is_empty() {
            return;
        }

        if !self.may_looped(self.next_seq, self.next_seq) {
            let keys_to_remove: Vec<u16> = self
                .pkt_sort_cache_map
                .keys()
                .copied()
                .take_while(|&seq| seq < self.next_seq)
                .collect();
            for key in keys_to_remove {
                self.pkt_sort_cache_map.remove(&key);
            }
        }

        loop {
            if let Some(&seq) = self.pkt_sort_cache_map.keys().next() {
                if seq == self.next_seq {
                    self.pop_iterator(seq, output);
                    continue;
                }
            }
            break;
        }
    }

    /// 强制flush
    fn force_flush(&mut self, next_seq: u16, output: &mut Vec<T>) {
        if self.pkt_sort_cache_map.is_empty() {
            return;
        }

        let mut it_seq = None;
        for &seq in self.pkt_sort_cache_map.keys() {
            if seq >= next_seq {
                it_seq = Some(seq);
                break;
            }
        }

        let it_seq = it_seq.unwrap_or_else(|| *self.pkt_sort_cache_map.keys().next().unwrap());

        self.pop_iterator(it_seq, output);
        self.flush_packet(output);

        let max_dist = self.max_distance;
        let next_seq = self.next_seq;
        self.pkt_sort_cache_map
            .retain(|&seq, _| Self::calc_distance(seq, next_seq) <= max_dist);
    }
}

impl<T> Default for PacketSorter<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn sequential_packets_need_no_sort() {
        let mut sorter = PacketSorter::new();
        let mut received = Vec::new();

        for seq in 100..110 {
            sorter.sort_packet_collect(seq, seq, &mut received);
        }

        assert_eq!(received, (100..110).collect::<Vec<_>>());
        assert_eq!(sorter.get_jitter_size(), 0);
    }

    #[test]
    fn light_disorder_is_recovered() {
        let mut sorter = PacketSorter::new();
        let mut output = Vec::new();

        let sequence = vec![100, 101, 102, 105, 104, 103, 106];
        for &seq in &sequence {
            sorter.sort_packet_collect(seq, seq, &mut output);
        }

        assert_eq!(output, vec![100, 101, 102, 103, 104, 105, 106]);
        assert_eq!(sorter.get_jitter_size(), 0);
    }

    #[test]
    fn heavy_disorder_within_window_flushes_in_order() {
        let mut sorter = PacketSorter::with_params(32, 500, 50);
        let mut output = Vec::new();
        let sequence = vec![100, 106, 104, 105, 103, 102, 101];

        for &seq in &sequence {
            sorter.sort_packet_collect(seq, seq, &mut output);
        }

        assert_eq!(output, vec![100, 101, 102, 103, 104, 105, 106]);
    }

    #[test]
    fn sequence_wraparound_is_handled() {
        let mut sorter = PacketSorter::with_params(16, 500, 5);
        let mut output = Vec::new();

        for &seq in &[65534u16, 65535, 0, 1, 2] {
            sorter.sort_packet_collect(seq, seq as u32, &mut output);
        }

        assert_eq!(output, vec![65534u32, 65535, 0, 1, 2]);
    }

    #[test]
    fn timeout_triggers_flush_of_waiting_packets() {
        let mut sorter = PacketSorter::with_params(10, 20, 50);
        let mut output = Vec::new();

        sorter.sort_packet_collect(100, 100, &mut output);
        sorter.sort_packet_collect(105, 105, &mut output);
        assert_eq!(sorter.get_jitter_size(), 1);

        std::thread::sleep(Duration::from_millis(25));
        sorter.sort_packet_collect(106, 106, &mut output);

        assert!(output.contains(&105));
        assert!(output.contains(&106));
    }

    #[test]
    fn buffer_overflow_forces_flush() {
        let mut sorter = PacketSorter::with_params(3, 1_000, 500);
        let mut output = Vec::new();

        sorter.sort_packet_collect(10, 10, &mut output);
        sorter.sort_packet_collect(20, 20, &mut output);
        sorter.sort_packet_collect(21, 21, &mut output);
        sorter.sort_packet_collect(22, 22, &mut output);
        sorter.sort_packet_collect(23, 23, &mut output);

        assert!(output.contains(&20));
        assert!(sorter.get_jitter_size() <= 3);
    }

    #[test]
    fn packet_loss_still_outputs_increasing_sequence() {
        let mut sorter = PacketSorter::with_params(32, 5, 50);
        let mut output = Vec::new();

        for seq in 100..200 {
            if seq % 5 != 0 {
                sorter.sort_packet_collect(seq, seq, &mut output);
            }
        }

        for i in 0..25 {
            std::thread::sleep(Duration::from_millis(5));
            let seq = 2000 + i;
            sorter.sort_packet_collect(seq as u16, seq, &mut output);
        }

        assert!(output.len() >= 70);
        for window in output.windows(2) {
            assert!(window[1] > window[0]);
        }
    }
}
