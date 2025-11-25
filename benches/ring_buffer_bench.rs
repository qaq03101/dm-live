use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dm_live::media::GopRingBuffer;
use dm_live::rtp::FrameData;
use dm_live::rtsp::TrackType;

fn frame(ts: u64, is_key: bool, _track: TrackType) -> FrameData {
    FrameData {
        timestamp_ms: ts,
        is_key,
        packets: Vec::new(),
    }
}

fn bench_ring_buffer(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer_write");
    for &gop_count in &[3usize, 5, 10] {
        group.bench_with_input(
            BenchmarkId::from_parameter(gop_count),
            &gop_count,
            |b, &count| {
                b.iter(|| {
                    let mut buffer = GopRingBuffer::new("bench".to_string(), count, 30, 20, 10);

                    // push multiple GOPs with key + P frames
                    for gop_idx in 0..(count + 2) {
                        buffer.write_frame(frame(gop_idx as u64 * 1000, true, TrackType::Video));
                        for i in 1..=15 {
                            buffer.write_frame(frame(
                                gop_idx as u64 * 1000 + i * 40,
                                false,
                                TrackType::Video,
                            ));
                        }
                    }

                    let storage = buffer.get_storage();
                    let _ = storage.gops.len();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_ring_buffer);
criterion_main!(benches);
