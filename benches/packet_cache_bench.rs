use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dm_live::rtp::{PacketCache, RtpHeader, RtpPacket};
use dm_live::rtsp::TrackType;
use std::sync::Arc;
use tokio::time::Instant;

fn build_packet(seq: u16, ts: u32, marker: bool) -> Arc<RtpPacket> {
    let header = RtpHeader {
        version: 2,
        padding: false,
        extension: false,
        csrc_count: 0,
        marker,
        payload_type: 96,
        sequence: seq,
        timestamp: ts,
        ssrc: 1,
    };
    let payload = Bytes::from_static(&[0x41, 0x00, 0x01]);
    let mut raw = header.serialize().to_vec();
    raw.extend_from_slice(&payload);
    Arc::new(RtpPacket::new(
        Bytes::from(raw),
        header,
        payload,
        TrackType::Video,
        "video".to_string(),
        Instant::now(),
    ))
}

fn bench_packet_cache(c: &mut Criterion) {
    c.bench_function("single_push", |b| {
        b.iter(|| {
            let mut cache = PacketCache::new();
            let packet = build_packet(1, 1000, true);
            let _ = cache.push(packet);
            let _ = cache.flush();
        });
    });

    let frag_cases = [1usize, 5, 10, 20, 50];
    let mut group = c.benchmark_group("fragmented_push");
    for &count in &frag_cases {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &cnt| {
            b.iter(|| {
                let mut cache = PacketCache::new();
                for i in 0..cnt {
                    let pkt = build_packet(i as u16, 1000, i + 1 == cnt);
                    let _ = cache.push(pkt);
                }
                let _ = cache.flush();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_packet_cache);
criterion_main!(benches);
