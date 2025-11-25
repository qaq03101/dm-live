use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dm_live::rtp::{PacketCache, PacketSorter, RtpHeader, RtpPacket};
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

fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("packet_pipeline");
    for &frag in &[5usize, 10, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(frag),
            &frag,
            |b, &frag_count| {
                b.iter(|| {
                    let mut cache = PacketCache::with_max_packets(frag_count + 2);
                    let mut sorter = PacketSorter::with_params(4096, 1000, 500);
                    let mut output = Vec::new();

                    for seq in 0..frag_count {
                        let pkt = build_packet(seq as u16, 1000, seq + 1 == frag_count);
                        if let Some(frame) = cache.push(pkt) {
                            for (idx, _p) in frame.packets.iter().enumerate() {
                                sorter.sort_packet_collect(idx as u16, idx as u16, &mut output);
                            }
                        }
                    }

                    if let Some(frame) = cache.flush() {
                        for (idx, _p) in frame.packets.iter().enumerate() {
                            sorter.sort_packet_collect(idx as u16, idx as u16, &mut output);
                        }
                    }

                    output.clear();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_pipeline);
criterion_main!(benches);
