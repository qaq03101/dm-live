use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dm_live::rtp::PacketSorter;

fn bench_packet_sorter(c: &mut Criterion) {
    let mut group = c.benchmark_group("packet_sorter");
    for &span in &[10u16, 50, 200] {
        group.bench_with_input(BenchmarkId::from_parameter(span), &span, |b, &dist| {
            b.iter(|| {
                let mut sorter = PacketSorter::with_params(2048, 1000, dist);
                let mut out = Vec::new();
                for seq in 0..1000u16 {
                    let val = (seq.wrapping_add(dist / 2)) % 1000;
                    sorter.sort_packet_collect(val, val, &mut out);
                    out.clear();
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_packet_sorter);
criterion_main!(benches);
