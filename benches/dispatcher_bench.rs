use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dm_live::media::reader::FrameRef;
use dm_live::media::{Dispatcher, Reader, StorageSnapshot};
use std::sync::Arc;

fn make_reader(id: &str) -> Reader {
    Reader::new(id.to_string(), Default::default(), |_frames: FrameRef| {})
}

fn bench_dispatcher(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatcher");
    for &reader_count in &[1usize, 10, 50, 100, 1000, 10000, 30000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(reader_count),
            &reader_count,
            |b, &count| {
                let storage = Arc::new(StorageSnapshot::empty());
                let mut dispatcher = Dispatcher::new(0, storage);
                for i in 0..count {
                    let reader = make_reader(&format!("r{i}"));
                    dispatcher.register_reader(reader).unwrap();
                }
                let snapshot = Arc::new(StorageSnapshot::empty());
                b.iter(|| {
                    dispatcher.update_storage(snapshot.clone());
                    dispatcher.dispatch(snapshot.clone());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_dispatcher);
criterion_main!(benches);
