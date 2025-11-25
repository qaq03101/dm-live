use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dm_live::event::RuntimePool;
use std::sync::mpsc::channel;

fn bench_runtime_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_pool_tasks");
    for &workers in &[1usize, 2, 4, 8] {
        group.bench_with_input(BenchmarkId::from_parameter(workers), &workers, |b, &w| {
            b.iter(|| {
                let pool = RuntimePool::new(Some(w));
                let handles = pool.handles();
                let total_tasks = 8000usize;
                let base = total_tasks / handles.len();
                let extra = total_tasks % handles.len();
                let mut expected = 0usize;
                let (tx, rx) = channel();

                for (idx, handle) in handles.iter().enumerate() {
                    let task_count = base + if idx < extra { 1 } else { 0 };
                    expected += task_count;
                    for _ in 0..task_count {
                        let tx = tx.clone();
                        handle.spawn(async move {
                            let _ = tx.send(());
                        });
                    }
                }
                drop(tx);

                for _ in 0..expected {
                    rx.recv().expect("task result");
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_runtime_pool);
criterion_main!(benches);
