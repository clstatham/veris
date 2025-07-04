use std::{
    hint::black_box,
    io::Cursor,
    time::{Duration, Instant},
};

use criterion::Criterion;
use itertools::Itertools;
use veris_db::{
    engine::{Catalog, Engine, Transaction, local::Local},
    storage::bitcask::Bitcask,
    types::{
        schema::{Column, Table},
        value::{DataType, Row, Value},
    },
};

struct Bench<E: Engine> {
    engine: E,
    table: Table,
}

impl<E: Engine> Bench<E> {
    fn new(engine: E) -> Self {
        let table = Table::new("test", 0).with_columns([
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String { length: None }),
            Column::new("age", DataType::Integer),
        ]);

        Self { engine, table }
    }

    fn create_table(&self) -> Duration {
        let tx = self.engine.begin().unwrap();
        let now = Instant::now();
        tx.create_table(black_box(self.table.clone())).unwrap();
        let delta = now.elapsed();
        tx.commit().unwrap();
        delta
    }

    fn drop_table(&self) -> Duration {
        let tx = self.engine.begin().unwrap();
        let now = Instant::now();
        tx.drop_table(black_box(&self.table.name)).unwrap();
        let delta = now.elapsed();
        tx.commit().unwrap();
        delta
    }

    fn show_tables(&self) -> Duration {
        let tx = self.engine.begin().unwrap();
        let now = Instant::now();
        black_box(tx.list_tables()).unwrap();
        let delta = now.elapsed();
        tx.commit().unwrap();
        delta
    }

    fn insert(&self, rows: impl AsRef<[Row]>) -> Duration {
        let tx = self.engine.begin().unwrap();
        let now = Instant::now();
        tx.insert(black_box(&self.table.name), black_box(rows))
            .unwrap();
        let delta = now.elapsed();
        tx.commit().unwrap();
        delta
    }

    fn scan(&self) -> Duration {
        let tx = self.engine.begin().unwrap();
        let now = Instant::now();
        let rows = tx
            .scan(black_box(&self.table.name), black_box(None))
            .unwrap();
        black_box(rows.collect::<Vec<_>>());
        let delta = now.elapsed();
        tx.commit().unwrap();
        delta
    }

    fn delete(&self, rows: impl AsRef<[Value]>) -> Duration {
        let tx = self.engine.begin().unwrap();
        let now = Instant::now();
        tx.delete(black_box(&self.table.name), black_box(rows))
            .unwrap();
        let delta = now.elapsed();
        tx.commit().unwrap();
        delta
    }

    fn get(&self, rows: impl AsRef<[Value]>) -> Duration {
        let tx = self.engine.begin().unwrap();
        let now = Instant::now();
        tx.get(black_box(&self.table.name), black_box(rows))
            .unwrap();
        let delta = now.elapsed();
        tx.commit().unwrap();
        delta
    }

    fn row(&self, id: i64) -> Row {
        Row::from(vec![
            Value::Integer(id),
            Value::String(format!("name_{}", id)),
            Value::Integer(id * 2),
        ])
    }

    fn n_rows(&self, n: usize) -> Vec<Row> {
        (0..n).map(|i| self.row(i as i64)).collect_vec()
    }

    fn bench_insert(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_insert"), |b| {
            b.iter_custom(|iters| {
                let mut delta = Duration::ZERO;
                self.create_table();
                for i in 0..iters {
                    let rows = vec![self.row(i as i64)];

                    delta += self.insert(rows);
                }
                self.drop_table();
                delta
            });
        });
    }

    fn bench_scan(&self, mode: &str, c: &mut Criterion, n: usize) {
        let rows = self.n_rows(n);
        self.create_table();
        self.insert(rows.clone());
        c.bench_function(&format!("{mode}_scan_{n}"), |b| {
            b.iter_custom(|iters| {
                let mut delta = Duration::ZERO;
                for _ in 0..iters {
                    delta += self.scan();
                }
                delta
            });
        });
        self.drop_table();
    }

    fn bench_delete(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_delete"), |b| {
            b.iter_custom(|iters| {
                let mut delta = Duration::ZERO;
                let rows = self.n_rows(iters as usize);
                self.create_table();
                self.insert(rows.clone());
                for i in 0..iters {
                    let rows = vec![Value::Integer(i as i64)];

                    delta += self.delete(rows);
                }
                self.drop_table();
                delta
            });
        });
    }

    fn bench_get(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_get"), |b| {
            b.iter_custom(|iters| {
                let mut delta = Duration::ZERO;
                let rows = self.n_rows(iters as usize);
                self.create_table();
                self.insert(rows);
                for i in 0..iters {
                    let rows = vec![Value::Integer(i as i64)];

                    delta += self.get(rows);
                }
                self.drop_table();
                delta
            });
        });
    }

    fn bench_drop_table(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_drop_table"), |b| {
            b.iter_custom(|iters| {
                let mut delta = Duration::ZERO;
                for _ in 0..iters {
                    self.create_table();

                    delta += self.drop_table();
                }
                delta
            });
        });
    }

    fn bench_show_tables(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_show_tables"), |b| {
            b.iter_custom(|iters| {
                let mut delta = Duration::ZERO;
                self.create_table();
                for _ in 0..iters {
                    delta += self.show_tables();
                }
                self.drop_table();
                delta
            });
        });
    }
}

fn bench_engine<E, F>(c: &mut Criterion, engine: &str, factory: F)
where
    E: Engine,
    F: Fn() -> Bench<E>,
{
    factory().bench_insert(engine, c);
    factory().bench_scan(engine, c, 1);
    factory().bench_scan(engine, c, 100);
    factory().bench_scan(engine, c, 10000);
    factory().bench_delete(engine, c);
    factory().bench_get(engine, c);
    factory().bench_drop_table(engine, c);
    factory().bench_show_tables(engine, c);
}

fn main() {
    let mut criterion = Criterion::default().sample_size(10).configure_from_args();

    bench_engine(&mut criterion, "bitcask", || {
        Bench::new(Local::new(Bitcask::new(Cursor::new(vec![])).unwrap()))
    });

    criterion.final_summary();
}
