use std::{hint::black_box, time::Duration};

use criterion::Criterion;
use itertools::Itertools;
use veris_db::{
    engine::{Catalog, Engine, Transaction, local::Local},
    storage::{bitcask::Bitcask, memory::Memory},
    types::{
        schema::{Column, Table},
        value::{DataType, Row, Rows, Value},
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

    fn create_table(&self) {
        let tx = self.engine.begin().unwrap();
        tx.create_table(self.table.clone()).unwrap();
        tx.commit().unwrap();
    }

    fn drop_table(&self) {
        let tx = self.engine.begin().unwrap();
        tx.drop_table(&self.table.name).unwrap();
        tx.commit().unwrap();
    }

    fn show_tables(&self) {
        let tx = self.engine.begin().unwrap();
        let tables = tx.list_tables().unwrap();
        black_box(tables);
        tx.commit().unwrap();
    }

    fn insert(&self, rows: impl Into<Rows>) {
        let tx = self.engine.begin().unwrap();
        tx.insert(&self.table.name, rows).unwrap();
        tx.commit().unwrap();
    }

    fn scan(&self) {
        let tx = self.engine.begin().unwrap();
        let rows = tx.scan(&self.table.name, None).unwrap();
        black_box(rows.collect::<Vec<_>>());
        tx.commit().unwrap();
    }

    fn delete(&self, rows: impl Into<Row>) {
        let tx = self.engine.begin().unwrap();
        tx.delete(&self.table.name, rows).unwrap();
        tx.commit().unwrap();
    }

    fn get(&self, rows: impl Into<Row>) {
        let tx = self.engine.begin().unwrap();
        let _ = tx.get(&self.table.name, rows).unwrap();
        tx.commit().unwrap();
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
                let mut delta = std::time::Duration::ZERO;
                self.create_table();
                for i in 0..iters {
                    let rows = vec![self.row(i as i64)];

                    let now = std::time::Instant::now();
                    self.insert(black_box(rows));
                    delta += now.elapsed();
                }
                self.drop_table();
                delta
            });
        });
    }

    fn bench_scan(&self, mode: &str, c: &mut Criterion, n: usize) {
        let rows = self.n_rows(n);
        c.bench_function(&format!("{mode}_scan"), |b| {
            b.iter_custom(|iters| {
                self.create_table();
                self.insert(rows.clone());
                let mut delta = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let now = std::time::Instant::now();
                    self.scan();
                    delta += now.elapsed();
                }
                self.drop_table();
                delta
            });
        });
    }

    fn bench_delete(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_delete"), |b| {
            b.iter_custom(|iters| {
                let mut delta = std::time::Duration::ZERO;
                let rows = self.n_rows(iters as usize);
                self.create_table();
                self.insert(rows.clone());
                for i in 0..iters {
                    let rows = vec![Value::Integer(i as i64)];

                    let now = std::time::Instant::now();
                    self.delete(black_box(rows));
                    delta += now.elapsed();
                }
                self.drop_table();
                delta
            });
        });
    }

    fn bench_get(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_get"), |b| {
            b.iter_custom(|iters| {
                let mut delta = std::time::Duration::ZERO;
                let rows = self.n_rows(iters as usize);
                self.create_table();
                self.insert(rows);
                for i in 0..iters {
                    let rows = vec![Value::Integer(i as i64)];

                    let now = std::time::Instant::now();
                    self.get(black_box(rows));
                    delta += now.elapsed();
                }
                self.drop_table();
                delta
            });
        });
    }

    fn bench_drop_table(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_drop_table"), |b| {
            b.iter_custom(|iters| {
                let mut delta = std::time::Duration::ZERO;
                for _ in 0..iters {
                    self.create_table();

                    let now = std::time::Instant::now();
                    self.drop_table();
                    delta += now.elapsed();
                }
                delta
            });
        });
    }

    fn bench_show_tables(&self, mode: &str, c: &mut Criterion) {
        c.bench_function(&format!("{mode}_show_tables"), |b| {
            b.iter_custom(|iters| {
                let mut delta = std::time::Duration::ZERO;
                self.create_table();
                for _ in 0..iters {
                    let now = std::time::Instant::now();
                    self.show_tables();
                    delta += now.elapsed();
                }
                self.drop_table();
                delta
            });
        });
    }
}

fn bench_memory(c: &mut Criterion) {
    let memory = Bench::new(Local::new(Memory::new()));
    let n = 100;
    memory.bench_insert("memory", c);
    memory.bench_scan("memory", c, n);
    memory.bench_delete("memory", c);
    memory.bench_get("memory", c);
    memory.bench_drop_table("memory", c);
    memory.bench_show_tables("memory", c);
}

fn bench_bitcask(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    let bitcask = Bench::new(Local::new(Bitcask::new(&temp).unwrap()));
    let n = 100;
    bitcask.bench_insert("bitcask", c);
    bitcask.bench_scan("bitcask", c, n);
    bitcask.bench_delete("bitcask", c);
    bitcask.bench_get("bitcask", c);
    bitcask.bench_drop_table("bitcask", c);
    bitcask.bench_show_tables("bitcask", c);
    temp.close().unwrap();
}

fn main() {
    let mut criterion = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(6))
        .configure_from_args();
    bench_memory(&mut criterion);
    bench_bitcask(&mut criterion);
    criterion.final_summary();
}
