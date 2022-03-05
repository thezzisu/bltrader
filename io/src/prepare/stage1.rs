use super::{
    stage0::{prepare_stage0, StockIds},
    util::read_hdf5,
};
use crate::config::IOConfig;
use std::{
    fs::{self, create_dir_all, remove_dir_all},
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
    thread::{self, JoinHandle},
};

fn reorder_matrix_i32(
    config: &IOConfig,
    filename: &str,
    dataset: &'static str,
    cache_root: &Path,
    ids: Vec<StockIds>,
) -> Vec<StockIds> {
    println!("[io] [stage-1] reorder {}", dataset);
    let (_, mat) = read_hdf5::<i32>(filename, dataset);
    let chunk_size = config.chunk_size;
    let arc = Arc::new(mat);

    let mut threads: Vec<JoinHandle<StockIds>> = vec![];

    println!("[io] [stage-1] starting threaded reorder");
    for id in ids {
        let cache_root = cache_root.to_owned();
        let mat_ref = arc.clone();
        threads.push(thread::spawn(move || {
            let cache_root = cache_root.join(format!("{}", id.0));
            create_dir_all(&cache_root).unwrap();

            let mut reordered: Vec<i32> = vec![];
            for (_, offset) in id.1.iter() {
                reordered.push(mat_ref[*offset as usize]);
            }
            for (chunk_id, chunk) in reordered.chunks(chunk_size).enumerate() {
                let cache_file = cache_root.join(format!("{}-{}.bin", dataset, chunk_id));
                let mut writer = BufWriter::new(
                    fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(cache_file)
                        .unwrap(),
                );
                writer
                    .write_all(&(chunk.len() as u32).to_ne_bytes())
                    .unwrap();
                for item in chunk {
                    writer.write_all(&item.to_ne_bytes()).unwrap();
                }
            }
            id
        }));
    }
    let mut ids: Vec<StockIds> = vec![];
    for thread in threads {
        ids.push(thread.join().unwrap());
    }
    println!("[io] [stage-1] threaded reorder finished");
    ids
}

fn reorder_matrix_f64(
    config: &IOConfig,
    filename: &str,
    dataset: &'static str,
    cache_root: &Path,
    ids: Vec<StockIds>,
) -> Vec<StockIds> {
    println!("[io] [stage-1] reorder {}", dataset);
    let (_, mat) = read_hdf5::<f64>(filename, dataset);
    let chunk_size = config.chunk_size;
    let arc = Arc::new(mat);

    let mut threads: Vec<JoinHandle<StockIds>> = vec![];

    println!("[io] [stage-1] starting threaded reorder");
    for id in ids {
        let cache_root = cache_root.to_owned();
        let mat_ref = arc.clone();
        threads.push(thread::spawn(move || {
            let cache_root = cache_root.join(format!("{}", id.0));
            create_dir_all(&cache_root).unwrap();

            let mut reordered: Vec<f64> = vec![];
            for (_, offset) in id.1.iter() {
                reordered.push(mat_ref[*offset as usize]);
            }
            for (chunk_id, chunk) in reordered.chunks(chunk_size).enumerate() {
                let cache_file = cache_root.join(format!("{}-{}.bin", dataset, chunk_id));
                let mut writer = BufWriter::new(
                    fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(cache_file)
                        .unwrap(),
                );
                writer
                    .write_all(&(chunk.len() as u32).to_ne_bytes())
                    .unwrap();
                for item in chunk {
                    writer.write_all(&item.to_ne_bytes()).unwrap();
                }
            }
            id
        }));
    }
    let mut ids: Vec<StockIds> = vec![];
    for thread in threads {
        ids.push(thread.join().unwrap());
    }
    println!("[io] [stage-1] threaded reorder finished");
    ids
}

fn save_ids(config: &IOConfig, cache_root: &Path, ids: Vec<StockIds>) {
    println!("[io] [stage-1] save ids");
    let chunk_size = config.chunk_size;

    let mut threads: Vec<JoinHandle<()>> = vec![];

    println!("[io] [stage-1] starting threaded reorder");
    for id in ids {
        let cache_root = cache_root.to_owned();
        threads.push(thread::spawn(move || {
            let cache_root = cache_root.join(format!("{}", id.0));
            create_dir_all(&cache_root).unwrap();

            for (chunk_id, chunk) in id.1.chunks(chunk_size).enumerate() {
                let cache_file = cache_root.join(format!("{}-{}.bin", "id", chunk_id));
                let mut writer = BufWriter::new(
                    fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(cache_file)
                        .unwrap(),
                );
                writer
                    .write_all(&(chunk.len() as u32).to_ne_bytes())
                    .unwrap();
                for item in chunk {
                    writer.write_all(&item.0.to_ne_bytes()).unwrap();
                }
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    println!("[io] [stage-1] threaded reorder finished");
}

pub fn prepare_stage1(config: &IOConfig) {
    let cache_root = Path::new(&config.cache_dir).join("stage-1");
    create_dir_all(&cache_root).unwrap();
    let cache_lock = cache_root.join(".lock");
    if !cache_lock.exists() {
        println!("[io] [stage-1] Preparing stage 1");
        let ids = prepare_stage0(config);
        let ids = reorder_matrix_f64(config, &config.price_file, "price", &cache_root, ids);
        let ids = reorder_matrix_i32(config, &config.volume_file, "volume", &cache_root, ids);
        let ids = reorder_matrix_i32(config, &config.type_file, "type", &cache_root, ids);
        let ids = reorder_matrix_i32(
            config,
            &config.direction_file,
            "direction",
            &cache_root,
            ids,
        );
        save_ids(config, &cache_root, ids);
        fs::File::create(&cache_lock).unwrap();
        remove_dir_all(Path::new(&config.cache_dir).join("stage-0")).unwrap();
    }
}
