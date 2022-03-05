use std::{
    fs::{self, create_dir_all},
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    thread::{self, JoinHandle},
};

use crate::{config::IOConfig, prepare::util::read_hdf5};

pub type StockIds = (i32, Vec<(i32, u32)>);

fn process_id(filename: &str) -> Vec<StockIds> {
    println!("[io] [stage-0] processing id file {}", filename);
    let (shape, mat) = read_hdf5::<i32>(filename, "order_id");
    let (n, m, k) = (shape[0], shape[1], shape[2]);
    println!("[io] [stage-0] id shape = {} {} {}", n, m, k);
    let offset = (m * k) as usize;
    let mut items: Vec<StockIds> = vec![];
    for i in 0..10 {
        items.push((i, vec![]));
    }
    let n = mat.len();
    println!("[io] [stage-0] processing {} items", n);
    for i in 0..n {
        items[(i / offset) % 10].1.push((mat[i], i as u32));
    }
    drop(mat);
    println!("[io] [stage-0] starting threaded sort");
    let mut threads: Vec<JoinHandle<StockIds>> = vec![];
    for mut item in items {
        threads.push(thread::spawn(move || {
            item.1.sort_unstable_by_key(|k| k.0);
            item
        }));
    }
    let mut items: Vec<StockIds> = vec![];
    for thread in threads {
        items.push(thread.join().unwrap())
    }
    println!("[io] [stage-0] threaded sort finished");
    items
}

fn write_cache(cache_file: &Path, items: &Vec<StockIds>) {
    println!("[io] [stage-0] writing cache file {}", cache_file.display());
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(cache_file)
        .unwrap();
    let mut writer = BufWriter::with_capacity(64 * 1024, file);
    writer
        .write_all(&(items.len() as u32).to_le_bytes())
        .unwrap();
    for item in items {
        writer.write(&(item.0.to_le_bytes())).unwrap();
        writer
            .write_all(&(item.1.len() as u32).to_le_bytes())
            .unwrap();
        for (k, v) in item.1.iter() {
            writer.write_all(&k.to_le_bytes()).unwrap();
            writer.write_all(&v.to_le_bytes()).unwrap();
        }
    }
    println!("[io] [stage-0] cache file written");
}

fn load_cache(cache_file: &Path) -> Vec<StockIds> {
    println!("[io] [stage-0] loading cache file {}", cache_file.display());
    let file = fs::File::open(cache_file).unwrap();
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut buf = [0; 4]; // i32
    reader.read_exact(&mut buf).unwrap();
    let n = u32::from_le_bytes(buf);
    let mut arcs: Vec<StockIds> = vec![];
    for _ in 0..n {
        reader.read_exact(&mut buf).unwrap();
        let n = i32::from_le_bytes(buf);
        reader.read_exact(&mut buf).unwrap();
        let m = u32::from_le_bytes(buf);
        let mut raw_items: Vec<(i32, u32)> = vec![];
        for _ in 0..m {
            reader.read_exact(&mut buf).unwrap();
            let k = i32::from_le_bytes(buf);
            reader.read_exact(&mut buf).unwrap();
            let l = u32::from_le_bytes(buf);
            raw_items.push((k, l));
        }
        arcs.push((n, raw_items));
    }
    println!("[io] [stage-0] cache file loaded");
    arcs
}

fn process_id_cached(config: &IOConfig, cache_root: &Path) -> Vec<StockIds> {
    let cache_file = cache_root.join("id.bin");
    if cache_file.exists() {
        load_cache(&cache_file)
    } else {
        let items = process_id(&config.order_file);
        write_cache(&cache_file, &items);
        items
    }
}

pub fn prepare_stage0(config: &IOConfig) -> Vec<StockIds> {
    println!("[io] [stage-0] Preparing stage 0");
    let cache_root = Path::new(&config.cache_dir).join("stage-0");
    create_dir_all(&cache_root).unwrap();
    process_id_cached(config, &cache_root)
}
