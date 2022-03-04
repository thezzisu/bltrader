pub mod config;
use config::IOConfig;
use hdf5;
use std::{
    fs,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    sync::Arc,
    thread::{self, JoinHandle},
    vec,
};

pub fn read_hdf5<T: hdf5::H5Type>(filename: &str, dataset: &str) -> (Vec<u32>, Vec<T>) {
    println!("[io] parsing hdf5 {}", filename);
    let file = hdf5::File::open(filename).unwrap();
    let ds = file.dataset(dataset).unwrap();
    let shape = ds.shape().iter().map(|x| *x as u32).collect::<Vec<_>>();
    let mat = ds.read_raw::<T>().unwrap();
    (shape, mat)
}

pub fn process_id(filename: &str) -> Vec<Vec<(i32, u32)>> {
    println!("[io] processing id file {}", filename);
    let (shape, mat) = read_hdf5::<i32>(filename, "order_id");
    let (n, m, k) = (shape[0], shape[1], shape[2]);
    println!("[io] id shape = {} {} {}", n, m, k);
    let offset = (m * k) as usize;
    let mut items: Vec<Vec<(i32, u32)>> = vec![];
    for _ in 0..10 {
        items.push(vec![]);
    }
    let n = mat.len();
    println!("[io] processing {} items", n);
    for i in 0..n {
        items[(i / offset) % 10].push((mat[i], i as u32));
    }
    drop(mat);
    println!("[io] starting threaded sort");
    let mut threads: Vec<JoinHandle<Vec<(i32, u32)>>> = vec![];
    for mut item in items {
        threads.push(thread::spawn(move || {
            item.sort_unstable_by_key(|k| k.0);
            item
        }));
    }
    let mut items: Vec<Vec<(i32, u32)>> = vec![];
    for thread in threads {
        items.push(thread.join().unwrap())
    }
    println!("[io] threaded sort finished");
    items
}

fn write_cache(cache_file: &Path, items: &Vec<Vec<(i32, u32)>>) {
    println!("[io] writing cache file {}", cache_file.display());
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(cache_file)
        .unwrap();
    let mut writer = BufWriter::with_capacity(64 * 1024, file);
    writer
        .write_all(&(items.len() as u32).to_ne_bytes())
        .unwrap();
    for item in items {
        writer
            .write_all(&(item.len() as u32).to_ne_bytes())
            .unwrap();
        for (k, v) in item.iter() {
            writer.write_all(&k.to_ne_bytes()).unwrap();
            writer.write_all(&v.to_ne_bytes()).unwrap();
        }
    }
    println!("[io] cache file written");
}

fn load_cache(cache_file: &Path) -> Vec<Vec<(i32, u32)>> {
    println!("[io] loading cache file {}", cache_file.display());
    let file = fs::File::open(cache_file).unwrap();
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut buf = [0; 4]; // i32
    reader.read_exact(&mut buf).unwrap();
    let n = u32::from_ne_bytes(buf);
    let mut arcs: Vec<Vec<(i32, u32)>> = vec![];
    for _ in 0..n {
        reader.read_exact(&mut buf).unwrap();
        let m = u32::from_ne_bytes(buf);
        let mut raw_items: Vec<(i32, u32)> = vec![];
        for _ in 0..m {
            reader.read_exact(&mut buf).unwrap();
            let k = i32::from_ne_bytes(buf);
            reader.read_exact(&mut buf).unwrap();
            let l = u32::from_ne_bytes(buf);
            raw_items.push((k, l));
        }
        arcs.push(raw_items);
    }
    println!("[io] cache file loaded");
    arcs
}

pub fn process_id_cached(config: &IOConfig) -> Vec<Vec<(i32, u32)>> {
    let cache_file = Path::new(&config.cache_dir).join("id.bin");
    if cache_file.exists() {
        load_cache(&cache_file)
    } else {
        let arcs = process_id(&config.order_file);
        write_cache(&cache_file, &arcs);
        arcs
    }
}

pub fn process_matrix<T: hdf5::H5Type + Copy>(
    filename: &str,
    dataset: &str,
    ids: &Vec<Vec<(i32, u32)>>,
) -> Vec<Vec<T>> {
    let (_, mat) = read_hdf5::<T>(filename, dataset);
    let mut items: Vec<Vec<T>> = vec![];
    for stock in ids {
        let mut item: Vec<T> = vec![];
        for (_, offset) in stock {
            item.push(mat[*offset as usize]);
        }
        items.push(item);
    }
    items
}

pub struct Data {
    //
}

pub fn prepare_data(config: &IOConfig) -> Arc<Data> {
    let ids = process_id_cached(config);
    Arc::new(Data {})
}
