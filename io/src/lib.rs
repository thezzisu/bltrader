pub mod config;
use hdf5;
use std::{
    fs,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    sync::{Arc, RwLock},
    thread::{self, JoinHandle},
};

pub fn read_hdf5<T: hdf5::H5Type>(filename: &str) -> (Vec<u32>, Vec<T>) {
    let file = hdf5::File::open(filename).unwrap();
    let ds = file.dataset("order_id").unwrap();
    let shape = ds.shape().iter().map(|x| *x as u32).collect::<Vec<_>>();
    let mat = ds.read_raw::<T>().unwrap();
    (shape, mat)
}

pub fn process_id(filename: &str) -> Vec<Arc<RwLock<Vec<(i32, u32)>>>> {
    println!("[io] processing id file {}", filename);
    let (shape, mat) = read_hdf5::<i32>(filename);
    let (n, m, k) = (shape[0], shape[1], shape[2]);
    println!("[io] id shape = {} {} {}", n, m, k);
    let offset = (m * k) as usize;
    let mut raw_items: Vec<Vec<(i32, u32)>> = vec![];
    for _ in 0..10 {
        raw_items.push(vec![]);
    }
    let n = mat.len();
    println!("[io] processing {} items", n);
    for i in 0..n {
        raw_items[(i / offset) % 10].push((mat[i], i as u32));
    }
    drop(mat);
    println!("[io] starting threaded sort");
    let mut arcs: Vec<Arc<RwLock<Vec<(i32, u32)>>>> = vec![];
    for raw_item in raw_items {
        arcs.push(Arc::new(RwLock::new(raw_item)));
    }
    let mut threads: Vec<JoinHandle<()>> = vec![];
    for arc in arcs.iter() {
        let item = arc.clone();
        threads.push(thread::spawn(move || {
            let mut content = item.write().unwrap();
            content.sort_unstable_by_key(|k| k.0);
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    println!("[io] threaded sort finished");
    arcs
}

fn write_cache(cache_file: &Path, arcs: &Vec<Arc<RwLock<Vec<(i32, u32)>>>>) {
    println!("[io] writing cache file {}", cache_file.display());
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(cache_file)
        .unwrap();
    let mut writer = BufWriter::with_capacity(64 * 1024, file);
    writer
        .write_all(&(arcs.len() as u32).to_ne_bytes())
        .unwrap();
    for arc in arcs {
        let item = arc.read().unwrap();
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

fn load_cache(cache_file: &Path) -> Vec<Arc<RwLock<Vec<(i32, u32)>>>> {
    println!("[io] loading cache file {}", cache_file.display());
    let file = fs::File::open(cache_file).unwrap();
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut buf = [0; 4]; // i32
    reader.read_exact(&mut buf).unwrap();
    let n = u32::from_ne_bytes(buf);
    let mut arcs: Vec<Arc<RwLock<Vec<(i32, u32)>>>> = vec![];
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
        arcs.push(Arc::new(RwLock::new(raw_items)));
    }
    println!("[io] cache file loaded");
    arcs
}

pub fn process_id_cached(filename: &str, cache: &str) -> Vec<Arc<RwLock<Vec<(i32, u32)>>>> {
    let cache_file = Path::new(cache).join("id.bin");
    if cache_file.exists() {
        load_cache(&cache_file)
    } else {
        let arcs = process_id(filename);
        write_cache(&cache_file, &arcs);
        arcs
    }
}

pub fn load_prices(filename: &str) -> Vec<Vec<f64>> {
    println!("[io] loading prices file {}", filename);
    vec![]
}
