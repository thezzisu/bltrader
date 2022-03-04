use hdf5::{File, H5Type};
use std::{
    sync::{Arc, RwLock},
    thread::{self, JoinHandle},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn read_hdf5<T: H5Type>(filename: &str) -> (Vec<usize>, Vec<T>) {
    let file = File::open(filename).unwrap();
    let ds = file.dataset("order_id").unwrap();
    let shape = ds.shape();
    let mat = ds.read_raw::<T>().unwrap();
    (shape, mat)
}

fn process_id(filename: &str) -> Vec<Arc<RwLock<Vec<(i32, usize)>>>> {
    println!("[io] processing id file {}", filename);
    let (shape, mat) = read_hdf5::<i32>(filename);
    let (n, m, k) = (shape[0], shape[1], shape[2]);
    println!("[io] id shape = {} {} {}", n, m, k);
    let offset = m * k;
    let mut raw_items: Vec<Vec<(i32, usize)>> = vec![];
    for _ in 0..10 {
        raw_items.push(vec![]);
    }
    let n = mat.len();
    println!("[io] processing {} items", n);
    for i in 0..n {
        raw_items[(i / offset) % 10].push((mat[i], i));
    }
    println!("[io] starting threaded sort");
    let mut arcs: Vec<Arc<RwLock<Vec<(i32, usize)>>>> = vec![];
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let items = process_id("D:\\Downloads\\order_id1.h5");

    let listener = TcpListener::bind("127.0.0.1:19268").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            // In a loop, read data from the socket and write the data back.
            loop {
                let n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                // Write the data back
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }
        });
    }
}
