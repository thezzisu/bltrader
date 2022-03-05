use super::util::read_hdf5;
use crate::config::IOConfig;
use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    vec,
};

pub fn prepare_stage2(config: &IOConfig) {
    let cache_root = Path::new(&config.cache_dir).join("stage-2");
    create_dir_all(&cache_root).unwrap();
    let cache_lock = cache_root.join(".lock");
    if !cache_lock.exists() {
        println!("[io] [stage-2] Preparing stage 2");
        let (shape, mat) = read_hdf5::<i32>(&config.hook_file, "hook");
        println!("[io] [stage-2] hook file shape: {:?}", shape);

        let len = shape[1];
        let offset = (len * 4) as usize;
        for i in 0..10 {
            let mut hooks: Vec<(i32, i32, i32, i32)> = vec![];
            for j in 0..(len as usize) {
                hooks.push((
                    mat[i * offset + j * 4],
                    mat[i * offset + j * 4 + 1],
                    mat[i * offset + j * 4 + 2],
                    mat[i * offset + j * 4 + 3],
                ));
            }
            hooks.sort_unstable_by_key(|x| x.0);
            let mut writer = BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(cache_root.join(format!("hook_{}.bin", i)))
                    .unwrap(),
            );
            writer.write_all(&len.to_le_bytes()).unwrap();
            for hook in hooks {
                writer.write_all(&hook.0.to_le_bytes()).unwrap();
                writer.write_all(&hook.1.to_le_bytes()).unwrap();
                writer.write_all(&hook.2.to_le_bytes()).unwrap();
                writer.write_all(&hook.3.to_le_bytes()).unwrap();
            }
        }
        File::create(&cache_lock).unwrap();
    }
}
