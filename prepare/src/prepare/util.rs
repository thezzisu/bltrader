use std::time::SystemTime;

pub fn read_hdf5<T: hdf5::H5Type>(filename: &str, dataset: &str) -> (Vec<u32>, Vec<T>) {
    println!("[io] parsing hdf5 {}", filename);
    let now = SystemTime::now();
    let file = hdf5::File::open(filename).unwrap();
    let ds = file.dataset(dataset).unwrap();
    let shape = ds.shape().iter().map(|x| *x as u32).collect::<Vec<_>>();
    let mat = ds.read_raw::<T>().unwrap();
    println!("[io] parsed in {}s", now.elapsed().unwrap().as_secs_f32());
    (shape, mat)
}
