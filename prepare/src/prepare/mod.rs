use crate::config::IOConfig;
use std::{sync::Arc, thread};

mod stage0;
mod stage1;
mod stage2;
mod util;

pub fn prepare(config: IOConfig) {
    let arc = Arc::new(config);
    let arc1 = arc.clone();
    let th1 = thread::spawn(move || {
        stage1::prepare_stage1(arc1.as_ref());
    });
    let arc2 = arc.clone();
    let th2 = thread::spawn(move || {
        stage2::prepare_stage2(arc2.as_ref());
    });
    th1.join().unwrap();
    th2.join().unwrap();
}
