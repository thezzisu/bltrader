use crate::config::IOConfig;

mod stage0;
mod stage1;
mod util;

pub fn prepare(config: &IOConfig) {
    stage1::prepare_stage1(config);
}
