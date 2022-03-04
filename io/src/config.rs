use std::fs::read_to_string;

use dirs;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct IOConfig {
    pub order_file: String,
    pub price_file: String,
    pub volume_file: String,
    pub type_file: String,
    pub direction_file: String,
    pub cache_dir: String,
}

pub fn load_config() -> Result<IOConfig, Box<dyn std::error::Error>> {
    let root = dirs::config_dir().unwrap();
    let path = root.join("bltrader").join("io.json");
    let content = read_to_string(path)?;
    let config: IOConfig = serde_json::from_str(&content)?;
    Ok(config)
}
