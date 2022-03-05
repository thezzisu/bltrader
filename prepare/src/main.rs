use bltrader_prepare::config::load_config;
use bltrader_prepare::prepare::prepare;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;
    prepare(config);
    println!("[io] done");
    Ok(())
}
