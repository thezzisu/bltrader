use bltrader_io::config::load_config;
use bltrader_io::process_id_cached;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handler(mut socket: TcpStream) {
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;
    #[allow(dead_code)]
    let items = process_id_cached(&config.order_file, &config.cache_dir);
    println!(
        "{:?}",
        items[0].read().unwrap().iter().take(20).collect::<Vec<_>>()
    );

    let addr = "127.0.0.1:19268";
    let listener = TcpListener::bind(addr).await?;
    println!("[io] listening on {}", addr);

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(handler(socket));
    }
}
