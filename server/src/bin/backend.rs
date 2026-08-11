use std::{backtrace, env, io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}, sync::atomic::{AtomicBool, Ordering}};

use server::ThreadPool;

static HEALTHY: AtomicBool = AtomicBool::new(true);

fn main() {
    
    let port = env::args().nth(1).unwrap_or_else(|| "8081".into());
    let id = env::args().nth(2).unwrap_or_else(|| "A".into());
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();
    let pool = ThreadPool::new(4);


    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let id = id.clone();
        pool.execute(move || handle_connections(stream, &id));
    }
}

fn handle_connections(mut stream: TcpStream, id: &str) {
    if !HEALTHY.load(Ordering::Relaxed) {
        return;
    }
    let mut buf = [0; 1024];
    stream.read(&mut buf).unwrap();

    let body =  format!("Hello from backend {id}\n");

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Lenght: {}\r\n\r\n{}",
        body.len(), body
    );

    stream.write_all(response.as_bytes()).unwrap();
}

