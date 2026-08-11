use std::{io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}, sync::Arc};

use arc_swap::ArcSwap;

use crate::algorithms::{LeastConnections, LeastResponseTime, LoadBalancer, RoundRobin, WeightedRoundRobin, default_backends};




pub fn run_admin_listener(lb_slot: Arc<ArcSwap<Box<dyn LoadBalancer>>>) {
    let listener = TcpListener::bind("127.0.0.1:7880").unwrap();
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let lb_slot = Arc::clone(&lb_slot);
        std::thread::spawn(move || handle_admin(stream, lb_slot));
    }
}

fn handle_admin(mut stream: TcpStream, lb_slot: Arc<ArcSwap<Box<dyn LoadBalancer>>>) {
    let mut reader = BufReader::new(&mut stream);
    let mut headers = String::new();
    
    // 1. Read headers line by line until we hit the empty line (\r\n) separating them from the body
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break, // Connection closed
            Ok(_) => {
                if line == "\r\n" || line == "\n" {
                    break; // Headers ended
                }
                headers.push_str(&line);
            }
            Err(_) => break,
        }
    }

    // 2. Parse the Content-Length header to see how many body bytes to wait for
    let content_length = headers
        .lines()
        .find(|line| line.to_lowercase().starts_with("content-length:"))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|val| val.trim().parse::<usize>().ok())
        .unwrap_or(0);

    // 3. Read exactly that many bytes into a dedicated body buffer
    let mut body_buf = vec![0; content_length];
    if content_length > 0 {
        let _ = reader.read_exact(&mut body_buf);
    }
    
    let body_str = String::from_utf8_lossy(&body_buf);
    let body = body_str.trim();

    // Debug print will now show your string payload
    println!("Received body: {:?} (len: {})", body, body.len());

    let new_lb: Option<Box<dyn LoadBalancer>> = match body {
        "round_robin" => Some(Box::new(RoundRobin::new(default_backends()))),
        "weighted_round_robin" => Some(Box::new(WeightedRoundRobin::new(default_backends()))),
        "least_connections" => Some(Box::new(LeastConnections::new(default_backends()))),
        "least_response_time" => Some(Box::new(LeastResponseTime::new(default_backends()))),
        _ => None,
    };

    let (status, msg) = match new_lb {
        Some(lb) => {
            lb_slot.store(Arc::new(lb));
            ("200 OK", "switched")
        }
        None => ("400 Bad Request", "unknown algorithm"),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{msg}",
        msg.len()
    );
    let _ = stream.write_all(response.as_bytes());
}