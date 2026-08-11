use std::{
    fs, io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}, sync::Arc, thread, time::Instant,
   
};

use arc_swap::ArcSwap;
use server::ThreadPool;

use crate::{algorithm_listener::run_admin_listener, algorithms::{LoadBalancer, RoundRobin, default_backends}};

mod algorithms;
mod algorithm_listener;

fn main() {
    let backends = default_backends();

    let chosen_algorithm = RoundRobin::new(backends);

    let lb_slot = Arc::new(ArcSwap::from_pointee(
        Box::new(RoundRobin::new(default_backends())) as Box<dyn LoadBalancer>
    ));

    let admin_slot = Arc::clone(&lb_slot);
    thread::spawn(move || run_admin_listener(admin_slot));

    let lb: Arc<dyn LoadBalancer> = Arc::new(chosen_algorithm);

    let pool = ThreadPool::new(8);

    let html_handle = thread::spawn(move || {
        let control_listener = TcpListener::bind("127.0.0.1:7878").unwrap();
        for stream in control_listener.incoming() {
            let stream = stream.unwrap();

            handle_connection(stream);
        }
    });

    let proxy_handle = thread::spawn(move || {
        let data_listener = TcpListener::bind("127.0.0.1:7879").unwrap();

        for stream in data_listener.incoming() {
            let stream = stream.unwrap();

            let lb_slot = Arc::clone(&lb_slot);

            pool.execute(move || proxy_connections(stream, &lb_slot));
        }
    });

    html_handle.join().unwrap();
    proxy_handle.join().unwrap();

    println!("Shutting down!");
}

fn proxy_connections(mut client: TcpStream, lb_slot: &Arc<ArcSwap<Box<dyn LoadBalancer>>>) {
    let mut buf = [0; 1024];
    let lb = lb_slot.load();

    let n = match client.read(&mut buf) {
        Ok(0) => return,
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
            println!("Client disconnected abruptly.");
            return;
        }
        Err(e) => {
            
            eprintln!("Unexpected network error:{}",e);
            return;
        }
    };

    let backend = lb.next();

    let start = Instant::now();
    let latency = start.elapsed();


    
        let mut upstream = match TcpStream::connect(&backend.backend.addr) {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!(
                    "Failed to connect to backend {}: {}",
                    backend.backend.addr,
                    e
                );

                lb.release(&backend, latency, false);

                return;
            }
        };
        upstream.write_all(&buf[..n]).unwrap();
    
        let mut resp = Vec::new();
        upstream.read_to_end(&mut resp).unwrap();
        client.write_all(&resp).unwrap();
    


    //let latency = start.elapsed();

  

   
}

fn handle_connection(mut stream: TcpStream) {
    // --snip--
    let buf_reader = BufReader::new(&stream);
    let request_line = buf_reader.lines().next().unwrap().unwrap();

     let (status_line, filename, content_type) = match request_line.as_str() {
        "GET / HTTP/1.1" => ("HTTP/1.1 200 OK", "hello.html", "text/html"),
        "GET /assets/weightedroundrobin.png HTTP/1.1" => ("HTTP/1.1 200 OK", "assets/weightedroundrobin.png", "image/png"),
        "GET /assets/roundrobin.png HTTP/1.1" => ("HTTP/1.1 200 OK", "assets/roundrobin.png", "image/png"),
        "GET /assets/leastconnections.png HTTP/1.1" => ("HTTP/1.1 200 OK", "assets/leastconnections.png", "image/png"),
        "GET /assets/leastresponsetime.png HTTP/1.1" => ("HTTP/1.1 200 OK", "assets/leastresponsetime.png", "image/png"),
        _ => ("HTTP/1.1 404 NOT FOUND", "404.html", "text/html"),
    };

   

    let contents = fs::read(filename).unwrap();
    let length = contents.len();

    let response = format!("{status_line}\r\nContent-Length: {length}\r\n\r\n");

    stream.write_all(response.as_bytes()).unwrap();
    stream.write_all(&contents).unwrap();
}

/*
fn handle_connections(mut stream: TcpStream) {
    let buf_reader = BufReader::new(&stream);
    let request_line = buf_reader.lines().next().unwrap().unwrap();


    let (status_line, filename) = match &request_line[..] {
        "GET / HTTP/1.1" => ("HTTP/1.1 200 OK", "hello.html"),
        "GET /sleep HTTP/1.1" => {
            thread::sleep(Duration::from_secs(5));
            ("HTTP/1.1 200 OK", "hello.html")
        }
        _ => ("HTTP/1.1 404 NOT FOUND", "404.html")
    };
    /*
    if request_line ==  {
       ()
   } else {

   };
     */


    let contents = fs::read_to_string(filename).unwrap();
        let lenght = contents.len();
        let response = format!("{status_line}\r\nContent-Lenght: {lenght}\r\n\r\n{contents}");

        stream.write_all(response.as_bytes()).unwrap();


        /*
        if request_line == "GET / HTTP/1.1" {
            let status_line = "HTTP/1.1 200 OK";


            let response = format!("{status_line}\r\nContent-Lenght: {lenght}\r\n\r\n{contents}");
            stream.write_all(response.as_bytes()).unwrap();
        } else {
            let status_line = "HTTP/1.1 404 NOT FOUND";
            let contents = fs::read_to_string("404.html").unwrap();
            let lenght = contents.len();

            let response = format!("{status_line}\r\nContent-Lenght: {lenght}\r\n\r\n{contents}");
            stream.write_all(response.as_bytes()).unwrap();


        }
         */
    /*
    let http_request: Vec<_> = buf_reader
        .lines()
        .map(|result| result.unwrap())
        .take_while(|line| !line.is_empty())
        .collect();
     */



    //let response = format!("{status_line}\r\nContent-Lenght: {lenght}\r\n\r\n{contents}");


}
 */
