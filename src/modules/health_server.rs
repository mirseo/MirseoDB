use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

const MAX_PORT: u16 = 65535;

pub fn start_health_server(start_port: u16) -> std::io::Result<u16> {
    let listener = bind_available_port(start_port)?;
    let port = listener.local_addr()?.port();

    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    handle_client(stream);
                }
                Err(e) => {
                    eprintln!("[MirseoDB][health] Connection error: {}", e);
                }
            }
        }
    });

    Ok(port)
}

fn bind_available_port(start_port: u16) -> std::io::Result<TcpListener> {
    let mut port = start_port;

    loop {
        match TcpListener::bind(("127.0.0.1", port)) {
            Ok(listener) => return Ok(listener),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                if port == MAX_PORT {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        "No available port for health server",
                    ));
                }
                port = port.saturating_add(1);
            }
            Err(e) => return Err(e),
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0u8; 1024];

    if let Ok(bytes_read) = stream.read(&mut buffer) {
        if bytes_read == 0 {
            return;
        }

        let request = String::from_utf8_lossy(&buffer[..bytes_read]);
        let mut lines = request.lines();

        if let Some(request_line) = lines.next() {
            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("");
            let path = parts.next().unwrap_or("");

            if method.eq_ignore_ascii_case("GET")
                && (path == "/health" || path == "/heatlh")
            {
                write_response(
                    &mut stream,
                    "200 OK",
                    "application/json",
                    "{\"status\":\"200 OK\"}",
                );
                return;
            }
        }
    }

    write_response(
        &mut stream,
        "404 Not Found",
        "text/plain",
        "Not Found",
    );
}

fn write_response(stream: &mut TcpStream, status: &str, content_type: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}",
        status = status,
        content_type = content_type,
        length = body.as_bytes().len(),
        body = body,
    );

    if let Err(e) = stream.write_all(response.as_bytes()) {
        eprintln!("[MirseoDB][health] Failed to write response: {}", e);
    }
}
