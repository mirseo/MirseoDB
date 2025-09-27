use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RouteConfig {
    pub routes: HashMap<String, String>,
}

impl RouteConfig {
    pub fn load() -> Result<Self, String> {
        let config_path = "route.cfg";

        if !std::path::Path::new(config_path).exists() {
            Self::create_default_config(config_path)?;
        }

        let content = fs::read_to_string(config_path)
            .map_err(|e| format!("Failed to read route.cfg: {}", e))?;

        let mut routes = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once('=') {
                routes.insert(key.trim().to_string(), value.trim().to_string());
            }
        }

        Ok(Self { routes })
    }

    fn create_default_config(path: &str) -> Result<(), String> {
        let default_content = r#"# Route Configuration for MirseoDB
# Format: route_name=server_url
# Example: backup_server=http://192.168.1.100:3306
# Example: analytics_server=http://analytics.company.com:3306

# Default fallback server (uncomment and configure as needed)
# fallback=http://localhost:3307
"#;

        fs::write(path, default_content)
            .map_err(|e| format!("Failed to create default route.cfg: {}", e))
    }

    pub fn get_fallback_server(&self) -> Option<&String> {
        self.routes.get("fallback")
    }
}

pub struct ForwardRequest {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

pub struct ForwardResponse {
    pub status_code: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
    pub is_forward_mode: bool,
}

impl ForwardResponse {
    pub fn new(status_code: u16, body: String) -> Self {
        Self {
            status_code,
            headers: HashMap::new(),
            body,
            is_forward_mode: false,
        }
    }

    pub fn with_forward_mode(mut self, is_forward: bool) -> Self {
        self.is_forward_mode = is_forward;
        self
    }

    pub fn add_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }
}

pub fn should_forward_request(headers: &HashMap<String, String>) -> bool {
    // Check for mode:fd parameter indicating this is a forward request
    if let Some(mode) = headers.get("x-mirseodb-mode") {
        return mode == "fd";
    }

    // Check for mode parameter in other headers or query params
    for (key, value) in headers {
        if key.to_lowercase().contains("mode") && value.contains("fd") {
            return true;
        }
    }

    false
}

pub fn forward_request(
    target_url: &str,
    request: &ForwardRequest,
) -> Result<ForwardResponse, String> {
    // Parse the target URL
    let url = if target_url.starts_with("http://") || target_url.starts_with("https://") {
        target_url.to_string()
    } else {
        format!("http://{}", target_url)
    };

    // Extract host and port from URL
    let (host, port) = parse_url(&url)?;

    // Create TCP connection
    let mut stream = TcpStream::connect(format!("{}:{}", host, port))
        .map_err(|e| format!("Failed to connect to {}: {}", target_url, e))?;

    // Set timeout
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| format!("Failed to set read timeout: {}", e))?;

    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| format!("Failed to set write timeout: {}", e))?;

    // Build HTTP request
    let mut http_request = format!("{} {} HTTP/1.1\r\n", request.method, request.path);
    http_request.push_str(&format!("Host: {}\r\n", host));
    http_request.push_str("Connection: close\r\n");
    http_request.push_str(&format!("Content-Length: {}\r\n", request.body.len()));

    // Add mode:fd header to indicate this is a forwarded request
    http_request.push_str("X-MirseoDB-Mode: fd\r\n");
    http_request.push_str("X-MirseoDB-Forward: true\r\n");

    // Add other headers
    for (key, value) in &request.headers {
        if !key.to_lowercase().starts_with("host")
            && !key.to_lowercase().starts_with("content-length")
            && !key.to_lowercase().starts_with("connection")
        {
            http_request.push_str(&format!("{}: {}\r\n", key, value));
        }
    }

    http_request.push_str("\r\n");

    // Send request
    stream
        .write_all(http_request.as_bytes())
        .map_err(|e| format!("Failed to send headers: {}", e))?;

    if !request.body.is_empty() {
        stream
            .write_all(&request.body)
            .map_err(|e| format!("Failed to send body: {}", e))?;
    }

    // Read response
    let mut response_data = Vec::new();
    let mut buffer = [0u8; 1024];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break, // Connection closed
            Ok(n) => response_data.extend_from_slice(&buffer[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(e) => return Err(format!("Failed to read response: {}", e)),
        }

        // Prevent infinite reading
        if response_data.len() > 1024 * 1024 {
            break;
        }
    }

    // Parse HTTP response
    parse_http_response(&response_data)
}

fn parse_url(url: &str) -> Result<(String, u16), String> {
    let url = url.strip_prefix("http://").unwrap_or(url);
    let url = url.strip_prefix("https://").unwrap_or(url);

    if let Some((host_port, _)) = url.split_once('/') {
        if let Some((host, port_str)) = host_port.split_once(':') {
            let port: u16 = port_str
                .parse()
                .map_err(|_| format!("Invalid port number: {}", port_str))?;
            Ok((host.to_string(), port))
        } else {
            Ok((host_port.to_string(), 80))
        }
    } else if let Some((host, port_str)) = url.split_once(':') {
        let port: u16 = port_str
            .parse()
            .map_err(|_| format!("Invalid port number: {}", port_str))?;
        Ok((host.to_string(), port))
    } else {
        Ok((url.to_string(), 80))
    }
}

fn parse_http_response(data: &[u8]) -> Result<ForwardResponse, String> {
    if data.is_empty() {
        return Err("Empty response".to_string());
    }

    let response_str = String::from_utf8_lossy(data);
    let lines: Vec<&str> = response_str.lines().collect();

    if lines.is_empty() {
        return Err("Invalid HTTP response".to_string());
    }

    // Parse status line
    let status_line = lines[0];
    let status_code = parse_status_code(status_line)?;

    // Parse headers
    let mut headers = HashMap::new();
    let mut body_start = 1;

    for (i, line) in lines.iter().enumerate().skip(1) {
        if line.trim().is_empty() {
            body_start = i + 1;
            break;
        }

        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_lowercase(), value.trim().to_string());
        }
    }

    // Extract body
    let body = if body_start < lines.len() {
        lines[body_start..].join("\n")
    } else {
        String::new()
    };

    // Check if response indicates forward mode
    let is_forward_mode = headers
        .get("x-mirseodb-mode")
        .map(|v| v == "fd")
        .unwrap_or(false)
        || body.contains("\"mode\":\"fd\"");

    Ok(ForwardResponse {
        status_code,
        headers,
        body,
        is_forward_mode,
    })
}

fn parse_status_code(status_line: &str) -> Result<u16, String> {
    let parts: Vec<&str> = status_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err("Invalid status line".to_string());
    }

    parts[1]
        .parse::<u16>()
        .map_err(|_| format!("Invalid status code: {}", parts[1]))
}

// Add Read trait import
use std::io::Read;
