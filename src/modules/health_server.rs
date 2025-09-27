use super::anysql_parser::AnySQL;
use super::database::Database;
use super::types::{DatabaseError, Row, SqlValue};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_PORT: u16 = 65535;
const MAX_REQUEST_SIZE: usize = 64 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(2);

struct HealthServerState {
    start_time: Instant,
    version: &'static str,
    last_checkpoint_ms: u128,
}

impl HealthServerState {
    fn new() -> Self {
        let start_time = Instant::now();
        let last_checkpoint_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        Self {
            start_time,
            version: env!("CARGO_PKG_VERSION"),
            last_checkpoint_ms,
        }
    }

    fn health_payload(&self) -> String {
        let uptime = self.start_time.elapsed().as_millis();
        let mut body = String::from("{");
        body.push_str("\"status\":\"200 OK\"");
        body.push_str(",\"status_code\":200");
        body.push_str(",\"uptime_ms\":");
        body.push_str(&uptime.to_string());
        body.push_str(",\"version\":\"");
        body.push_str(&escape_json_string(self.version));
        body.push_str("\"");
        body.push_str(",\"transactions_active\":0");
        body.push_str(",\"wal_lsn\":\"0/0\"");
        body.push_str(",\"last_checkpoint\":");
        body.push_str(&self.last_checkpoint_ms.to_string());
        body.push('}');
        body
    }
}

struct ApiServerState {
    health: HealthServerState,
    database: Arc<Mutex<Database>>,
    parser: Arc<AnySQL>,
    auth_token: Option<String>,
}

impl ApiServerState {
    fn new(
        database: Arc<Mutex<Database>>,
        parser: Arc<AnySQL>,
        auth_token: Option<String>,
    ) -> Self {
        Self {
            health: HealthServerState::new(),
            database,
            parser,
            auth_token,
        }
    }
}

struct QueryRequest {
    sql: String,
    auth_token: Option<String>,
}

pub fn start_health_server(
    start_port: u16,
    database: Arc<Mutex<Database>>,
    parser: Arc<AnySQL>,
    auth_token: Option<String>,
) -> std::io::Result<u16> {
    let listener = bind_available_port(start_port)?;
    let port = listener.local_addr()?.port();
    let state = Arc::new(ApiServerState::new(database, parser, auth_token));

    thread::spawn({
        let state = Arc::clone(&state);
        move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => handle_client(stream, Arc::clone(&state)),
                    Err(e) => eprintln!("[MirseoDB][api] Connection error: {}", e),
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
                        "No available port for API server",
                    ));
                }
                port = port.saturating_add(1);
            }
            Err(e) => return Err(e),
        }
    }
}

fn handle_client(mut stream: TcpStream, state: Arc<ApiServerState>) {
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));

    let request_bytes = match read_full_request(&mut stream) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("[MirseoDB][api] Failed to read request: {}", e);
            let response = HttpResponse::text("400 Bad Request", "Malformed request");
            let _ = write_http_response(&mut stream, &response);
            return;
        }
    };

    if request_bytes.is_empty() {
        return;
    }

    let (header_text, body_bytes) = match split_request(&request_bytes) {
        Some(parts) => parts,
        None => {
            let response = HttpResponse::text("400 Bad Request", "Invalid HTTP request");
            let _ = write_http_response(&mut stream, &response);
            return;
        }
    };

    let mut lines = header_text.lines();
    let request_line = lines.next().unwrap_or("");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or("");
    let path = request_parts.next().unwrap_or("");
    let headers = parse_headers(lines);

    let response = match (method, path) {
        ("GET", "/health") | ("GET", "/heatlh") => {
            HttpResponse::json("200 OK", state.health.health_payload())
        }
        ("POST", "/query") => handle_query_request(&state, &headers, body_bytes),
        _ => HttpResponse::text("404 Not Found", "Not Found"),
    };

    let _ = write_http_response(&mut stream, &response);
}

fn handle_query_request(
    state: &Arc<ApiServerState>,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> HttpResponse {
    let start_time = Instant::now();

    if body.is_empty() {
        return HttpResponse::json(
            "400 Bad Request",
            error_json("Request body cannot be empty", start_time.elapsed()),
        );
    }

    let content_type =
        find_header(headers, "content-type").map(|value| normalize_content_type(value));

    if let Some(ref ct) = content_type {
        let supported = ct.contains("application/json") || ct.contains("application/sql");
        if !supported {
            return HttpResponse::json(
                "415 Unsupported Media Type",
                error_json(
                    "Supported content types are application/json and application/sql",
                    start_time.elapsed(),
                ),
            );
        }
    }

    let allow_raw_sql = content_type
        .as_ref()
        .map(|ct| ct.contains("application/sql"))
        .unwrap_or(false);

    let request = match parse_query_payload(body, allow_raw_sql) {
        Ok(req) => req,
        Err(message) => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json(&message, start_time.elapsed()),
            );
        }
    };

    let provided_token = extract_auth_token(headers, request.auth_token);

    if let Some(expected) = state.auth_token.as_ref() {
        match provided_token {
            Some(ref token) if token == expected => {}
            _ => {
                return HttpResponse::json(
                    "401 Unauthorized",
                    error_json("Invalid or missing auth token", start_time.elapsed()),
                );
            }
        }
    }

    let statement = match state.parser.parse(&request.sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json(&format!("SQL parse error: {:?}", err), start_time.elapsed()),
            );
        }
    };

    let execution_result = {
        let mut db = match state.database.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                return HttpResponse::json(
                    "500 Internal Server Error",
                    error_json(
                        &format!("Database lock poisoned: {}", poisoned),
                        start_time.elapsed(),
                    ),
                );
            }
        };

        db.execute(statement)
    };

    match execution_result {
        Ok(rows) => {
            let elapsed = start_time.elapsed();
            let mut body = String::from("{");
            body.push_str("\"status\":\"ok\"");
            body.push_str(",\"status_code\":200");
            body.push_str(",\"row_count\":");
            body.push_str(&rows.len().to_string());
            body.push_str(",\"rows\":");
            body.push_str(&rows_to_json(&rows));
            if rows.is_empty() {
                body.push_str(",\"message\":\"Command executed successfully\"");
            }
            append_execution_time(&mut body, elapsed);
            body.push('}');

            HttpResponse::json("200 OK", body)
        }
        Err(err) => {
            let elapsed = start_time.elapsed();
            HttpResponse::json(
                "400 Bad Request",
                error_json(&database_error_to_string(err), elapsed),
            )
        }
    }
}

fn parse_query_payload(body: &[u8], allow_raw_sql: bool) -> Result<QueryRequest, String> {
    let text = std::str::from_utf8(body)
        .map_err(|_| "Request body must be valid UTF-8".to_string())?
        .trim();

    if text.is_empty() {
        return Err("Body must not be empty".to_string());
    }

    if text.starts_with('{') && text.ends_with('}') {
        return parse_query_request_json(text);
    }

    if allow_raw_sql {
        return Ok(QueryRequest {
            sql: text.to_string(),
            auth_token: None,
        });
    }

    Err("Body must be a JSON object with a 'sql' field".to_string())
}

fn parse_query_request_json(text: &str) -> Result<QueryRequest, String> {
    let sql =
        extract_json_string_field(text, "sql").ok_or_else(|| "Missing 'sql' field".to_string())?;

    let auth_token = extract_json_string_field(text, "auth_token")
        .or_else(|| extract_json_string_field(text, "token"))
        .or_else(|| extract_json_string_field(text, "auth"));

    Ok(QueryRequest { sql, auth_token })
}

fn extract_json_string_field(text: &str, field: &str) -> Option<String> {
    let pattern = format!("\"{}\"", field);
    let bytes = text.as_bytes();
    let mut search_start = 0;

    while let Some(relative_index) = text[search_start..].find(&pattern) {
        let key_index = search_start + relative_index;
        let mut idx = key_index + pattern.len();

        while idx < text.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        if idx >= text.len() || bytes[idx] != b':' {
            search_start = key_index + pattern.len();
            continue;
        }

        idx += 1;
        while idx < text.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        if idx >= text.len() || bytes[idx] != b'"' {
            search_start = key_index + pattern.len();
            continue;
        }

        idx += 1;
        let mut value = String::new();

        while idx < text.len() {
            let ch = bytes[idx];
            if ch == b'\\' {
                if idx + 1 >= text.len() {
                    return None;
                }
                let next = bytes[idx + 1];
                match next {
                    b'"' => value.push('"'),
                    b'\\' => value.push('\\'),
                    b'n' => value.push('\n'),
                    b't' => value.push('\t'),
                    b'r' => value.push('\r'),
                    b'b' => value.push('\u{0008}'),
                    b'f' => value.push('\u{000C}'),
                    other => value.push(other as char),
                }
                idx += 2;
            } else if ch == b'"' {
                return Some(value);
            } else {
                value.push(ch as char);
                idx += 1;
            }
        }

        return None;
    }

    None
}

fn rows_to_json(rows: &[Row]) -> String {
    let mut out = String::from("[");

    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            out.push(',');
        }
        out.push('{');

        let mut entries: Vec<_> = row.columns.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));

        for (col_idx, (column, value)) in entries.iter().enumerate() {
            if col_idx > 0 {
                out.push(',');
            }
            out.push('"');
            out.push_str(&escape_json_string(column));
            out.push_str("\":");
            append_sql_value(&mut out, value);
        }

        out.push('}');
    }

    out.push(']');
    out
}

fn append_sql_value(out: &mut String, value: &SqlValue) {
    match value {
        SqlValue::Integer(v) => out.push_str(&v.to_string()),
        SqlValue::Float(v) => {
            if v.is_finite() {
                out.push_str(&v.to_string());
            } else {
                out.push_str("null");
            }
        }
        SqlValue::Text(v) => {
            out.push('"');
            out.push_str(&escape_json_string(v));
            out.push('"');
        }
        SqlValue::Boolean(v) => out.push_str(if *v { "true" } else { "false" }),
        SqlValue::Null => out.push_str("null"),
    }
}

fn append_execution_time(body: &mut String, elapsed: Duration) {
    let elapsed_secs = elapsed.as_secs_f64();
    let elapsed_ms = elapsed_secs * 1000.0;
    body.push_str(",\"execution_time_ms\":");
    body.push_str(&format!("{:.3}", elapsed_ms));
    body.push_str(",\"execution_time\":\"");
    body.push_str(&format!("{:.3} sec", elapsed_secs));
    body.push('"');
}

fn error_json(message: &str, elapsed: Duration) -> String {
    let mut body = String::from("{");
    body.push_str("\"error\":\"");
    body.push_str(&escape_json_string(message));
    body.push_str("\"");
    append_execution_time(&mut body, elapsed);
    body.push('}');
    body
}

fn escape_json_string(input: &str) -> String {
    let mut escaped = String::new();

    for ch in input.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(escaped, "\\u{:04X}", ch as u32);
            }
            other => escaped.push(other),
        }
    }

    escaped
}

fn database_error_to_string(error: DatabaseError) -> String {
    match error {
        DatabaseError::TableNotFound(name) => format!("Table not found: {}", name),
        DatabaseError::ColumnNotFound(name) => format!("Column not found: {}", name),
        DatabaseError::ParseError(msg) => format!("Parse error: {}", msg),
        DatabaseError::IoError(msg) => format!("I/O error: {}", msg),
        DatabaseError::InvalidDataType(msg) => format!("Invalid data type: {}", msg),
        DatabaseError::UniqueConstraintViolation(msg) => {
            format!("Unique constraint violation: {}", msg)
        }
        DatabaseError::IndexAlreadyExists(name) => {
            format!("Index already exists: {}", name)
        }
        DatabaseError::IndexNotFound(name) => format!("Index not found: {}", name),
        DatabaseError::PrimaryKeyViolation(msg) => format!("Primary key violation: {}", msg),
    }
}

fn extract_auth_token(
    headers: &HashMap<String, String>,
    request_token: Option<String>,
) -> Option<String> {
    if let Some(token) = request_token {
        if !token.is_empty() {
            return Some(token);
        }
    }

    headers.get("authorization").and_then(|value| {
        let trimmed = value.trim();
        if let Some(rest) = trimmed.strip_prefix("Bearer ") {
            let token = rest.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
        if !trimmed.is_empty() {
            Some(trimmed.to_string())
        } else {
            None
        }
    })
}

fn read_full_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut data = Vec::new();
    let mut buffer = [0u8; 1024];

    loop {
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            break;
        }

        data.extend_from_slice(&buffer[..bytes_read]);

        if data.len() > MAX_REQUEST_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Request too large",
            ));
        }

        if request_complete(&data) {
            break;
        }
    }

    Ok(data)
}

fn request_complete(data: &[u8]) -> bool {
    if let Some(split_index) = find_double_crlf(data) {
        let header_bytes = &data[..split_index - 4];
        if let Some(content_length) = parse_content_length(header_bytes) {
            let total_expected = split_index + content_length;
            return data.len() >= total_expected;
        }

        return true;
    }

    false
}

fn find_double_crlf(data: &[u8]) -> Option<usize> {
    data.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

fn parse_content_length(header_bytes: &[u8]) -> Option<usize> {
    let header_text = String::from_utf8_lossy(header_bytes);

    for line in header_text.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("Content-Length") {
                if let Ok(length) = value.trim().parse::<usize>() {
                    return Some(length);
                }
            }
        }
    }

    None
}

fn split_request(data: &[u8]) -> Option<(String, &[u8])> {
    let header_end = find_double_crlf(data)?;
    let header_text = String::from_utf8_lossy(&data[..header_end - 4]).to_string();
    let body = &data[header_end..];
    Some((header_text, body))
}

fn parse_headers<'a, I>(lines: I) -> HashMap<String, String>
where
    I: Iterator<Item = &'a str>,
{
    let mut headers = HashMap::new();

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }

        if let Some((name, value)) = line.split_once(':') {
            let normalized_name: String = name
                .chars()
                .filter(|c| !c.is_ascii_whitespace())
                .collect::<String>()
                .to_ascii_lowercase();
            headers.insert(normalized_name, value.trim().to_string());
        }
    }

    headers
}

struct HttpResponse {
    status: &'static str,
    content_type: &'static str,
    body: String,
}

impl HttpResponse {
    fn json(status: &'static str, body: String) -> Self {
        Self {
            status,
            content_type: "application/json",
            body,
        }
    }

    fn text(status: &'static str, body: &str) -> Self {
        Self {
            status,
            content_type: "text/plain",
            body: body.to_string(),
        }
    }
}

fn write_http_response(stream: &mut TcpStream, response: &HttpResponse) -> std::io::Result<()> {
    let message = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}",
        status = response.status,
        content_type = response.content_type,
        length = response.body.as_bytes().len(),
        body = response.body,
    );

    stream.write_all(message.as_bytes())
}

fn normalize_content_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

fn find_header<'a>(headers: &'a HashMap<String, String>, target: &str) -> Option<&'a str> {
    let normalized_target = normalize_header_key(target);

    headers.iter().find_map(|(key, value)| {
        if normalize_header_key(key) == normalized_target {
            Some(value.as_str())
        } else {
            None
        }
    })
}

fn normalize_header_key(key: &str) -> String {
    let mut normalized = String::new();

    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        }
    }

    normalized
}
