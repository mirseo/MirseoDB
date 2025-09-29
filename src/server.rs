use super::auth::AuthConfig;
use super::configuration::ConfigManager;
use super::core_types::{DatabaseError, Row, SqlValue};
use super::engine::Database;
use super::routing::{forward_request, should_forward_request, ForwardRequest, RouteConfig};
use super::smart_parser::AnySQL;
use super::two_factor_auth::TwoFactorAuth;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

const CONSOLE_PROXY_ADDR: &str = "127.0.0.1:5173";

const MAX_PORT: u16 = 65535;
const MAX_REQUEST_SIZE: usize = 64 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(2);

const SUSPICIOUS_PATTERNS: &[(&str, &str)] = &[
    ("' or '1'='1", "'"),
    ("\" or \"1\"=\"1\"", "\""),
    ("' or 1=1", "'"),
    ("\" or 1=1", "\""),
    (" or 1=1", " "),
    (" or '1'='1", " "),
    (" or \"1\"=\"1\"", " "),
];

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
    route_config: Arc<RouteConfig>,
    auth_token: Option<String>,
    two_factor_auth: Arc<Mutex<TwoFactorAuth>>,
}

impl ApiServerState {
    fn new(
        database: Arc<Mutex<Database>>,
        parser: Arc<AnySQL>,
        route_config: Arc<RouteConfig>,
        auth_token: Option<String>,
    ) -> Self {
        let two_factor_auth = TwoFactorAuth::load().unwrap_or_else(|_| TwoFactorAuth::new());

        Self {
            health: HealthServerState::new(),
            database,
            parser,
            route_config,
            auth_token,
            two_factor_auth: Arc::new(Mutex::new(two_factor_auth)),
        }
    }
}

struct QueryRequest {
    sql: String,
    auth_token: Option<String>,
    totp_token: Option<String>, // 2차 인증 토큰
    email: Option<String>,      // 사용자 이메일
}

pub fn start_health_server(
    start_port: u16,
    database: Arc<Mutex<Database>>,
    parser: Arc<AnySQL>,
    route_config: Arc<RouteConfig>,
    auth_token: Option<String>,
) -> std::io::Result<u16> {
    let listener = bind_available_port(start_port)?;
    let port = listener.local_addr()?.port();
    let state = Arc::new(ApiServerState::new(
        database,
        parser,
        route_config,
        auth_token,
    ));

    thread::spawn({
        let state = Arc::clone(&state);
        move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let state = Arc::clone(&state);
                        thread::spawn(move || handle_client(stream, state));
                    }
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
            Some(HttpResponse::json("200 OK", state.health.health_payload()))
        }
        ("GET", "/time") => Some(handle_time_request()),
        ("GET", "/setup/status") => Some(handle_setup_status()),
        ("POST", "/setup/init") => Some(handle_setup_init(&state, &headers, body_bytes)),
        ("POST", "/setup/complete") => Some(handle_setup_complete(&state, &headers, body_bytes)),
        ("GET", "/query") => {
            Some(handle_get_query_request(&state, &headers, path))
        }
        ("POST", "/query") | ("PUT", "/query") | ("DELETE", "/query") | ("PATCH", "/query") | ("POST", "/api/query") => {
            Some(handle_query_request(&state, &headers, body_bytes))
        }
        ("POST", "/2fa/setup") => Some(handle_2fa_setup(&state, &headers, body_bytes)),
        ("GET", "/2fa/qr") => Some(handle_2fa_qr(&state, &headers)),
        ("POST", "/2fa/verify") => Some(handle_2fa_verify(&state, &headers, body_bytes)),
        _ => None,
    };

    if let Some(response) = response {
        let _ = write_http_response(&mut stream, &response);
        return;
    }

    proxy_to_console(stream, request_bytes);
}

fn proxy_to_console(mut client_stream: TcpStream, request_bytes: Vec<u8>) {
    match TcpStream::connect(CONSOLE_PROXY_ADDR) {
        Ok(mut console_stream) => {
            if let Err(err) = console_stream.write_all(&request_bytes) {
                eprintln!(
                    "[MirseoDB][console-proxy] Failed to write request to console server: {}",
                    err
                );
                let response = HttpResponse::text(
                    "502 Bad Gateway",
                    "Console dev server unavailable (could not write request)",
                );
                let _ = write_http_response(&mut client_stream, &response);
                return;
            }

            if let Err(err) = console_stream.flush() {
                eprintln!(
                    "[MirseoDB][console-proxy] Failed to flush request to console server: {}",
                    err
                );
                let response = HttpResponse::text(
                    "502 Bad Gateway",
                    "Console dev server unavailable (flush failed)",
                );
                let _ = write_http_response(&mut client_stream, &response);
                return;
            }

            let mut console_reader = match console_stream.try_clone() {
                Ok(stream) => stream,
                Err(err) => {
                    eprintln!(
                        "[MirseoDB][console-proxy] Failed to clone console stream: {}",
                        err
                    );
                    let response = HttpResponse::text(
                        "502 Bad Gateway",
                        "Console dev server unavailable (clone failed)",
                    );
                    let _ = write_http_response(&mut client_stream, &response);
                    return;
                }
            };

            let mut client_writer = match client_stream.try_clone() {
                Ok(stream) => stream,
                Err(err) => {
                    eprintln!(
                        "[MirseoDB][console-proxy] Failed to clone client stream: {}",
                        err
                    );
                    let response = HttpResponse::text(
                        "502 Bad Gateway",
                        "Console dev server unavailable (clone failed)",
                    );
                    let _ = write_http_response(&mut client_stream, &response);
                    return;
                }
            };

            let console_to_client = thread::spawn(move || {
                let _ = std::io::copy(&mut console_reader, &mut client_writer);
            });

            let mut client_reader = client_stream;
            let mut console_writer = console_stream;

            if let Err(err) = std::io::copy(&mut client_reader, &mut console_writer) {
                eprintln!(
                    "[MirseoDB][console-proxy] Error while piping client to console: {}",
                    err
                );
            }

            let _ = console_writer.shutdown(Shutdown::Write);
            let _ = console_to_client.join();
        }
        Err(err) => {
            eprintln!(
                "[MirseoDB][console-proxy] Failed to connect to console server at {}: {}",
                CONSOLE_PROXY_ADDR, err
            );
            let response = HttpResponse::text(
                "502 Bad Gateway",
                "Console dev server unavailable (connection failed)",
            );
            let _ = write_http_response(&mut client_stream, &response);
        }
    }
}

fn handle_query_request(
    state: &Arc<ApiServerState>,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> HttpResponse {
    let start_time = Instant::now();

    // Check if this is a forwarded request that should be ignored
    if should_forward_request(headers) {
        // This is a forwarded request, process normally but add forward mode indicator
        return handle_forwarded_query_request(state, headers, body, start_time);
    }

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

    let QueryRequest {
        sql: mut sql_text,
        auth_token: request_token,
        totp_token: request_totp,
        email: request_email,
    } = request;

    let provided_token = extract_auth_token(headers, request_token.clone());

    let mut sanitized_applied = false;
    let config = ConfigManager::load();
    if config.sql_injection_protect {
        if let Some(filtered) = sanitize_sql_input(&sql_text) {
            sanitized_applied = true;
            eprintln!("[MirseoDB][security] Suspicious SQL patterns detected; sanitized request");
            sql_text = filtered;
        }
    }

    if let Some(expected) = state.auth_token.as_ref() {
        match provided_token {
            Some(ref token) if token == expected => {}
            _ => {
                let mut body = error_json("Invalid or missing auth token", start_time.elapsed());
                if sanitized_applied {
                    insert_sanitized_flag(&mut body);
                }
                return HttpResponse::json("401 Unauthorized", body);
            }
        }
    }

    // Check if setup is completed first
    let auth_config = match AuthConfig::load() {
        Ok(config) => config,
        Err(e) => {
            let mut body = error_json(&format!("Auth config error: {}", e), start_time.elapsed());
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }
            return HttpResponse::json("500 Internal Server Error", body);
        }
    };

    if !auth_config.is_setup_completed() {
        let mut body = error_json(
            "Database setup not completed. Please complete initial setup at /setup/init",
            start_time.elapsed(),
        );
        if sanitized_applied {
            insert_sanitized_flag(&mut body);
        }
        return HttpResponse::json("503 Service Unavailable", body);
    }

    // Check email-based SQL permissions
    if let Some(email) = request_email.as_ref() {
        if !auth_config.check_sql_permission(email, &sql_text) {
            let user_role = auth_config.get_user_role(email).unwrap_or("unknown");
            let mut body = error_json(
                &format!(
                    "SQL permission denied for user '{}' with role '{}'",
                    email, user_role
                ),
                start_time.elapsed(),
            );
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }
            return HttpResponse::json("403 Forbidden", body);
        }
    }

    let statement = match state.parser.parse(&sql_text) {
        Ok(stmt) => stmt,
        Err(err) => {
            let mut body = error_json(&format!("SQL parse error: {:?}", err), start_time.elapsed());
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }
            return HttpResponse::json("400 Bad Request", body);
        }
    };

    // 민감한 작업인지 확인하고 2차 인증 검사
    if statement.requires_2fa() {
        let user_id = "default"; // 실제 구현에서는 적절한 사용자 ID를 사용해야 함

        // TOTP 토큰 확인
        match request_totp {
            Some(totp) if !totp.is_empty() => {
                let two_factor_auth = match state.two_factor_auth.lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        return HttpResponse::json(
                            "500 Internal Server Error",
                            error_json("2FA system error", start_time.elapsed()),
                        );
                    }
                };

                if !two_factor_auth.verify_token(user_id, &totp) {
                    let mut body = error_json(
                        &format!(
                            "2FA required for {} operation. Invalid or expired TOTP token.",
                            statement.get_operation_name()
                        ),
                        start_time.elapsed(),
                    );
                    if sanitized_applied {
                        insert_sanitized_flag(&mut body);
                    }
                    insert_2fa_required_flag(&mut body);
                    return HttpResponse::json("403 Forbidden", body);
                }
            }
            _ => {
                let mut body = error_json(
                    &format!("2FA required for {} operation. Please provide 'authtoken' field with your TOTP code.", 
                            statement.get_operation_name()),
                    start_time.elapsed(),
                );
                if sanitized_applied {
                    insert_sanitized_flag(&mut body);
                }
                insert_2fa_required_flag(&mut body);
                return HttpResponse::json("403 Forbidden", body);
            }
        }
    }

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
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }

            HttpResponse::json("200 OK", body)
        }
        Err(err) => {
            let elapsed = start_time.elapsed();

            // Check if we should forward the request to another server
            if let Some(fallback_server) = state.route_config.get_fallback_server() {
                if let Ok(forward_result) =
                    attempt_forward_request(state, headers, body, fallback_server)
                {
                    return forward_result;
                }
            }

            let mut body = error_json(&database_error_to_string(err), elapsed);
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }

            HttpResponse::json("400 Bad Request", body)
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
            totp_token: None,
            email: None,
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

    let totp_token = extract_json_string_field(text, "authtoken")
        .or_else(|| extract_json_string_field(text, "totp"))
        .or_else(|| extract_json_string_field(text, "totp_token"));

    let email = extract_json_string_field(text, "email")
        .or_else(|| extract_json_string_field(text, "user_email"))
        .or_else(|| extract_json_string_field(text, "user"));

    Ok(QueryRequest {
        sql,
        auth_token,
        totp_token,
        email,
    })
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

fn sanitize_sql_input(sql: &str) -> Option<String> {
    let mut sanitized = sql.to_string();
    let mut modified = false;

    for (pattern, replacement) in SUSPICIOUS_PATTERNS.iter() {
        let (updated, changed) = replace_case_insensitive(&sanitized, pattern, replacement);
        if changed {
            sanitized = updated;
            modified = true;
        }
    }

    if modified {
        Some(sanitized)
    } else {
        None
    }
}

fn replace_case_insensitive(input: &str, pattern: &str, replacement: &str) -> (String, bool) {
    if pattern.is_empty() {
        return (input.to_string(), false);
    }

    let pattern_lower = pattern.to_ascii_lowercase();
    let input_lower = input.to_ascii_lowercase();

    if !input_lower.contains(&pattern_lower) {
        return (input.to_string(), false);
    }

    let pattern_bytes = pattern_lower.as_bytes();
    let pattern_len = pattern_bytes.len();
    let lower_bytes = input_lower.as_bytes();

    let mut result = String::with_capacity(input.len());
    let mut last_index = 0usize;
    let mut index = 0usize;

    while index <= lower_bytes.len().saturating_sub(pattern_len) {
        if &lower_bytes[index..index + pattern_len] == pattern_bytes {
            result.push_str(&input[last_index..index]);
            result.push_str(replacement);
            index += pattern_len;
            last_index = index;
        } else {
            index += 1;
        }
    }

    result.push_str(&input[last_index..]);

    (result, true)
}

fn insert_2fa_required_flag(body: &mut String) {
    if let Some(pos) = body.rfind('}') {
        body.insert_str(pos, ",\"requires_2fa\":true");
    }
}

fn handle_2fa_setup(
    state: &Arc<ApiServerState>,
    headers: &HashMap<String, String>,
    _body: &[u8],
) -> HttpResponse {
    let start_time = Instant::now();

    // Basic API token 인증 확인
    if let Some(expected) = state.auth_token.as_ref() {
        let provided_token = extract_auth_token(headers, None);
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

    let user_id = "default"; // 실제 구현에서는 적절한 사용자 ID를 사용해야 함

    let mut two_factor_auth = match state.two_factor_auth.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return HttpResponse::json(
                "500 Internal Server Error",
                error_json("2FA system error", start_time.elapsed()),
            );
        }
    };

    match two_factor_auth.generate_secret_for_user(user_id) {
        Ok(secret) => {
            let mut body = String::from("{");
            body.push_str("\"status\":\"ok\"");
            body.push_str(",\"message\":\"2FA setup initiated\"");
            body.push_str(",\"secret\":\"");
            body.push_str(&escape_json_string(&secret));
            body.push_str("\"");
            body.push_str(",\"user_id\":\"");
            body.push_str(&escape_json_string(user_id));
            body.push_str("\"");
            append_execution_time(&mut body, start_time.elapsed());
            body.push('}');

            HttpResponse::json("200 OK", body)
        }
        Err(err) => HttpResponse::json(
            "500 Internal Server Error",
            error_json(
                &format!("Failed to setup 2FA: {}", err),
                start_time.elapsed(),
            ),
        ),
    }
}

fn handle_2fa_qr(state: &Arc<ApiServerState>, headers: &HashMap<String, String>) -> HttpResponse {
    let start_time = Instant::now();

    // Basic API token 인증 확인
    if let Some(expected) = state.auth_token.as_ref() {
        let provided_token = extract_auth_token(headers, None);
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

    let user_id = "default"; // 실제 구현에서는 적절한 사용자 ID를 사용해야 함

    let two_factor_auth = match state.two_factor_auth.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return HttpResponse::json(
                "500 Internal Server Error",
                error_json("2FA system error", start_time.elapsed()),
            );
        }
    };

    match two_factor_auth.generate_qr_code(user_id, "MirseoDB") {
        Ok(qr_ascii) => {
            let secret = two_factor_auth.get_setup_info(user_id).unwrap_or_default();

            let mut body = String::from("{");
            body.push_str("\"status\":\"ok\"");
            body.push_str(",\"qr_ascii\":\"");
            body.push_str(&escape_json_string(&qr_ascii));
            body.push_str("\"");
            body.push_str(",\"secret\":\"");
            body.push_str(&escape_json_string(&secret));
            body.push_str("\"");
            body.push_str(",\"instructions\":\"Install Google Authenticator or similar TOTP app and scan the QR code or manually enter the secret key.\"");
            append_execution_time(&mut body, start_time.elapsed());
            body.push('}');

            HttpResponse::json("200 OK", body)
        }
        Err(err) => HttpResponse::json(
            "400 Bad Request",
            error_json(
                &format!("2FA not setup for user: {}", err),
                start_time.elapsed(),
            ),
        ),
    }
}

fn handle_2fa_verify(
    state: &Arc<ApiServerState>,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> HttpResponse {
    let start_time = Instant::now();

    // Basic API token 인증 확인
    if let Some(expected) = state.auth_token.as_ref() {
        let provided_token = extract_auth_token(headers, None);
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

    if body.is_empty() {
        return HttpResponse::json(
            "400 Bad Request",
            error_json("Request body cannot be empty", start_time.elapsed()),
        );
    }

    let text = match std::str::from_utf8(body) {
        Ok(t) => t.trim(),
        Err(_) => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json("Request body must be valid UTF-8", start_time.elapsed()),
            );
        }
    };

    let totp_token = if text.starts_with('{') && text.ends_with('}') {
        // JSON format
        extract_json_string_field(text, "totp_token")
            .or_else(|| extract_json_string_field(text, "token"))
            .or_else(|| extract_json_string_field(text, "code"))
    } else {
        // Plain text token
        Some(text.to_string())
    };

    let totp_token = match totp_token {
        Some(token) if !token.is_empty() => token,
        _ => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json("TOTP token is required", start_time.elapsed()),
            );
        }
    };

    let user_id = "default"; // 실제 구현에서는 적절한 사용자 ID를 사용해야 함

    let two_factor_auth = match state.two_factor_auth.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return HttpResponse::json(
                "500 Internal Server Error",
                error_json("2FA system error", start_time.elapsed()),
            );
        }
    };

    let is_valid = two_factor_auth.verify_token(user_id, &totp_token);

    let mut body = String::from("{");
    body.push_str("\"status\":\"");
    body.push_str(if is_valid { "ok" } else { "error" });
    body.push_str("\"");
    body.push_str(",\"valid\":");
    body.push_str(if is_valid { "true" } else { "false" });
    body.push_str(",\"message\":\"");
    body.push_str(if is_valid {
        "TOTP token is valid"
    } else {
        "Invalid or expired TOTP token"
    });
    body.push_str("\"");
    append_execution_time(&mut body, start_time.elapsed());
    body.push('}');

    let status = if is_valid {
        "200 OK"
    } else {
        "400 Bad Request"
    };
    HttpResponse::json(status, body)
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
        DatabaseError::PermissionDenied(msg) => format!("Permission denied: {}", msg),
        DatabaseError::InvalidCredentials(msg) => format!("Invalid credentials: {}", msg),
        DatabaseError::TwoFactorAuthRequired(msg) => format!("Two-factor authentication required: {}", msg),
        DatabaseError::NetworkError(msg) => format!("Network error: {}", msg),
        DatabaseError::HttpError(msg) => format!("HTTP error: {}", msg),
        DatabaseError::InvalidSqlSyntax(msg) => format!("Invalid SQL syntax: {}", msg),
        DatabaseError::SqlInjectionDetected => format!("SQL injection attempt detected"),
        DatabaseError::QueryTooComplex => format!("Query too complex"),
        DatabaseError::InvalidIndexHint(msg) => format!("Invalid index hint: {}", msg),
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

fn handle_forwarded_query_request(
    state: &Arc<ApiServerState>,
    headers: &HashMap<String, String>,
    body: &[u8],
    start_time: Instant,
) -> HttpResponse {
    // This is a forwarded request, process it normally but indicate it's in forward mode
    if body.is_empty() {
        return HttpResponse::json(
            "400 Bad Request",
            error_json_with_mode("Request body cannot be empty", start_time.elapsed(), true),
        );
    }

    let content_type =
        find_header(headers, "content-type").map(|value| normalize_content_type(value));

    if let Some(ref ct) = content_type {
        let supported = ct.contains("application/json") || ct.contains("application/sql");
        if !supported {
            return HttpResponse::json(
                "415 Unsupported Media Type",
                error_json_with_mode(
                    "Supported content types are application/json and application/sql",
                    start_time.elapsed(),
                    true,
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
                error_json_with_mode(&message, start_time.elapsed(), true),
            );
        }
    };

    let QueryRequest {
        sql: mut sql_text,
        auth_token: request_token,
        totp_token: _request_totp, // 포워드 모드에서는 2FA 검사하지 않음
        email: request_email,
    } = request;

    let provided_token = extract_auth_token(headers, request_token.clone());

    let mut sanitized_applied = false;
    let config = ConfigManager::load();
    if config.sql_injection_protect {
        if let Some(filtered) = sanitize_sql_input(&sql_text) {
            sanitized_applied = true;
            eprintln!(
                "[MirseoDB][security] Suspicious SQL patterns detected; sanitized forwarded request"
            );
            sql_text = filtered;
        }
    }

    if let Some(expected) = state.auth_token.as_ref() {
        match provided_token {
            Some(ref token) if token == expected => {}
            _ => {
                let mut body = error_json_with_mode(
                    "Invalid or missing auth token",
                    start_time.elapsed(),
                    true,
                );
                if sanitized_applied {
                    insert_sanitized_flag(&mut body);
                }
                return HttpResponse::json("401 Unauthorized", body);
            }
        }
    }

    let statement = match state.parser.parse(&sql_text) {
        Ok(stmt) => stmt,
        Err(err) => {
            let mut body = error_json_with_mode(
                &format!("SQL parse error: {:?}", err),
                start_time.elapsed(),
                true,
            );
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }
            return HttpResponse::json("400 Bad Request", body);
        }
    };

    let execution_result = {
        let mut db = match state.database.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                return HttpResponse::json(
                    "500 Internal Server Error",
                    error_json_with_mode(
                        &format!("Database lock poisoned: {}", poisoned),
                        start_time.elapsed(),
                        true,
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
            body.push_str(",\"mode\":\"fd\""); // Indicate forward mode
            body.push_str(",\"row_count\":");
            body.push_str(&rows.len().to_string());
            body.push_str(",\"rows\":");
            body.push_str(&rows_to_json(&rows));
            if rows.is_empty() {
                body.push_str(",\"message\":\"Command executed successfully\"");
            }
            append_execution_time(&mut body, elapsed);
            body.push('}');
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }

            HttpResponse::json("200 OK", body)
        }
        Err(err) => {
            let elapsed = start_time.elapsed();
            let mut body = error_json_with_mode(&database_error_to_string(err), elapsed, true);
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }
            HttpResponse::json("400 Bad Request", body)
        }
    }
}

fn attempt_forward_request(
    _state: &Arc<ApiServerState>,
    headers: &HashMap<String, String>,
    body: &[u8],
    target_url: &str,
) -> Result<HttpResponse, String> {
    // Create forward request
    let forward_payload = ForwardRequest {
        method: "POST".to_string(),
        path: "/query".to_string(),
        headers: headers.clone(),
        body: body.to_vec(),
    };

    // Forward the request
    match forward_request(target_url, &forward_payload) {
        Ok(response) => {
            let mut response_body = response.body;

            // If the response doesn't contain mode:fd, add it to indicate forwarding occurred
            if !response_body.contains("\"mode\":\"fd\"") && response_body.starts_with('{') {
                // Insert mode:fd into the JSON response
                let close_brace_pos = response_body.rfind('}');
                if let Some(pos) = close_brace_pos {
                    response_body.insert_str(pos, ",\"mode\":\"fd\",\"forwarded\":true");
                }
            }

            let status = if response.status_code == 200 {
                "200 OK"
            } else {
                "400 Bad Request"
            };
            Ok(HttpResponse::json(status, response_body))
        }
        Err(e) => {
            eprintln!("[MirseoDB] Forward request failed: {}", e);
            Err(e)
        }
    }
}

fn error_json_with_mode(message: &str, elapsed: Duration, forward_mode: bool) -> String {
    let mut body = String::from("{");
    body.push_str("\"error\":\"");
    body.push_str(&escape_json_string(message));
    body.push_str("\"");
    if forward_mode {
        body.push_str(",\"mode\":\"fd\"");
    }
    append_execution_time(&mut body, elapsed);
    body.push('}');
    body
}

fn insert_sanitized_flag(body: &mut String) {
    if let Some(pos) = body.rfind('}') {
        body.insert_str(pos, ",\"sanitized\":true");
    }
}

fn handle_setup_status() -> HttpResponse {
    let auth_config = match AuthConfig::load() {
        Ok(config) => config,
        Err(e) => {
            return HttpResponse::json(
                "500 Internal Server Error",
                format!("{{\"error\":\"Failed to load auth config: {}\"}}", e),
            );
        }
    };

    let mut body = String::from("{");
    body.push_str("\"setup_completed\":");
    body.push_str(if auth_config.is_setup_completed() { "true" } else { "false" });

    if let Some(admin_email) = &auth_config.admin_email {
        body.push_str(",\"admin_email\":\"");
        body.push_str(&escape_json_string(admin_email));
        body.push_str("\"");
    }

    body.push_str(",\"user_count\":");
    body.push_str(&auth_config.emails.len().to_string());
    body.push('}');

    HttpResponse::json("200 OK", body)
}

fn handle_setup_init(
    state: &Arc<ApiServerState>,
    _headers: &HashMap<String, String>,
    body: &[u8],
) -> HttpResponse {
    let start_time = Instant::now();

    if body.is_empty() {
        return HttpResponse::json(
            "400 Bad Request",
            error_json("Request body cannot be empty", start_time.elapsed()),
        );
    }

    let text = match std::str::from_utf8(body) {
        Ok(t) => t.trim(),
        Err(_) => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json("Request body must be valid UTF-8", start_time.elapsed()),
            );
        }
    };

    let admin_email = if text.starts_with('{') && text.ends_with('}') {
        extract_json_string_field(text, "admin_email")
            .or_else(|| extract_json_string_field(text, "email"))
    } else {
        Some(text.to_string())
    };

    let admin_email = match admin_email {
        Some(email) if !email.is_empty() && email.contains('@') => email,
        _ => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json("Valid admin email is required", start_time.elapsed()),
            );
        }
    };

    let auth_config = match AuthConfig::load() {
        Ok(config) => config,
        Err(e) => {
            return HttpResponse::json(
                "500 Internal Server Error",
                error_json(&format!("Auth config error: {}", e), start_time.elapsed()),
            );
        }
    };

    if auth_config.is_setup_completed() {
        return HttpResponse::json(
            "400 Bad Request",
            error_json("Setup already completed", start_time.elapsed()),
        );
    }

    // 관리자용 2FA 설정 시작
    let user_id = &admin_email; // 이메일을 user_id로 사용

    let mut two_factor_auth = match state.two_factor_auth.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return HttpResponse::json(
                "500 Internal Server Error",
                error_json("2FA system error", start_time.elapsed()),
            );
        }
    };

    match two_factor_auth.generate_secret_for_user(user_id) {
        Ok(secret) => {
            let qr_result = two_factor_auth.generate_qr_code(user_id, "MirseoDB Admin Setup");

            let mut response_body = String::from("{");
            response_body.push_str("\"status\":\"ok\"");
            response_body.push_str(",\"message\":\"Admin setup initiated\"");
            response_body.push_str(",\"admin_email\":\"");
            response_body.push_str(&escape_json_string(&admin_email));
            response_body.push_str("\"");
            response_body.push_str(",\"secret\":\"");
            response_body.push_str(&escape_json_string(&secret));
            response_body.push_str("\"");

            if let Ok(qr_ascii) = qr_result {
                response_body.push_str(",\"qr_code\":\"");
                response_body.push_str(&escape_json_string(&qr_ascii));
                response_body.push_str("\"");
            }

            response_body.push_str(",\"setup_2fa\":true");
            response_body.push_str(",\"instructions\":\"Please setup 2FA using the secret key or QR code, then call /setup/complete with your TOTP token. You can also skip 2FA setup by calling /setup/complete with skip_2fa=true.\"");
            append_execution_time(&mut response_body, start_time.elapsed());
            response_body.push('}');

            HttpResponse::json("200 OK", response_body)
        }
        Err(err) => HttpResponse::json(
            "500 Internal Server Error",
            error_json(&format!("Failed to setup 2FA: {}", err), start_time.elapsed()),
        ),
    }
}

fn handle_setup_complete(
    state: &Arc<ApiServerState>,
    _headers: &HashMap<String, String>,
    body: &[u8],
) -> HttpResponse {
    let start_time = Instant::now();

    if body.is_empty() {
        return HttpResponse::json(
            "400 Bad Request",
            error_json("Request body cannot be empty", start_time.elapsed()),
        );
    }

    let text = match std::str::from_utf8(body) {
        Ok(t) => t.trim(),
        Err(_) => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json("Request body must be valid UTF-8", start_time.elapsed()),
            );
        }
    };

    if !text.starts_with('{') || !text.ends_with('}') {
        return HttpResponse::json(
            "400 Bad Request",
            error_json("Request body must be JSON", start_time.elapsed()),
        );
    }

    let admin_email = extract_json_string_field(text, "admin_email")
        .or_else(|| extract_json_string_field(text, "email"));

    let totp_token = extract_json_string_field(text, "totp_token")
        .or_else(|| extract_json_string_field(text, "token"));

    let skip_2fa = extract_json_string_field(text, "skip_2fa")
        .map(|s| s.to_lowercase() == "true")
        .unwrap_or(false);

    let admin_email = match admin_email {
        Some(email) if !email.is_empty() && email.contains('@') => email,
        _ => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json("Valid admin email is required", start_time.elapsed()),
            );
        }
    };

    let mut auth_config = match AuthConfig::load() {
        Ok(config) => config,
        Err(e) => {
            return HttpResponse::json(
                "500 Internal Server Error",
                error_json(&format!("Auth config error: {}", e), start_time.elapsed()),
            );
        }
    };

    if auth_config.is_setup_completed() {
        return HttpResponse::json(
            "400 Bad Request",
            error_json("Setup already completed", start_time.elapsed()),
        );
    }

    // 2FA 검증 (skip하지 않는 경우에만)
    if !skip_2fa {
        match totp_token {
            Some(token) if !token.is_empty() => {
                let two_factor_auth = match state.two_factor_auth.lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        return HttpResponse::json(
                            "500 Internal Server Error",
                            error_json("2FA system error", start_time.elapsed()),
                        );
                    }
                };

                if !two_factor_auth.verify_token(&admin_email, &token) {
                    return HttpResponse::json(
                        "400 Bad Request",
                        error_json("Invalid or expired TOTP token", start_time.elapsed()),
                    );
                }
            }
            _ => {
                return HttpResponse::json(
                    "400 Bad Request",
                    error_json("TOTP token required (or set skip_2fa=true)", start_time.elapsed()),
                );
            }
        }
    }

    // 설정 완료
    if let Err(e) = auth_config.complete_setup(admin_email.clone()) {
        return HttpResponse::json(
            "500 Internal Server Error",
            error_json(&format!("Failed to complete setup: {}", e), start_time.elapsed()),
        );
    }

    let mut response_body = String::from("{");
    response_body.push_str("\"status\":\"ok\"");
    response_body.push_str(",\"message\":\"Database setup completed successfully\"");
    response_body.push_str(",\"admin_email\":\"");
    response_body.push_str(&escape_json_string(&admin_email));
    response_body.push_str("\"");
    response_body.push_str(",\"2fa_enabled\":");
    response_body.push_str(if skip_2fa { "false" } else { "true" });
    response_body.push_str(",\"setup_completed\":true");
    append_execution_time(&mut response_body, start_time.elapsed());
    response_body.push('}');

    HttpResponse::json("200 OK", response_body)
}

fn handle_time_request() -> HttpResponse {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let unix_seconds = since_epoch.as_secs();
    let nano_offset = since_epoch.subsec_nanos();
    let timestamp_ms = since_epoch.as_millis();
    const NTP_UNIX_OFFSET: u64 = 2_208_988_800;
    let ntp_timestamp = unix_seconds + NTP_UNIX_OFFSET;

    let iso8601 = OffsetDateTime::from_unix_timestamp(unix_seconds as i64)
        .ok()
        .and_then(|dt| dt.replace_nanosecond(nano_offset).ok())
        .and_then(|dt| dt.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

    let mut body = String::from("{");
    body.push_str("\"time_server\":true");
    body.push_str(",\"ntp_timestamp\":");
    body.push_str(&ntp_timestamp.to_string());
    body.push_str(",\"unix_timestamp\":");
    body.push_str(&unix_seconds.to_string());
    body.push_str(",\"nano_offset\":");
    body.push_str(&nano_offset.to_string());
    body.push_str(",\"timestamp_ms\":");
    body.push_str(&timestamp_ms.to_string());
    body.push_str(",\"iso8601\":\"");
    body.push_str(&iso8601);
    body.push_str("\"");
    body.push('}');

    HttpResponse::json("200 OK", body)
}

fn handle_get_query_request(
    state: &Arc<ApiServerState>,
    headers: &HashMap<String, String>,
    path: &str,
) -> HttpResponse {
    let start_time = Instant::now();

    let sql = if let Some(query_start) = path.find('?') {
        let query_string = &path[query_start + 1..];
        parse_url_query_params(query_string).get("sql").cloned()
    } else {
        None
    };

    let sql = match sql {
        Some(s) if !s.is_empty() => s,
        _ => {
            return HttpResponse::json(
                "400 Bad Request",
                error_json("Missing 'sql' query parameter", start_time.elapsed()),
            );
        }
    };

    let request = QueryRequest {
        sql: url_decode(&sql),
        auth_token: extract_auth_token(headers, None),
        totp_token: None,
        email: None,
    };

    execute_query_request(state, request, start_time, false)
}

fn parse_url_query_params(query_string: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();

    for pair in query_string.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            params.insert(key.to_string(), value.to_string());
        }
    }

    params
}

fn url_decode(input: &str) -> String {
    let mut result = String::new();
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '%' {
            if let (Some(hex1), Some(hex2)) = (chars.next(), chars.next()) {
                if let Ok(byte) = u8::from_str_radix(&format!("{}{}", hex1, hex2), 16) {
                    result.push(byte as char);
                } else {
                    result.push(ch);
                    result.push(hex1);
                    result.push(hex2);
                }
            } else {
                result.push(ch);
            }
        } else if ch == '+' {
            result.push(' ');
        } else {
            result.push(ch);
        }
    }

    result
}

fn execute_query_request(
    state: &Arc<ApiServerState>,
    request: QueryRequest,
    start_time: Instant,
    sanitized_applied: bool,
) -> HttpResponse {
    let QueryRequest {
        sql: mut sql_text,
        auth_token: request_token,
        totp_token: request_totp,
        email: request_email,
    } = request;

    let provided_token = extract_auth_token(&HashMap::new(), request_token.clone());

    let mut sanitized_applied = sanitized_applied;
    let config = ConfigManager::load();
    if config.sql_injection_protect {
        if let Some(filtered) = sanitize_sql_input(&sql_text) {
            sanitized_applied = true;
            eprintln!("[MirseoDB][security] Suspicious SQL patterns detected; sanitized request");
            sql_text = filtered;
        }
    }

    if let Some(expected) = state.auth_token.as_ref() {
        match provided_token {
            Some(ref token) if token == expected => {}
            _ => {
                let mut body = error_json("Invalid or missing auth token", start_time.elapsed());
                if sanitized_applied {
                    insert_sanitized_flag(&mut body);
                }
                return HttpResponse::json("401 Unauthorized", body);
            }
        }
    }

    let auth_config = match AuthConfig::load() {
        Ok(config) => config,
        Err(e) => {
            let mut body = error_json(&format!("Auth config error: {}", e), start_time.elapsed());
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }
            return HttpResponse::json("500 Internal Server Error", body);
        }
    };

    if !auth_config.is_setup_completed() {
        let mut body = error_json(
            "Database setup not completed. Please complete initial setup at /setup/init",
            start_time.elapsed(),
        );
        if sanitized_applied {
            insert_sanitized_flag(&mut body);
        }
        return HttpResponse::json("503 Service Unavailable", body);
    }

    if let Some(email) = request_email.as_ref() {
        if !auth_config.check_sql_permission(email, &sql_text) {
            let user_role = auth_config.get_user_role(email).unwrap_or("unknown");
            let mut body = error_json(
                &format!(
                    "SQL permission denied for user '{}' with role '{}'",
                    email, user_role
                ),
                start_time.elapsed(),
            );
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }
            return HttpResponse::json("403 Forbidden", body);
        }
    }

    let statement = match state.parser.parse(&sql_text) {
        Ok(stmt) => stmt,
        Err(err) => {
            let mut body = error_json(&format!("SQL parse error: {:?}", err), start_time.elapsed());
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }
            return HttpResponse::json("400 Bad Request", body);
        }
    };

    if statement.requires_2fa() {
        let user_id = "default";

        match request_totp {
            Some(totp) if !totp.is_empty() => {
                let two_factor_auth = match state.two_factor_auth.lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        return HttpResponse::json(
                            "500 Internal Server Error",
                            error_json("2FA system error", start_time.elapsed()),
                        );
                    }
                };

                if !two_factor_auth.verify_token(user_id, &totp) {
                    let mut body = error_json(
                        &format!(
                            "2FA required for {} operation. Invalid or expired TOTP token.",
                            statement.get_operation_name()
                        ),
                        start_time.elapsed(),
                    );
                    if sanitized_applied {
                        insert_sanitized_flag(&mut body);
                    }
                    insert_2fa_required_flag(&mut body);
                    return HttpResponse::json("403 Forbidden", body);
                }
            }
            _ => {
                let mut body = error_json(
                    &format!("2FA required for {} operation. Please provide 'authtoken' field with your TOTP code.",
                            statement.get_operation_name()),
                    start_time.elapsed(),
                );
                if sanitized_applied {
                    insert_sanitized_flag(&mut body);
                }
                insert_2fa_required_flag(&mut body);
                return HttpResponse::json("403 Forbidden", body);
            }
        }
    }

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
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }

            HttpResponse::json("200 OK", body)
        }
        Err(err) => {
            let elapsed = start_time.elapsed();

            if let Some(fallback_server) = state.route_config.get_fallback_server() {
                if let Ok(forward_result) =
                    attempt_forward_request(state, &HashMap::new(), &[], fallback_server)
                {
                    return forward_result;
                }
            }

            let mut body = error_json(&database_error_to_string(err), elapsed);
            if sanitized_applied {
                insert_sanitized_flag(&mut body);
            }

            HttpResponse::json("400 Bad Request", body)
        }
    }
}
