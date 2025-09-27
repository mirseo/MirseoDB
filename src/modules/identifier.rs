pub fn normalize_identifier(token: &str) -> String {
    token
        .trim()
        .trim_matches(|ch| matches!(ch, '[' | ']' | '`' | '"' | '\'' | ';'))
        .to_string()
}

pub fn normalize_table_name(token: &str) -> String {
    let trimmed = token.trim();
    let is_quoted = is_quoted_identifier(trimmed);
    let cleaned = normalize_identifier(token);

    if cleaned.is_empty() {
        cleaned
    } else if is_quoted {
        cleaned
    } else {
        cleaned.to_ascii_uppercase()
    }
}

fn is_quoted_identifier(token: &str) -> bool {
    if token.len() < 2 {
        return false;
    }

    let first = token.chars().next().unwrap();
    let last = token.chars().last().unwrap();

    matches!(
        (first, last),
        ('"', '"') | ('`', '`') | ('[', ']') | ('\'', '\'')
    )
}
