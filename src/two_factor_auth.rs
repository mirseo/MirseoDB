use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const TOTP_DIGITS: usize = 6;
const TOTP_PERIOD: u64 = 30;
const BASE32_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

#[derive(Debug, Clone)]
pub struct TwoFactorAuth {
    secrets: HashMap<String, String>, // user_id -> secret
}

impl TwoFactorAuth {
    pub fn new() -> Self {
        Self {
            secrets: HashMap::new(),
        }
    }

    pub fn load() -> Result<Self, String> {
        let config_path = ".mirseoDB/2fa_secrets.dat";

        if !Path::new(config_path).exists() {
            return Ok(Self::new());
        }

        let content = fs::read_to_string(config_path)
            .map_err(|e| format!("Failed to read 2FA config: {}", e))?;

        let mut secrets = HashMap::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((user_id, secret)) = line.split_once(':') {
                secrets.insert(user_id.trim().to_string(), secret.trim().to_string());
            }
        }

        Ok(Self { secrets })
    }

    pub fn save(&self) -> Result<(), String> {
        let config_dir = ".mirseoDB";
        if !Path::new(config_dir).exists() {
            fs::create_dir_all(config_dir)
                .map_err(|e| format!("Failed to create config directory: {}", e))?;
        }

        let config_path = format!("{}/2fa_secrets.dat", config_dir);
        let mut content = String::new();
        content.push_str("# MirseoDB 2FA Secrets\n");
        content.push_str("# Format: user_id:secret\n\n");

        for (user_id, secret) in &self.secrets {
            content.push_str(&format!("{}:{}\n", user_id, secret));
        }

        fs::write(&config_path, content).map_err(|e| format!("Failed to write 2FA config: {}", e))
    }

    pub fn generate_secret_for_user(&mut self, user_id: &str) -> Result<String, String> {
        let secret = generate_random_secret();
        self.secrets.insert(user_id.to_string(), secret.clone());
        self.save()?;
        Ok(secret)
    }

    pub fn verify_token(&self, user_id: &str, token: &str) -> bool {
        if let Some(secret) = self.secrets.get(user_id) {
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Check current time window and adjacent windows for clock drift tolerance
            for time_offset in [-1, 0, 1] {
                let time_counter =
                    (current_time as i64 + (time_offset * TOTP_PERIOD as i64)) as u64 / TOTP_PERIOD;
                let expected_token = generate_totp(secret, time_counter);
                if token == expected_token {
                    return true;
                }
            }
        }
        false
    }

    pub fn generate_qr_code(&self, user_id: &str, issuer: &str) -> Result<String, String> {
        let secret = self
            .secrets
            .get(user_id)
            .ok_or_else(|| "User not found".to_string())?;

        let otpauth_url = format!(
            "otpauth://totp/{}:{}?secret={}&issuer={}",
            issuer, user_id, secret, issuer
        );

        generate_qr_ascii(&otpauth_url)
    }

    pub fn get_setup_info(&self, user_id: &str) -> Option<String> {
        self.secrets.get(user_id).cloned()
    }

    pub fn has_user(&self, user_id: &str) -> bool {
        self.secrets.contains_key(user_id)
    }
}

fn generate_random_secret() -> String {
    // Generate a 20-byte random secret and encode it in base32
    let mut secret_bytes = [0u8; 20];

    // Simple pseudo-random number generation using system time
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    for byte in &mut secret_bytes {
        seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
        *byte = (seed >> 24) as u8;
    }

    base32_encode(&secret_bytes)
}

fn base32_encode(input: &[u8]) -> String {
    let mut result = String::new();
    let mut bits = 0u32;
    let mut value = 0u32;

    for &byte in input {
        value = (value << 8) | (byte as u32);
        bits += 8;

        while bits >= 5 {
            bits -= 5;
            let index = ((value >> bits) & 0x1F) as usize;
            result.push(BASE32_ALPHABET[index] as char);
        }
    }

    if bits > 0 {
        let index = ((value << (5 - bits)) & 0x1F) as usize;
        result.push(BASE32_ALPHABET[index] as char);
    }

    result
}

fn base32_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut result = Vec::new();
    let mut bits = 0u32;
    let mut value = 0u32;

    for ch in input.chars() {
        let index = BASE32_ALPHABET
            .iter()
            .position(|&c| c == ch as u8)
            .ok_or_else(|| format!("Invalid base32 character: {}", ch))?;

        value = (value << 5) | (index as u32);
        bits += 5;

        if bits >= 8 {
            bits -= 8;
            result.push((value >> bits) as u8);
        }
    }

    Ok(result)
}

fn generate_totp(secret: &str, time_counter: u64) -> String {
    let key = base32_decode(secret).unwrap_or_default();
    let counter_bytes = time_counter.to_be_bytes();

    let hash = hmac_sha1(&key, &counter_bytes);
    let offset = (hash[hash.len() - 1] & 0x0f) as usize;

    let binary = ((hash[offset] & 0x7f) as u32) << 24
        | ((hash[offset + 1] & 0xff) as u32) << 16
        | ((hash[offset + 2] & 0xff) as u32) << 8
        | (hash[offset + 3] & 0xff) as u32;

    let otp = binary % 10_u32.pow(TOTP_DIGITS as u32);
    format!("{:0width$}", otp, width = TOTP_DIGITS)
}

// Simple HMAC-SHA1 implementation
fn hmac_sha1(key: &[u8], message: &[u8]) -> Vec<u8> {
    const BLOCK_SIZE: usize = 64;
    const IPAD: u8 = 0x36;
    const OPAD: u8 = 0x5c;

    let mut k = if key.len() > BLOCK_SIZE {
        sha1(key)
    } else {
        key.to_vec()
    };

    // Pad key to block size
    k.resize(BLOCK_SIZE, 0);

    // Create inner and outer padded keys
    let mut inner_key = vec![0u8; BLOCK_SIZE];
    let mut outer_key = vec![0u8; BLOCK_SIZE];

    for i in 0..BLOCK_SIZE {
        inner_key[i] = k[i] ^ IPAD;
        outer_key[i] = k[i] ^ OPAD;
    }

    // HMAC = SHA1(outer_key || SHA1(inner_key || message))
    let mut inner_hash_input = inner_key;
    inner_hash_input.extend_from_slice(message);
    let inner_hash = sha1(&inner_hash_input);

    let mut outer_hash_input = outer_key;
    outer_hash_input.extend_from_slice(&inner_hash);
    sha1(&outer_hash_input)
}

// Simple SHA-1 implementation
fn sha1(input: &[u8]) -> Vec<u8> {
    let mut h = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];

    let mut message = input.to_vec();
    let original_len = message.len();

    // Padding
    message.push(0x80);
    while (message.len() + 8) % 64 != 0 {
        message.push(0);
    }

    // Append original length as 64-bit big-endian
    let bit_len = (original_len * 8) as u64;
    message.extend_from_slice(&bit_len.to_be_bytes());

    // Process message in 512-bit chunks
    for chunk in message.chunks_exact(64) {
        let mut w = [0u32; 80];

        // Break chunk into sixteen 32-bit words
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }

        // Extend the words
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let mut a: u32 = h[0];
        let mut b: u32 = h[1];
        let mut c: u32 = h[2];
        let mut d: u32 = h[3];
        let mut e: u32 = h[4];

        for i in 0..80 {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                60..=79 => (b ^ c ^ d, 0xCA62C1D6),
                _ => unreachable!(),
            };

            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);

            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }

    // Convert to bytes
    let mut result = Vec::new();
    for word in h {
        result.extend_from_slice(&word.to_be_bytes());
    }

    result
}

fn generate_qr_ascii(data: &str) -> Result<String, String> {
    // Simple ASCII QR code representation
    // This is a simplified representation - not a real QR code
    let mut result = String::new();
    result.push_str(&format!("┌─ MirseoDB 2FA Setup ─┐\n"));
    result.push_str(&format!("│ Scan with authenticator │\n"));
    result.push_str(&format!("│ app (Google, Authy, etc)│\n"));
    result.push_str(&format!("├─────────────────────────┤\n"));

    // Create a simple pattern based on the data hash
    let hash = sha1(data.as_bytes());
    let size = 17; // QR code size

    for y in 0..size {
        result.push_str("│ ");
        for x in 0..size {
            let index = ((y * size + x) / 8) % hash.len();
            let bit_index = (y * size + x) % 8;
            let bit = (hash[index] >> bit_index) & 1;
            result.push(if bit == 1 { '█' } else { ' ' });
        }
        result.push_str(" │\n");
    }

    result.push_str(&format!("├─────────────────────────┤\n"));
    result.push_str(&format!("│ Manual setup key:       │\n"));

    // Break the URL into smaller chunks for display
    let url_parts: Vec<&str> = data.split('?').collect();
    if url_parts.len() >= 2 {
        let params: Vec<&str> = url_parts[1].split('&').collect();
        for param in params {
            if param.starts_with("secret=") {
                let secret = &param[7..];
                result.push_str(&format!("│ {}│\n", format!("{:23}", secret)));
                break;
            }
        }
    }

    result.push_str(&format!("└─────────────────────────┘\n"));
    result.push_str("\n");
    result.push_str("Setup Instructions:\n");
    result.push_str("1. Install Google Authenticator or similar TOTP app\n");
    result.push_str("2. Add account manually using the secret key above\n");
    result.push_str("3. Use the 6-digit code for authentication\n");

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base32_encode_decode() {
        let data = b"Hello World";
        let encoded = base32_encode(data);
        let decoded = base32_decode(&encoded).unwrap();
        assert_eq!(data.to_vec(), decoded);
    }

    #[test]
    fn test_totp_generation() {
        let secret = "JBSWY3DPEHPK3PXP";
        let time_counter = 1;
        let token = generate_totp(secret, time_counter);
        assert_eq!(token.len(), 6);
        assert!(token.chars().all(|c| c.is_ascii_digit()));
    }
}
