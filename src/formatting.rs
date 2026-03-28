use once_cell::sync::Lazy;
use regex::Regex;

static GENERAL_NUMBER_FORMAT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[0-9]*(\.[0-9]*)?$").unwrap());

/// Format an integer as a fixed-decimal string.
/// e.g. `number_to_string(Some(1234), 2)` → `"12.34"`
/// e.g. `number_to_string(None, 2)`        → `""`
pub fn number_to_string(value: Option<i64>, decimal_digits: usize) -> String {
    let value = match value {
        None => return String::new(), // `return` inside `match` works fine in Rust
        Some(v) => v,
    };

    if decimal_digits < 1 {
        return value.to_string();
    }

    let base = value.to_string();

    // Pad with leading zeros so the string is long enough to split
    let padding = if decimal_digits + 1 > base.len() {
        "0".repeat(decimal_digits + 1 - base.len())
    } else {
        String::new()
    };
    let padded = format!("{}{}", padding, base);

    let split_at = padded.len() - decimal_digits;
    format!("{}.{}", &padded[..split_at], &padded[split_at..])
}

/// Like `number_to_string()` but strips trailing zeros and a trailing dot.
/// e.g. `trim_number_to_string(Some(1200), 2)` → `"12"` not `"12.00"`
pub fn trim_number_to_string(value: Option<i64>, decimal_digits: usize) -> String {
    let s = number_to_string(value, decimal_digits);
    if s.is_empty() || decimal_digits < 1 {
        return s;
    }
    // Strip trailing zeros then trailing dot
    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
    trimmed.to_string()
}

/// Parse a user-entered string into a fixed-point integer.
/// `decimal_digits` controls how many decimal places are implied.
/// e.g. `format_string_to_number("12.34", 2)` → `Some(1234)`
/// e.g. `format_string_to_number("abc", 2)`   → `None`
pub fn format_string_to_number(value: &str, decimal_digits: usize) -> Option<i64> {
    let value = value.trim();

    if !GENERAL_NUMBER_FORMAT.is_match(value) || value.is_empty() {
        return None;
    }

    // Build a regex that matches up to `decimal_digits` decimal places
    let num_pattern = if decimal_digits > 0 {
        format!(r"^[0-9]*(\.[0-9]{{0,{}}})?", decimal_digits)
    } else {
        r"^[0-9]*".to_string()
    };
    let num_re = Regex::new(&num_pattern).unwrap();

    let matched = num_re.find(value)?.as_str();

    if decimal_digits < 1 {
        return matched.parse::<i64>().ok();
    }

    if matched.contains('.') && !matched.ends_with('.') {
        // e.g. "12.3" with decimal_digits=2 → 1230
        let decimal_part_len = matched.split('.').nth(1).unwrap_or("").len();
        let without_dot = matched.replace('.', "");
        let int_val: i64 = without_dot.parse().ok()?;
        Some(int_val * 10_i64.pow((decimal_digits - decimal_part_len) as u32))
    } else {
        // e.g. "12" or "12." with decimal_digits=2 → 1200
        let without_dot = matched.trim_end_matches('.');
        let int_val: i64 = without_dot.parse().ok()?;
        Some(int_val * 10_i64.pow(decimal_digits as u32))
    }
}

/// Convert a display name into a safe SQLite table/column identifier.
/// Spaces become underscores; non-ascii letters are removed.
/// e.g. `string_to_db_name("Mµ Cool Table!")` → `"M_Cool_Table"`
pub fn string_to_db_name(display_name: &str) -> Result<String, String> {
    let result: String = display_name
        .chars()
        .map(|c| if c.is_whitespace() { '_' } else { c })
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .collect();
    if result.is_empty() {
        Err("Resulting table name would be empty".into())
    } else {
        Ok(result)
    }
}