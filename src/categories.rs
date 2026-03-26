// =============================================================================
// categories.rs
//
// This is a translation of the Python CategoryColumn + Category classes into
// Rust. It covers column definitions, type handling, number formatting, and
// all the SQLite CRUD operations (insert, update, delete, upsert, query).
//
// =============================================================================

use once_cell::sync::Lazy;
// `once_cell::sync::Lazy` lets us initialise a value the first time it's
// accessed, and then reuse it forever. We use this for compiled regexes
// (same pattern as your module-level `re.compile(...)` in Python).

use regex::Regex;
use rusqlite::{params, Connection, Result as SqlResult, Row};
// `rusqlite` is the SQLite library. `params!` is a macro for binding query
// parameters safely (equivalent to the `?` placeholders you already use).

use serde::{Deserialize, Serialize};
// `serde` is Rust's serialisation framework. Adding `#[derive(Serialize,
// Deserialize)]` to a struct gives it free JSON read/write support.

use serde_json;
// We use this instead of Python's `eval()` — safe, structured, no code execution.

use std::sync::{Mutex, OnceLock};
// `Mutex` wraps a value so only one thread can access it at a time.
// `OnceLock` is like `Lazy` but initialised explicitly by calling code.

// =============================================================================
// DATABASE SINGLETON
//
// This is the Rust equivalent of your module-level `con` global, but wrapped
// safely. `OnceLock` ensures the connection is set up exactly once.
// `Mutex` ensures only one piece of code touches it at a time.
//
// Usage anywhere in your code:
//   let db = db();          // get a reference to the Mutex-wrapped connection
//   let conn = db.lock().unwrap();  // lock it (like acquiring a lock in Python)
//   conn.execute(...)?;     // use it
//   // lock releases automatically when `conn` goes out of scope
// =============================================================================

static DB: OnceLock<Mutex<Connection>> = OnceLock::new();

/// Call this once at startup (in `main`) to initialise the database.
/// Loads from disk into an in-memory DB, exactly like your Python version.
pub fn init_db(disk_path: &str) -> SqlResult<()> {
    // Open the on-disk database file
    let disk_db = Connection::open(disk_path)?;

    // Create a fresh in-memory database
    let mut mem_db = Connection::open_in_memory()?;

    // Copy everything from disk into memory (equivalent to `database_on_disk.backup(con)`)
    // rusqlite's backup API works slightly differently — we restore *from* disk *into* memory.
    {
        let backup = rusqlite::backup::Backup::new(&disk_db, &mut mem_db)?;
        backup.run_to_completion(100, std::time::Duration::from_millis(250), None)?;
    }

    // Store the in-memory connection in our global singleton.
    // `set` fails if called twice — that's intentional, we only want one DB.
    DB.set(Mutex::new(mem_db))
        .map_err(|_| rusqlite::Error::InvalidParameterName("DB already initialised".into()))?;

    Ok(())
}

/// Get a reference to the global DB mutex. Panics if `init_db` was never called.
/// This is the function you call everywhere instead of importing `con` directly.
pub fn db() -> &'static Mutex<Connection> {
    DB.get().expect("Database not initialised — call init_db() first in main()")
}

/// Save the in-memory DB back to disk. Call this on shutdown.
/// Equivalent to your `update_disk_db()` function.
pub fn save_db_to_disk(disk_path: &str) -> SqlResult<()> {
    let conn = db().lock().unwrap();
    let mut disk_db = Connection::open(disk_path)?;
    let backup = rusqlite::backup::Backup::new(&*conn, &mut disk_db)?;
    backup.run_to_completion(100, std::time::Duration::from_millis(250), None)?;
    Ok(())
}

// =============================================================================
// NUMBER FORMATTING
//
// Direct port of your `format_number`, `format_number_trim`, and
// `format_string_to_number` helper functions.
// =============================================================================

// Pre-compiled regexes — equivalent to your module-level `re.compile(...)`.
// `Lazy` means they're built the first time they're used, then cached forever.
static GENERAL_NUMBER_FORMAT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[0-9]*(\.[0-9]*)?$").unwrap());

/// Format an integer as a fixed-decimal string.
/// e.g. `format_number(Some(1234), 2)` → `"12.34"`
/// e.g. `format_number(None, 2)`        → `""`
pub fn format_number(value: Option<i64>, decimal_digits: usize) -> String {
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

/// Like `format_number` but strips trailing zeros and a trailing dot.
/// e.g. `format_number_trim(Some(1200), 2)` → `"12"` not `"12.00"`
pub fn format_number_trim(value: Option<i64>, decimal_digits: usize) -> String {
    let s = format_number(value, decimal_digits);
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

    if !GENERAL_NUMBER_FORMAT.is_match(value) || value == "" {
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
/// Spaces become underscores; non-letters are removed.
/// e.g. `to_db_name("My Cool Table!")` → `"My_Cool_Table"`
pub fn to_db_name(display_name: &str) -> String {
    display_name
        .chars()
        .map(|c| if c.is_whitespace() { '_' } else { c })
        .filter(|c| c.is_alphabetic() || *c == '_')
        .collect()
}

// =============================================================================
// COLUMN TYPE
//
// In Python you used string constants ("TEXT", "INTEGER", "BOOLEAN") stored in
// a `TableTypes` class. In Rust we use an `enum` — a type that can be exactly
// one of a fixed set of variants. This is safer because the compiler will warn
// you if you forget to handle a case.
// =============================================================================

/// Represents the SQLite type of a column.
/// `#[derive(...)]` auto-generates useful trait implementations:
///   - `Debug`:   lets you print it with `println!("{:?}", col_type)`
///   - `Clone`:   lets you copy it with `.clone()`
///   - `PartialEq`: lets you compare with `==`
///   - `Serialize/Deserialize`: lets serde_json read/write it as a string
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
// ^ This tells serde to write the enum as "TEXT", "INTEGER", "BOOLEAN" in JSON
//   (matching your existing Python data format).
pub enum ColumnType {
    Text,
    Integer,
    Boolean,
}

impl ColumnType {
    /// The SQLite type name for use in CREATE TABLE statements.
    pub fn sql_type(&self) -> &'static str {
        // `match` in Rust is like `switch` in other languages, but exhaustive —
        // the compiler forces you to handle every variant.
        match self {
            ColumnType::Text => "TEXT",
            ColumnType::Integer => "INTEGER",
            ColumnType::Boolean => "BOOLEAN",
        }
    }
}

// =============================================================================
// SEARCH VALUE
//
// Your Python `transform_string_for_search` returns a union type:
//   str | bool | int | None | tuple[str, int | None]
//
// Rust doesn't have union types, but enums with data serve the same purpose.
// Each variant can carry its own payload.
// =============================================================================

/// The result of parsing a user's search string for a particular column type.
pub enum SearchValue {
    /// A string column search — use LIKE %value%
    Text(String),
    /// A boolean/integer exact match
    Exact(i64),
    /// An operator-based numeric search, e.g. "> 100"
    /// The String is the operator (">", "<=", etc.), the Option<i64> is the number.
    Comparison(String, Option<i64>),
    /// The input was empty or unparseable — skip this column
    Skip,
}

// =============================================================================
// CELL VALUE
//
// Your Python code uses `list[Any]` to represent a row, where each cell can
// be a string, int, bool, or None. Rust doesn't have `Any` like Python does,
// so we define our own enum that covers exactly the types you use.
// =============================================================================

/// A single cell value in a data row.
/// This replaces Python's `Any` for row data.
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue {
    Text(String),
    Integer(Option<i64>),  // Option because integers can be NULL in SQLite
    Boolean(bool),
}

impl CellValue {
    /// Convert this value to its display string for the UI.
    /// Equivalent to `transform_personal_value_to_string` in Python.
    pub fn to_display_string(&self, decimal_digits: usize) -> String {
        match self {
            CellValue::Text(s) => s.clone(),
            CellValue::Boolean(true) => "✅".to_string(),
            CellValue::Boolean(false) => "❌".to_string(),
            CellValue::Integer(v) => format_number_trim(*v, decimal_digits),
        }
    }

    /// The default/empty value for a given column type.
    pub fn default_for(col_type: &ColumnType) -> Self {
        match col_type {
            ColumnType::Text => CellValue::Text(String::new()),
            ColumnType::Boolean => CellValue::Boolean(false),
            ColumnType::Integer => CellValue::Integer(None),
        }
    }
}

// =============================================================================
// CATEGORY COLUMN
//
// Direct equivalent of your Python `CategoryColumn` class.
// `#[derive(Serialize, Deserialize)]` replaces your manual
// `load_columns_from_dict` / `transform_columns_to_dict` — serde handles the
// JSON roundtrip automatically, and it's safe (no `eval()`).
// =============================================================================

/// Describes a single column in a user-defined category table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CategoryColumn {
    /// The SQLite column name (snake_case, no spaces)
    pub col_name: String,

    /// The human-readable name shown in the UI
    pub display_name: String,

    /// Whether this column is part of the primary key
    #[serde(default)] // if missing from JSON, default to false
    pub is_primary_key: bool,

    /// The data type of this column
    #[serde(default = "default_col_type")]
    pub col_type: ColumnType,

    /// How many decimal places integers are stored with (e.g. 2 means values
    /// are stored as cents — 1234 represents 12.34)
    #[serde(default)]
    pub decimal_digits: usize,

    /// If true, this column auto-increments on new rows
    #[serde(default)]
    pub incrementer: bool,

    /// If true, the UI should show a multi-line text area instead of a single line
    #[serde(default)]
    pub text_area: bool,
}

// serde needs a plain function (not a closure) for `default = "..."` above
fn default_col_type() -> ColumnType {
    ColumnType::Text
}

impl CategoryColumn {
    /// Create a new column with all fields specified.
    pub fn new(
        col_name: impl Into<String>,
        display_name: impl Into<String>,
        is_primary_key: bool,
        col_type: ColumnType,
        decimal_digits: usize,
        incrementer: bool,
        text_area: bool,
    ) -> Self {
        // `impl Into<String>` means the caller can pass either a `&str` or a
        // `String` — Rust will convert automatically. You'll see this pattern
        // a lot in well-written Rust libraries.
        CategoryColumn {
            col_name: col_name.into(),
            display_name: display_name.into(),
            is_primary_key,
            col_type,
            decimal_digits,
            incrementer,
            text_area,
        }
    }

    /// Parse a user search string into a typed `SearchValue`.
    /// Equivalent to Python's `transform_string_for_search`.
    pub fn parse_search(&self, input: &str) -> SearchValue {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return SearchValue::Skip;
        }

        match &self.col_type {
            ColumnType::Text => SearchValue::Text(trimmed.to_string()),

            ColumnType::Boolean => {
                // Any of these strings counts as "true"
                let upper = trimmed.to_uppercase();
                let is_true = matches!(upper.as_str(), "1" | "YES" | "TRUE");
                SearchValue::Exact(is_true as i64)
            }

            ColumnType::Integer => {
                // Check if the user typed an operator prefix like "> 100"
                let operators = ["== ", "= ", ">= ", "<= ", "!= ", "<> ", "> ", "< "];
                for op in &operators {
                    if trimmed.starts_with(op) {
                        let rest = &trimmed[op.len()..];
                        let op_str = op.trim().to_string();
                        // Normalise "==" and "=" both to "="
                        let op_str = if op_str == "==" { "=".to_string() } else { op_str };
                        return SearchValue::Comparison(
                            op_str,
                            format_string_to_number(rest, self.decimal_digits),
                        );
                    }
                }
                // No operator — treat as exact match
                match format_string_to_number(trimmed, self.decimal_digits) {
                    Some(n) => SearchValue::Exact(n),
                    None => SearchValue::Skip,
                }
            }
        }
    }

    /// Read a `CellValue` for this column from a SQLite row.
    /// `rusqlite` gives us a `Row` object; we pull the right type based on
    /// this column's `col_type`.
    pub fn read_from_row(&self, row: &Row, index: usize) -> SqlResult<CellValue> {
        match &self.col_type {
            ColumnType::Text => {
                let s: String = row.get(index)?;
                Ok(CellValue::Text(s))
            }
            ColumnType::Boolean => {
                let v: i64 = row.get(index)?;
                Ok(CellValue::Boolean(v != 0))
            }
            ColumnType::Integer => {
                // Integers can be NULL in SQLite, so we use `Option<i64>`
                let v: Option<i64> = row.get(index)?;
                Ok(CellValue::Integer(v))
            }
        }
    }

    /// Convert a `CellValue` into the `rusqlite::types::Value` needed to bind
    /// it as a SQL parameter.
    pub fn to_sql_value(&self, cell: &CellValue) -> rusqlite::types::Value {
        match cell {
            CellValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
            CellValue::Boolean(b) => rusqlite::types::Value::Integer(*b as i64),
            CellValue::Integer(Some(n)) => rusqlite::types::Value::Integer(*n),
            CellValue::Integer(None) => rusqlite::types::Value::Null,
        }
    }
}

// =============================================================================
// CATEGORY
//
// Equivalent to your Python `Category` class. Owns a list of `CategoryColumn`s
// and handles all the SQL generation and execution.
//
// One important Rust difference: we don't store pre-built SQL strings as fields
// (like `__primary_key_statement`) because in Rust borrowing rules make that
// tricky when the struct also owns the data they reference. Instead, we compute
// them on the fly via small methods — this is zero-cost in practice.
// =============================================================================

/// A user-defined data category, backed by a SQLite table.
#[derive(Clone, PartialEq, Debug)]
pub struct Category {
    /// The row ID in the `master` table. -1 means not yet saved.
    pub id: i64,

    /// The SQLite table name (no spaces, letters and underscores only)
    pub db_name: String,

    /// The human-readable name shown in the UI
    pub display_name: String,

    /// The column definitions for this category
    pub columns: Vec<CategoryColumn>,
}

impl Category {
    /// Create a new, unsaved category.
    pub fn new(display_name: impl Into<String>, columns: Vec<CategoryColumn>) -> Self {
        let display_name = display_name.into();
        let db_name = to_db_name(&display_name);
        Category {
            id: -1,
            db_name,
            display_name,
            columns,
        }
    }

    // ---- SQL helpers --------------------------------------------------------
    // These compute the SQL fragments used in queries. They're private (`fn`
    // without `pub`) — only this struct's methods use them.

    /// Comma-separated list of all column names: `"col1, col2, col3"`
    fn col_list(&self) -> String {
        self.columns
            .iter()
            .map(|c| c.col_name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `col1 = ?, col2 = ?, ...` — used in UPDATE SET clauses
    fn full_update_clause(&self) -> String {
        self.columns
            .iter()
            .map(|c| format!("{} = ?", c.col_name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `pk1 = ? AND pk2 = ?` — used in WHERE clauses for primary key lookups
    fn primary_key_where(&self) -> String {
        self.columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| format!("{} = ?", c.col_name))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    /// Indices of primary key columns within `self.columns`
    fn primary_key_positions(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_primary_key)
            .map(|(i, _)| i)
            .collect()
    }

    /// Names of primary key columns
    fn primary_key_names(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.col_name.as_str())
            .collect()
    }

    /// Serialise column definitions to JSON for storage in the `master` table.
    /// This replaces Python's `str(CategoryColumn.transform_columns_to_dict(...))`
    /// and is safe — no `eval()` needed to read it back.
    fn columns_to_json(&self) -> String {
        // `serde_json::to_string` can only fail for types with non-string keys —
        // ours are fine, so `.unwrap()` is safe here.
        serde_json::to_string(&self.columns).unwrap()
    }

    /// Deserialise columns from JSON stored in the `master` table.
    fn columns_from_json(json: &str) -> Result<Vec<CategoryColumn>, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// The CREATE TABLE statement for this category.
    fn create_table_sql(&self) -> String {
        let col_defs: Vec<String> = self
            .columns
            .iter()
            .map(|c| format!("{} {}", c.col_name, c.col_type.sql_type()))
            .collect();

        let pk_names = self.primary_key_names().join(", ");

        format!(
            "CREATE TABLE {} ({}, PRIMARY KEY ({}));",
            self.db_name,
            col_defs.join(", "),
            pk_names
        )
    }

    /// A default (empty) row for this category — used as a base when importing.
    pub fn default_row(&self) -> Vec<CellValue> {
        self.columns
            .iter()
            .map(|c| CellValue::default_for(&c.col_type))
            .collect()
    }

    // ---- Display name search -----------------------------------------------

    /// Find the index of a column by its display name (case-insensitive).
    /// Accepts one name or a list of alternatives (e.g. ["Title", "Name"]).
    /// Returns `None` if not found. Equivalent to Python's `column_position`.
    pub fn column_position(
        &self,
        display_names: &[&str],
        col_type: &ColumnType,
        decimal_digits: Option<usize>,
    ) -> Option<usize> {
        // Uppercase all the search names once
        let upper_names: Vec<String> = display_names.iter().map(|n| n.to_uppercase()).collect();

        self.columns.iter().position(|col| {
            upper_names.contains(&col.display_name.to_uppercase())
                && col.col_type == *col_type
                && decimal_digits.map_or(true, |d| d == col.decimal_digits)
        })
    }

    pub fn has_column(&self, display_names: &[&str], col_type: &ColumnType, decimal_digits: Option<usize>) -> bool {
        self.column_position(display_names, col_type, decimal_digits).is_some()
    }

    // ---- Database operations -----------------------------------------------

    /// Load all categories from the database.
    /// Creates the `master` table if it doesn't exist yet.
    /// Equivalent to Python's `Category.load_categories_from_db()`.
    pub fn load_all() -> SqlResult<Vec<Category>> {
        let conn = db().lock().unwrap();

        // Check whether the master table exists yet
        let master_exists: bool = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='master'",
            [],
            |row| row.get::<_, i64>(0),
        )? > 0;

        if !master_exists {
            conn.execute_batch(
                "CREATE TABLE master (
                    ID      INTEGER PRIMARY KEY AUTOINCREMENT,
                    DB_NAME TEXT UNIQUE,
                    NAME    TEXT,
                    COLUMNS TEXT
                );"
            )?;
            return Ok(vec![]);
        }

        // Load every row from master
        let mut stmt = conn.prepare(
            "SELECT ID, DB_NAME, NAME, COLUMNS FROM master ORDER BY ID"
        )?;

        // `query_map` iterates over rows and maps each to a value.
        // The closure `|row| { ... }` runs once per row.
        let categories: SqlResult<Vec<Category>> = stmt
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let db_name: String = row.get(1)?;
                let display_name: String = row.get(2)?;
                let columns_json: String = row.get(3)?;

                // Parse columns from JSON (safe — no eval!)
                let columns = Category::columns_from_json(&columns_json)
                    .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;

                Ok(Category { id, db_name, display_name, columns })
            })?
            .collect();
        // `.collect()` here gathers all the `Result<Category>` values into
        // `Result<Vec<Category>>` — if any row fails, the whole thing fails.

        categories
    }

    /// Persist this category to the database for the first time.
    /// Creates the master row and the user-data table.
    /// Equivalent to Python's `add_category()`.
    pub fn save(&mut self) -> SqlResult<()> {
        let conn = db().lock().unwrap();
        let columns_json = self.columns_to_json();

        // Insert into master and get the auto-generated ID back
        // `last_insert_rowid()` is the Rust equivalent of `RETURNING ID`
        conn.execute(
            "INSERT INTO master (DB_NAME, NAME, COLUMNS) VALUES (?1, ?2, ?3)",
            params![self.db_name, self.display_name, columns_json],
        )?;
        self.id = conn.last_insert_rowid();

        // Create the user-data table
        conn.execute_batch(&self.create_table_sql())?;

        Ok(())
    }

    /// Update the category's metadata in the master table.
    /// Does not touch user data. Equivalent to Python's `_update_in_db()`.
    pub fn update_meta(&self) -> SqlResult<()> {
        if self.id == -1 {
            return Ok(()); // Not saved yet — nothing to update
        }
        let conn = db().lock().unwrap();
        let columns_json = self.columns_to_json();
        conn.execute(
            "UPDATE master SET DB_NAME = ?1, NAME = ?2, COLUMNS = ?3 WHERE ID = ?4",
            params![self.db_name, self.display_name, columns_json, self.id],
        )?;
        Ok(())
    }

    /// Delete this category and all its data.
    /// Equivalent to Python's `delete_me()`.
    pub fn delete(self) -> SqlResult<()> {
        let conn = db().lock().unwrap();
        conn.execute("DELETE FROM master WHERE ID = ?1", params![self.id])?;
        conn.execute_batch(&format!("DROP TABLE {};", self.db_name))?;
        Ok(())
    }

    // ---- Row operations ----------------------------------------------------

    /// Insert a new row. Equivalent to Python's `add_entry()`.
    pub fn insert_row(&self, row: &[CellValue]) -> SqlResult<()> {
        let col_list = self.col_list();
        let placeholders = (1..=row.len())
            .map(|i| format!("?{}", i))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.db_name, col_list, placeholders
        );

        let conn = db().lock().unwrap();
        // We convert each CellValue to a SQL-compatible value using the column's
        // `to_sql_value` method, then collect into a Vec for binding.
        let params: Vec<rusqlite::types::Value> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| self.columns[i].to_sql_value(cell))
            .collect();

        conn.execute(&sql, rusqlite::params_from_iter(params))?;
        Ok(())
    }

    /// Insert or update a row (upsert). Equivalent to Python's `upsert_entry()`.
    pub fn upsert_row(&self, row: &[CellValue]) -> SqlResult<()> {
        let col_list = self.col_list();
        let pk_names = self.primary_key_names().join(", ");
        let placeholders = (1..=row.len())
            .map(|i| format!("?{}", i))
            .collect::<Vec<_>>()
            .join(", ");

        // For the ON CONFLICT SET clause, we need a fresh set of placeholders
        // that start after the INSERT placeholders
        let update_start = row.len() + 1;
        let update_clause: Vec<String> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{} = ?{}", c.col_name, update_start + i))
            .collect();

        let pk_where_start = update_start + row.len();
        let pk_positions = self.primary_key_positions();
        let pk_where: Vec<String> = self
            .columns
            .iter()
            .filter(|c| c.is_primary_key)
            .enumerate()
            .map(|(i, c)| format!("{} = ?{}", c.col_name, pk_where_start + i))
            .collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {} WHERE {}",
            self.db_name,
            col_list,
            placeholders,
            pk_names,
            update_clause.join(", "),
            pk_where.join(" AND ")
        );

        // Build params: row values, then row values again for SET, then PK values for WHERE
        let mut params: Vec<rusqlite::types::Value> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| self.columns[i].to_sql_value(cell))
            .collect();
        // Append row again for the SET clause
        for (i, cell) in row.iter().enumerate() {
            params.push(self.columns[i].to_sql_value(cell));
        }
        // Append primary key values for the WHERE clause
        for &pk_pos in &pk_positions {
            params.push(self.columns[pk_pos].to_sql_value(&row[pk_pos]));
        }

        let conn = db().lock().unwrap();
        conn.execute(&sql, rusqlite::params_from_iter(params))?;
        Ok(())
    }

    /// Update all fields of a row identified by its old primary key values.
    /// Equivalent to Python's `do_full_update()`.
    pub fn update_row(&self, old_row: &[CellValue], new_row: &[CellValue]) -> SqlResult<()> {
        let set_clause = self.full_update_clause();
        let where_clause = self.primary_key_where();
        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            self.db_name, set_clause, where_clause
        );

        let mut params: Vec<rusqlite::types::Value> = new_row
            .iter()
            .enumerate()
            .map(|(i, cell)| self.columns[i].to_sql_value(cell))
            .collect();

        for &pk_pos in &self.primary_key_positions() {
            params.push(self.columns[pk_pos].to_sql_value(&old_row[pk_pos]));
        }

        let conn = db().lock().unwrap();
        conn.execute(&sql, rusqlite::params_from_iter(params))?;
        Ok(())
    }

    /// Update a single field by position. Equivalent to Python's `do_partial_update()`.
    pub fn update_cell(&self, old_row: &[CellValue], new_value: CellValue, position: usize) -> SqlResult<()> {
        let col_name = &self.columns[position].col_name;
        let where_clause = self.primary_key_where();
        let sql = format!(
            "UPDATE {} SET {} = ?1 WHERE {}",
            self.db_name, col_name, where_clause
        );

        let mut params = vec![self.columns[position].to_sql_value(&new_value)];
        for &pk_pos in &self.primary_key_positions() {
            params.push(self.columns[pk_pos].to_sql_value(&old_row[pk_pos]));
        }

        let conn = db().lock().unwrap();
        conn.execute(&sql, rusqlite::params_from_iter(params))?;
        Ok(())
    }

    /// Delete a row by its primary key. Equivalent to Python's `delete_entry()`.
    pub fn delete_row(&self, row: &[CellValue]) -> SqlResult<()> {
        let where_clause = self.primary_key_where();
        let sql = format!("DELETE FROM {} WHERE {}", self.db_name, where_clause);

        let params: Vec<rusqlite::types::Value> = self
            .primary_key_positions()
            .iter()
            .map(|&i| self.columns[i].to_sql_value(&row[i]))
            .collect();

        let conn = db().lock().unwrap();
        conn.execute(&sql, rusqlite::params_from_iter(params))?;
        Ok(())
    }

    // ---- Querying ----------------------------------------------------------

    /// Read a single row by its primary key. Equivalent to Python's `load_entry()`.
    pub fn load_row(&self, key_row: &[CellValue]) -> SqlResult<Option<Vec<CellValue>>> {
        let col_list = self.col_list();
        let where_clause = self.primary_key_where();
        let sql = format!(
            "SELECT {} FROM {} WHERE {}",
            col_list, self.db_name, where_clause
        );

        let params: Vec<rusqlite::types::Value> = self
            .primary_key_positions()
            .iter()
            .map(|&i| self.columns[i].to_sql_value(&key_row[i]))
            .collect();

        let conn = db().lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params))?;

        // `rows.next()` returns `Ok(Some(row))` if there's a row, `Ok(None)` if not
        if let Some(row) = rows.next()? {
            let cells: SqlResult<Vec<CellValue>> = self
                .columns
                .iter()
                .enumerate()
                .map(|(i, col)| col.read_from_row(row, i))
                .collect();
            Ok(Some(cells?))
        } else {
            Ok(None)
        }
    }

    /// Query the full table ordered by all columns.
    /// Equivalent to Python's `query_full_table()`.
    pub fn query_all(&self) -> SqlResult<Vec<Vec<CellValue>>> {
        let col_list = self.col_list();
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            col_list, self.db_name, col_list
        );
        self.run_query(&sql, vec![])
    }

    /// Query the first page of results, with optional search filters.
    /// Returns the rows and a boolean indicating whether more pages exist.
    /// Equivalent to Python's `query_first_page()`.
    pub fn query_page(
        &self,
        page: usize,           // 0-indexed page number
        page_size: usize,
        filters: Option<&[String]>,
    ) -> SqlResult<(Vec<Vec<CellValue>>, bool)> {
        let col_list = self.col_list();
        let (where_clause, params) = self.build_where_clause(filters);
        let offset = page * page_size;

        // Fetch one extra row to detect whether a next page exists
        let sql = format!(
            "SELECT {} FROM {} {} ORDER BY {} LIMIT {} OFFSET {}",
            col_list, self.db_name, where_clause, col_list,
            page_size + 1, offset
        );

        let mut rows = self.run_query(&sql, params)?;
        let has_more = rows.len() > page_size;
        rows.truncate(page_size);
        Ok((rows, has_more))
    }

    /// Convert a page of raw `CellValue` rows to display strings.
    /// Equivalent to Python's `transform_query_into_string()`.
    pub fn rows_to_display(&self, rows: &[Vec<CellValue>]) -> Vec<Vec<String>> {
        rows.iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .map(|(i, cell)| cell.to_display_string(self.columns[i].decimal_digits))
                    .collect()
            })
            .collect()
    }

    // ---- Internal helpers --------------------------------------------------

    /// Run a SELECT query and return typed rows. Used internally.
    fn run_query(&self, sql: &str, params: Vec<rusqlite::types::Value>) -> SqlResult<Vec<Vec<CellValue>>> {
        let conn = db().lock().unwrap();
        let mut stmt = conn.prepare(sql)?;
        let columns = &self.columns; // borrow for use in closure

        let rows: SqlResult<Vec<Vec<CellValue>>> = stmt
            .query_map(rusqlite::params_from_iter(params), |row| {
                columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| col.read_from_row(row, i))
                    .collect()
            })?
            .collect();
        rows
    }

    /// Build a WHERE clause from user filter strings.
    /// Equivalent to Python's `_build_dynamic_where()`.
    fn build_where_clause(&self, filters: Option<&[String]>) -> (String, Vec<rusqlite::types::Value>) {
        let filters = match filters {
            None => return (String::new(), vec![]),
            Some(f) => f,
        };

        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<rusqlite::types::Value> = Vec::new();

        for (col, raw) in self.columns.iter().zip(filters.iter()) {
            match col.parse_search(raw) {
                SearchValue::Skip => {}

                SearchValue::Text(s) => {
                    conditions.push(format!("{} LIKE ? ESCAPE '\\'", col.col_name));
                    params.push(rusqlite::types::Value::Text(format!("%{}%", s)));
                }

                SearchValue::Exact(n) => {
                    conditions.push(format!("{} = ?", col.col_name));
                    params.push(rusqlite::types::Value::Integer(n));
                }

                SearchValue::Comparison(op, Some(n)) => {
                    conditions.push(format!("{} {} ?", col.col_name, op));
                    params.push(rusqlite::types::Value::Integer(n));
                }

                SearchValue::Comparison(_, None) => {
                    // Unparseable number in a comparison — skip this filter
                }
            }
        }

        if conditions.is_empty() {
            return (String::new(), vec![]);
        }

        (format!("WHERE {}", conditions.join(" AND ")), params)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Error::SqliteFailure;
    use rusqlite::ErrorCode::ConstraintViolation;
    use rusqlite::ffi::Error;
    use super::*;

    #[test]
    fn test_database() {
        let path = "./test.sqlite";
        test_database_clean(path);
        test_database_conn(path);

        // Create a category and that is properly saved to the DB so that it loads exactly as desired too
        let mut movies = Category::new(
            "Movies",
            vec![
                CategoryColumn::new("Title", "Title", true, ColumnType::Text, 0, false, false),
                CategoryColumn::new("Year", "Year", true, ColumnType::Integer, 0, false, false),
                CategoryColumn::new("Watched", "Watched", false, ColumnType::Boolean, 0, false, false),
                CategoryColumn::new("Rating", "Rating", false, ColumnType::Integer, 1, false, false),
                CategoryColumn::new("Notes", "Notes", false, ColumnType::Text, 0, false, true),
            ],
        );

        let manipulation_row = &mut [
            CellValue::Text("The Grand Budapest Hotel".into()),
            CellValue::Integer(Some(2014)),
            CellValue::Boolean(true),
            CellValue::Integer(Some(55)),
            CellValue::Text("Wonderful film ".into()),
        ];

        assert_eq!(movies.save(), Ok(()));

        test_database_load_categories(movies.clone());
        test_database_inserts(movies.clone(), manipulation_row);
        test_database_queries(movies.clone());
        test_database_manipulation(movies, manipulation_row);

        assert_eq!(save_db_to_disk(path), Ok(()));
    }


    fn test_database_clean(path: &str) {
        match std::fs::remove_file(path) {
            Ok(_) => {}
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {}
                _ => {
                    panic!("Failed to remove test file: {}", e);
                }
            }
        }
    }

    fn test_database_load_categories(movies: Category) {
        match Category::load_all() {
            Ok(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0], movies);
            }
            Err(e) => {
                panic!("Failed load the database categories: {}", e);
            }
        }
    }

    fn test_database_inserts(movies: Category, old_row: &[CellValue]) {
        // Try inserting two different rows
        assert_eq!(movies.insert_row(old_row), Ok(()));
        let double_entry = &[
            CellValue::Text("Fast and Furious".into()),
            CellValue::Integer(Some(2014)),
            CellValue::Boolean(false),
            CellValue::Integer(Some(20)),
            CellValue::Text("Eh film".into()),
        ];
        assert_eq!(movies.insert_row(double_entry), Ok(()));
        assert_eq!(movies.insert_row(double_entry),
                   Err(SqliteFailure(
                       Error{code: ConstraintViolation, extended_code: 1555},
                       Some("UNIQUE constraint failed: Movies.Title, Movies.Year".parse().unwrap())
                   )));
    }

    fn test_database_conn(path: &str) {
        // check if the init works correctly and the basic DB features are added
        assert_eq!(init_db(path), Ok(()));
        match Category::load_all() {
            Ok(v) => {
                assert_eq!(v.len(), 0)
            }
            Err(e) => {
                panic!("Failed init the database: {}", e);
            }
        }
    }

    fn test_database_queries(movies: Category) {
        let (rows, has_more) = movies.query_page(0, 10, None).expect("Query failed");
        assert_eq!(rows.len(), 2);
        assert_eq!(has_more, false);

        // Test the result transformer too
        assert_eq!(movies.rows_to_display(&rows),
                   &[&["Fast and Furious", "2014", "❌", "2", "Eh film"],
                     &["The Grand Budapest Hotel", "2014", "✅", "5.5", "Wonderful film "]]);

        let (rows, has_more) = movies.query_page(0, 1, None).expect("Second Query failed");
        assert_eq!(rows.len(), 1);
        assert_eq!(has_more, true);
    }

    fn test_database_manipulation(movies: Category, old_row: &mut[CellValue]) {
        assert_eq!(movies.update_cell(old_row, CellValue::Boolean(false), 2), Ok(()));
        old_row[2] = CellValue::Boolean(false);
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(None, 2), "");
        assert_eq!(format_number(None, 0), "");
        assert_eq!(format_number(Some(124), 0), "124");
        assert_eq!(format_number(Some(125), 2), "1.25");
    }

    #[test]
    fn test_format_number_trim() {
        assert_eq!(format_number_trim(None, 2), "");
        assert_eq!(format_number_trim(None, 0), "");
        assert_eq!(format_number_trim(Some(1230), 2), "12.3");
        assert_eq!(format_number_trim(Some(1231), 2), "12.31");
        assert_eq!(format_number_trim(Some(1230), 0), "1230");
    }

    #[test]
    fn test_format_string_to_number() {
        assert_eq!(format_string_to_number("", 12), None);
        assert_eq!(format_string_to_number("12", 2), Some(1200));
        assert_eq!(format_string_to_number("0.12", 2), Some(12));
        assert_eq!(format_string_to_number(".1", 2), Some(10));
        assert_eq!(format_string_to_number(".", 2), None);
        assert_eq!(format_string_to_number(".", 0), None);
        assert_eq!(format_string_to_number("132", 0), Some(132));
        assert_eq!(format_string_to_number("6", 0), Some(6));
    }
}