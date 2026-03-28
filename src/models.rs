use serde::{Deserialize, Serialize};
use crate::formatting;
use crate::store::{ConnectionType, DataAccess, RequestResult};

type Result<T> = RequestResult<T>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ColumnType {
    Text,
    Date,
    Integer,
    Boolean,
}

impl ColumnType {
    /// The type name for use in the different kind of connections.
    pub fn get_type(&self, conn_type: ConnectionType) -> &'static str {
        match conn_type {
            ConnectionType::SQLite => match self {
                ColumnType::Text | ColumnType::Date => "TEXT",
                ColumnType::Integer => "INTEGER",
                ColumnType::Boolean => "BOOLEAN",
            }
        }
    }
}
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
            CellValue::Integer(v) => formatting::trim_number_to_string(*v, decimal_digits),
        }
    }

    /// The default/empty value for a given column type.
    pub fn default_for(col_type: &ColumnType) -> Self {
        match col_type {
            ColumnType::Text => CellValue::Text(String::new()),
            ColumnType::Date => CellValue::Text(String::new()),
            ColumnType::Boolean => CellValue::Boolean(false),
            ColumnType::Integer => CellValue::Integer(None),
        }
    }
}

/// Describes a single column in a user-defined category table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CategoryColumn {
    /// The SQLite column name (snake_case, no spaces)
    pub internal_name: String,

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

fn default_col_type() -> ColumnType {
    ColumnType::Text
}

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
            internal_name: col_name.into(),
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

            ColumnType::Date => SearchValue::Text(trimmed.to_string()),

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
                            formatting::format_string_to_number(rest, self.decimal_digits),
                        );
                    }
                }
                // No operator — treat as exact match
                match formatting::format_string_to_number(trimmed, self.decimal_digits) {
                    Some(n) => SearchValue::Exact(n),
                    None => SearchValue::Skip,
                }
            }
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Category {
    /// The row ID in the `master` table. -1 means not yet saved.
    pub id: i64,

    /// The internal name used, if the DataAccess needs a different kind if naming convention
    pub internal_name: String,

    /// The human-readable name shown in the UI
    pub display_name: String,

    /// The column definitions for this category
    pub columns: Vec<CategoryColumn>,
}

impl Category {
    pub fn new(display_name: impl Into<String>, columns: Vec<CategoryColumn>) -> Self {
        let display_name = display_name.into();
        Category {
            id: -1,
            internal_name: "".into(),
            display_name,
            columns,
        }
    }

    pub fn save(&mut self, conn: &impl DataAccess) -> Result<()> {
        (self.internal_name, self.id) = conn.save_category(&self.display_name, &self.columns)?;
        Ok(())
    }

    pub fn update_columns(&mut self, conn: &impl DataAccess, new_columns: &[CategoryColumn], old_column_names: &[Option<String>]) -> Result<()> {
        todo!()
    }

    pub fn update_self(&mut self, conn: &impl DataAccess, new_display_name: &str) -> Result<()> {
        todo!()
    }

    pub fn delete(self, conn: &impl DataAccess) -> Result<()> {
        conn.delete_category(self)
    }

    pub fn insert_entry(&self, conn: &impl DataAccess, row: &[CellValue]) -> Result<()> {
        conn.insert_entry(self, row)
    }

    pub fn update_entry(&self, conn: &impl DataAccess, old_row: &[CellValue], new_row: &[CellValue]) -> Result<()> {
        conn.update_entry(self, old_row, new_row)
    }

    pub fn delete_entry(&self, conn: &impl DataAccess, row: &[CellValue]) -> Result<()> {
        conn.delete_entry(self, row)
    }

    pub fn load_entries(&self, conn: &impl DataAccess, amount: u32, offset: u32, filters: Option<&[String]>) -> Result<(Vec<Vec<CellValue>>, bool)> {
        match filters {
            None => {
                let filters: Vec<SearchValue> = self.columns.iter().map(|_| SearchValue::Skip).collect();
                conn.load_entries(self, amount, offset, &filters)
            }
            Some(s) => {
                let filters: Vec<SearchValue> = self.columns.iter().zip(s.iter()).map(|(col, raw)| col.parse_search(raw)).collect();
                conn.load_entries(self, amount, offset, &filters)
            }
        }
    }

    pub fn load_entries_for_display(&self, conn: &impl DataAccess, amount: u32, offset: u32, filters: Option<&[String]>) -> Result<(Vec<Vec<String>>, bool)> {
        match self.load_entries(conn, amount, offset, filters) {
            Ok((res, next)) => {Ok((self.rows_to_display(&res), next))}
            Err(err) => {Err(err)}
        }
    }

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

    /// List of all internal column names as a vector
    pub fn internal_column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| c.internal_name.clone())
            .collect::<Vec<String>>()
    }

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
}