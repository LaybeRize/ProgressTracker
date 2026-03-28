use std::sync::Mutex;
use rusqlite::{params, Connection, Row, Result as SqliteResult};
use crate::formatting;
use crate::models::{Category, CategoryColumn, CellValue, ColumnType, SearchValue};
use crate::store::{ConnectionType, DataAccess, RequestResult};

pub struct SqliteDataAccess {
    conn: Mutex<Connection>
}

impl SqliteDataAccess {
    pub fn open(file_path: &str) -> Self {
        let disk_db = Connection::open(file_path).unwrap();

        // Create a fresh in-memory database
        let mut mem_db = Connection::open_in_memory().unwrap();

        // Copy everything from disk into memory
        {
            let backup = rusqlite::backup::Backup::new(&disk_db, &mut mem_db).unwrap();
            backup.run_to_completion(100, std::time::Duration::from_millis(250), None).unwrap();
        }

        // And create master table if it doesn't exist yet
        let master_exists: bool = mem_db.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='master'",
            [],
            |row| row.get::<_, i64>(0),
        ).unwrap() > 0;

        if !master_exists {
            mem_db.execute_batch(
                "CREATE TABLE master (
                    ID      INTEGER PRIMARY KEY AUTOINCREMENT,
                    DB_NAME TEXT UNIQUE,
                    NAME    TEXT,
                    COLUMNS TEXT
                );"
            ).unwrap();
        }

        // Give back that new connection.
        SqliteDataAccess{
            conn:  Mutex::new(mem_db),
        }
    }

    fn create_table_sql(&self,db_name: &str, cols: &[CategoryColumn]) -> String {
        let col_defs: Vec<String> = cols
            .iter()
            .map(|c| format!("{} {}", c.internal_name, c.col_type.get_type(ConnectionType::SQLite)))
            .collect();

        let pk_names: Vec<String> = cols
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.internal_name.clone())
            .collect();

        format!(
            "CREATE TABLE {} ({}, PRIMARY KEY ({}));",
            db_name,
            col_defs.join(", "),
            pk_names.join(", ")
        )
    }

    fn to_sql_value(&self, cell: &CellValue) -> rusqlite::types::Value {
        match cell {
            CellValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
            CellValue::Boolean(b) => rusqlite::types::Value::Integer(*b as i64),
            CellValue::Integer(Some(n)) => rusqlite::types::Value::Integer(*n),
            CellValue::Integer(None) => rusqlite::types::Value::Null,
        }
    }

    fn build_where_clause(&self, cat: &Category, filters: &[SearchValue]) -> (String, Vec<rusqlite::types::Value>) {
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<rusqlite::types::Value> = Vec::new();

        for (col, search) in cat.columns.iter().zip(filters.iter()) {
            match search {
                SearchValue::Skip => {}

                SearchValue::Text(s) => {
                    conditions.push(format!("{} LIKE ? ESCAPE '\\'", col.internal_name));
                    params.push(rusqlite::types::Value::Text(format!("%{}%", s)));
                }

                SearchValue::Exact(n) => {
                    conditions.push(format!("{} = ?", col.internal_name));
                    params.push(rusqlite::types::Value::Integer(*n));
                }

                SearchValue::Comparison(op, Some(n)) => {
                    conditions.push(format!("{} {} ?", col.internal_name, op));
                    params.push(rusqlite::types::Value::Integer(*n));
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

    fn run_query(&self, cat: &Category, sql: &str, params: Vec<rusqlite::types::Value>) -> SqliteResult<Vec<Vec<CellValue>>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(sql)?;
        let columns = &cat.columns; // borrow for use in closure

        let rows: SqliteResult<Vec<Vec<CellValue>>> = stmt
            .query_map(rusqlite::params_from_iter(params), |row| {
                columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| self.read_from_row(col, row, i))
                    .collect()
            })?
            .collect();
        rows
    }

    fn read_from_row(&self, col: &CategoryColumn, row: &Row, index: usize) -> SqliteResult<CellValue> {
        match col.col_type {
            ColumnType::Text => {
                let s: String = row.get(index)?;
                Ok(CellValue::Text(s))
            }
            ColumnType::Date => {
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

    fn full_update_clause(&self, cols: &[CategoryColumn]) -> String {
        cols.iter()
            .map(|c| format!("{} = ?", c.internal_name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `pk1 = ? AND pk2 = ?` — used in WHERE clauses for primary key lookups
    fn primary_key_where(&self, cols: &[CategoryColumn]) -> String {
        cols
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| format!("{} = ?", c.internal_name))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    /// Indices of primary key columns within catgory
    fn primary_key_positions(&self, cols: &[CategoryColumn]) -> Vec<usize> {
        cols
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_primary_key)
            .map(|(i, _)| i)
            .collect()
    }

    fn columns_from_json(&self, json: &str) -> Result<Vec<CategoryColumn>, serde_json::Error> {
        serde_json::from_str(json)
    }
}

impl DataAccess for SqliteDataAccess {

    fn load_categories(&self) -> RequestResult<Vec<Category>> {
        let conn = self.conn.lock().unwrap();

        // Load every row from master
        let mut stmt = conn.prepare(
            "SELECT ID, DB_NAME, NAME, COLUMNS FROM master ORDER BY ID"
        )?;

        let categories: SqliteResult<Vec<Category>> = stmt
            .query_map([], |row| {
                let id: i64 = row.get(0)?;
                let internal_name: String = row.get(1)?;
                let display_name: String = row.get(2)?;
                let columns_json: String = row.get(3)?;

                // Parse columns from JSON (safe — no eval!)
                let columns = self.columns_from_json(&columns_json)
                    .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;

                Ok(Category { id, internal_name, display_name, columns })
            })?
            .collect();

        match categories {
            Ok(cat) => {Ok(cat)}
            Err(err) => {Err(Box::from(err))}
        }
    }

    fn save_category(&self, name: &str, cols: &[CategoryColumn]) -> RequestResult<(String, i64)> {
        let db_name = formatting::string_to_db_name(&name)?;
        let mut db = self.conn.lock().unwrap();
        let conn = db.transaction()?;
        let columns_json = serde_json::to_string(cols)?;

        conn.execute(
            "INSERT INTO master (DB_NAME, NAME, COLUMNS) VALUES (?1, ?2, ?3)",
            params![db_name, name, columns_json],
        )?;
        let id = conn.last_insert_rowid();

        // Create the user-data table
        conn.execute_batch(&self.create_table_sql(&db_name, cols))?;
        conn.commit()?;
        Ok((db_name, id))
    }

    fn delete_category(&self, id: i64, db_name: String) -> RequestResult<()> {
        let mut db = self.conn.lock().unwrap();
        let conn = db.transaction()?;
        conn.execute("DELETE FROM master WHERE ID = ?1", params![id])?;
        conn.execute_batch(&format!("DROP TABLE {};", db_name))?;
        conn.commit()?;
        Ok(())
    }

    fn load_entries(&self, cat: &Category, amount: u32, offset: u32, filters: &[SearchValue]) -> RequestResult<(Vec<Vec<CellValue>>, bool)> {
        let col_list = cat.internal_column_names();
        let (where_clause, params) = self.build_where_clause(cat, filters);

        // Fetch one extra row to detect whether a next page exists
        let sql = format!(
            "SELECT {} FROM {} {} ORDER BY {} LIMIT {} OFFSET {}",
            col_list.join(", "), cat.internal_name, where_clause, col_list.join(", "),
            amount + 1, offset
        );

        let mut rows = self.run_query(cat, &sql, params)?;
        let has_more = rows.len() > amount as usize;
        rows.truncate(amount as usize);
        Ok((rows, has_more))
    }

    fn insert_entry(&self, cat: &Category, row: &[CellValue]) -> RequestResult<()> {
        let col_list = cat.internal_column_names().join(", ");
        let placeholders = (1..=row.len())
            .map(|i| format!("?{}", i))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            cat.internal_name, col_list, placeholders
        );

        let mut db = self.conn.lock().unwrap();
        let conn = db.transaction()?;
        // We convert each CellValue to a SQL-compatible value using the column's
        // `to_sql_value` method, then collect into a Vec for binding.
        let params: Vec<rusqlite::types::Value> = row
            .iter()
            .map(| cell | self.to_sql_value(cell))
            .collect();

        conn.execute(&sql, rusqlite::params_from_iter(params))?;
        conn.commit()?;
        Ok(())
    }

    fn update_entry(&self, cat: &Category, old_row: &[CellValue], new_row: &[CellValue]) -> RequestResult<()> {
        let set_clause = self.full_update_clause(&cat.columns);
        let where_clause = self.primary_key_where(&cat.columns);
        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            cat.internal_name, set_clause, where_clause
        );

        let mut params: Vec<rusqlite::types::Value> = new_row
            .iter()
            .map(| cell | self.to_sql_value(cell))
            .collect();

        for pk_pos in self.primary_key_positions(&cat.columns) {
            params.push(self.to_sql_value(&old_row[pk_pos]));
        }

        let conn = self.conn.lock().unwrap();
        conn.execute(&sql, rusqlite::params_from_iter(params))?;
        Ok(())
    }

    fn delete_entry(&self, cat: &Category, row: &[CellValue]) -> RequestResult<()> {
        let where_clause = self.primary_key_where(&cat.columns);
        let sql = format!("DELETE FROM {} WHERE {}", cat.internal_name, where_clause);

        let params: Vec<rusqlite::types::Value> = self
            .primary_key_positions(&cat.columns)
            .iter()
            .map(|&i| self.to_sql_value(&row[i]))
            .collect();

        let conn = self.conn.lock().unwrap();
        conn.execute(&sql, rusqlite::params_from_iter(params))?;
        Ok(())
    }

    fn save_to_disk(&self, disk_path: &str) -> RequestResult<()> {
        let conn = self.conn.lock().unwrap();
        let mut disk_db = Connection::open(disk_path)?;
        let backup = rusqlite::backup::Backup::new(&*conn, &mut disk_db)?;
        backup.run_to_completion(100, std::time::Duration::from_millis(250), None)?;
        Ok(())
    }
}