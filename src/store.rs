use crate::models::{Category, CategoryColumn, CellValue, SearchValue};


pub type RequestResult<T> = Result<T, Box<dyn std::error::Error>>;

pub enum ConnectionType {
    SQLite,
}

pub trait DataAccess {

    fn load_categories(&self) -> RequestResult<Vec<Category>>;

    fn save_category(&self, name: &str, cols: &[CategoryColumn]) -> RequestResult<(String, i64)>;

    fn delete_category(&self, id: i64, name: String) -> RequestResult<()>;

    fn load_entries(&self, cat: &Category, amount: u32, offset: u32, filters: &[SearchValue]) -> RequestResult<(Vec<Vec<CellValue>>, bool)>;

    fn insert_entry(&self, cat: &Category, row: &[CellValue]) -> RequestResult<()>;

    fn update_entry(&self, cat: &Category, old_row: &[CellValue], new_row: &[CellValue]) -> RequestResult<()>;

    fn delete_entry(&self, cat: &Category, row: &[CellValue]) -> RequestResult<()>;

    fn save_to_disk(&self, disk_path: &str) -> RequestResult<()>;
}

