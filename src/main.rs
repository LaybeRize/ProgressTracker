use crate::models::{Category, CategoryColumn, CellValue, ColumnType};
use crate::sqlite_backend::SqliteDataAccess;
use crate::store::DataAccess;

mod store;
mod formatting;
mod models;
mod sqlite_backend;

fn main() {
    // Initialise the DB at startup — everything else just calls `db()`
    let access = SqliteDataAccess::open("./data.sqlite");

    // Example: create a new category
    let mut movies = Category::new(
        "Movies",
        vec![
            CategoryColumn::new("Title", "Title", true, ColumnType::Text, 0, false, false),
            CategoryColumn::new("Year", "Year", true, ColumnType::Integer, 0, false, false),
            CategoryColumn::new("Watched", "Watched", false, ColumnType::Boolean, 0, false, false),
            CategoryColumn::new("Notes", "Notes", false, ColumnType::Text, 0, false, true),
        ],
    );
    movies.save(&access).expect("Failed to save category");

    // Insert a row
    movies.insert_entry(&access,&[
        CellValue::Text("The Grand Budapest Hotel".into()),
        CellValue::Integer(Some(2014)),
        CellValue::Boolean(true),
        CellValue::Text("Wonderful film".into()),
    ]).expect("Failed to insert row");

    // Query first page
    let (rows, has_more) = movies.load_entries(&access,10, 0, None).expect("Query failed");
    let display = movies.rows_to_display(&rows);
    for row in &display {
        println!("{:?}", row);
    }
    println!("Has more pages: {}", has_more);

    // Save back to disk on exit
    access.save_to_disk("./data.sqlite").expect("Failed to save to disk");
}