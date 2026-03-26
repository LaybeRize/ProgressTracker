use crate::categories::{init_db, save_db_to_disk, Category, CategoryColumn, CellValue, ColumnType};

mod categories;

fn main() {
    // Initialise the DB at startup — everything else just calls `db()`
    init_db("./data.sqlite").expect("Failed to open database");

    // Load existing categories
    let categories = Category::load_all().expect("Failed to load categories");
    println!("Loaded {} categories", categories.len());

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
    movies.save().expect("Failed to save category");

    // Insert a row
    movies.insert_row(&[
        CellValue::Text("The Grand Budapest Hotel".into()),
        CellValue::Integer(Some(2014)),
        CellValue::Boolean(true),
        CellValue::Text("Wonderful film".into()),
    ]).expect("Failed to insert row");

    // Query first page
    let (rows, has_more) = movies.query_page(0, 10, None).expect("Query failed");
    let display = movies.rows_to_display(&rows);
    for row in &display {
        println!("{:?}", row);
    }
    println!("Has more pages: {}", has_more);

    // Save back to disk on exit
    save_db_to_disk("./data.sqlite").expect("Failed to save to disk");
}