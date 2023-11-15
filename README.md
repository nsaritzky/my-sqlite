# Rust SQLite Database Parser

This project is a SQLite database parser written in Rust. It provides a set of functions to read and parse SQLite database files, and to perform various operations on the parsed data.

## Features

- Execute SELECT queries on single tables with a WHERE clause.
- Read SQLite database files and parse them into a structured format.
- Retrieve the root page of a table given its name.
- Retrieve the SQL statement used to create a table given its name.
- Retrieve all pages of a table given its root page.
- Retrieve all rows of a table given its page and column definitions.
- Search an index given its root page and a value to search for.
- Search a table for a row given its rowid.
- Match a row with its column names given a table name.

## Usage

The main entry point to the library is the `Database` struct. You can create a new `Database` instance by calling `Database::new(filename: &str)`, where `filename` is the path to the SQLite database file.

Once you have a `Database` instance, you can call various methods on it to perform operations on the database. For example, to get the root page of a table, you can call `Database::get_root_page(table_name: &'a str)`.

## Example

```shell
cargo run compaines.db "SELECT id, name from companies where country = 'france'"
```

`

```rust
let db = Database::new("path/to/database/file.sqlite")?;
let root_page = db.get_root_page("table_name")?;
```

## Dependencies

This project uses the following external crates:

- `anyhow`: For flexible error handling.
- nom : For parsing.
- `regex`: For regular expression support.
- `std`: For various standard library features.

## Limitations

This project is a work in progress and has some limitations:

- It does not support writing to SQLite database files, only reading.
- It does not support all SQLite features, only a subset of them.

## License

This project is licensed under the MIT License.
