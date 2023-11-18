mod data;
mod parser;

use anyhow::{anyhow, bail, Result};
use data::get_rows;
use data::Database;
use parser::{parse_cell, parse_page, Data};

use std::fs::File;
use std::io::prelude::*;

use crate::parser::parse_cell_pointers;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let (rest, header) = parser::parse_header(&buf).map_err(|e| anyhow::anyhow!("{e}"))?;
            let (rest, page_header) =
                parser::parse_page_header(rest).map_err(|e| anyhow::anyhow!("{e}"))?;
            let (_rest, _cell_pointers) = parse_cell_pointers(rest, page_header.number_of_cells)
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            println!("database page size: {}", header.page_size);
            println!("number of tables: {}", page_header.number_of_cells);
        }

        ".tables" => {
            let mut file = File::open(&args[1])?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let db = &buf[100..];
            let (rest, page_header) =
                parser::parse_page_header(db).map_err(|e| anyhow::anyhow!("{e}"))?;
            let (_rest, cell_pointers) = parse_cell_pointers(rest, page_header.number_of_cells)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut res = Vec::new();
            for p in cell_pointers {
                let (_rest, cell) = parse_cell(&db[p as usize - 100..], page_header.page_type)
                    .map_err(|e| anyhow!("{e}"))?;
                match cell {
                    parser::Cell::TableLeaf(content) => {
                        if let parser::Data::Text(s) = content.payload[2].clone() {
                            res.push(s);
                        }
                    }
                    _ => bail!("Not a table leaf"),
                }
            }
            for r in res {
                print!("{r} ");
            }
        }
        s => {
            let db = Database::new(&args[1])?;
            match parser::parse_select(s) {
                Ok((_rest, (names, table, where_))) => {
                    let root_page = db.get_root_page(table)?;
                    if let Some(Data::Integer(n)) = root_page {
                        let leaf_pages = data::get_pages(n as usize, &db)?;
                        let mut values = Vec::new();
                        for leaf in &leaf_pages {
                            let (_rest, page) = parse_page(&db.read_page_at(*leaf as u64)?, false)
                                .map_err(|e| anyhow!("{e}"))?;
                            values.extend(page.values);
                        }
                        if names == ["count(*)"] {
                            println!("{}", values.len());
                        // If there is a where clause reference an indexed column, use the index
                        } else if where_.is_some()
                            && db
                                .find_index_root(&where_.as_ref().unwrap().column, table) //Unwrap is safe due to is_some check
                                .is_some()
                        {
                            if let Some(where_) = where_ {
                                // I've only implemented index search for one column,
                                // and only for text values
                                let rows = db.find_by_index(
                                    &where_.column,
                                    table,
                                    Data::Text(where_.value.clone()),
                                )?;
                                let create_table = db.get_create_table(table)?;
                                // Use the create-table statement to get the column names
                                if let Some(Data::Text(_s)) = create_table {
                                    let row_maps = rows
                                        .iter()
                                        .map(|row| db.match_row_with_column_names(row, table))
                                        .collect::<Result<Vec<_>, _>>()?;
                                    for map in row_maps.iter().filter(|m| {
                                        m.get(&where_.column)
                                            == Some(&Data::Text(where_.value.clone()))
                                    }) {
                                        let mut row_values = Vec::new();
                                        for name in &names {
                                            if let Some(value) = map.get(*name) {
                                                row_values.push(value);
                                            }
                                        }
                                        println!(
                                            "{}",
                                            row_values
                                                .iter()
                                                .map(|v| v.to_string())
                                                .collect::<Vec<_>>()
                                                .join("|")
                                        );
                                    }
                                }
                            }
                        // Handle the general case where there is no index
                        } else {
                            let create_table = db.get_create_table(table)?;
                            if let Some(Data::Text(s)) = create_table {
                                let (_rest, columns) =
                                    parser::parse_create_table(&s).map_err(|e| anyhow!("{e}"))?;
                                let rows = leaf_pages
                                    .iter()
                                    .flat_map(|i| {
                                        let (_rest, page) =
                                            parse_page(&db.read_page_at(*i as u64)?, false)
                                                .map_err(|e| anyhow!("{e}"))?;
                                        get_rows(&page, &columns, where_.clone())
                                    })
                                    .flatten();
                                for row in rows {
                                    let mut row_values = Vec::new();
                                    for name in &names {
                                        if let Some(value) = row.get(*name) {
                                            row_values.push(value);
                                        }
                                    }
                                    println!(
                                        "{}",
                                        row_values
                                            .iter()
                                            .map(|v| v.to_string())
                                            .collect::<Vec<_>>()
                                            .join("|")
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) => bail!("{e}"),
            }
        }
    }

    Ok(())
}
