mod data;
mod parser;

use anyhow::{anyhow, bail, Result};
use data::{get_create_table, get_root_page, get_rows};
use parser::{parse_cell, parse_page, Data, PageValue};
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;

use crate::parser::parse_cell_pointers;

fn get_pages() {}

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
            let (rest, cell_pointers) = parse_cell_pointers(rest, page_header.number_of_cells)
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            println!("database page size: {}", header.page_size);
            println!("number of tables: {}", page_header.number_of_cells);
            println!("{:?}", cell_pointers);
        }
        "pages" => {
            let mut file = File::open(&args[1])?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let (rest, header) = parser::parse_header(&buf).map_err(|e| anyhow::anyhow!("{e}"))?;
            let raw_pages: Vec<&[u8]> = buf.chunks(header.page_size as usize).collect();
            let (
                rest,
                parser::Page {
                    header: schema_header,
                    values: schema,
                },
            ) = parser::parse_page(raw_pages[0], true).map_err(|e| anyhow!("{e}"))?;
            if let parser::Data::Integer(root_page_index) =
                get_root_page("superheroes", &schema)?.unwrap()
            {
                println!("root page: {root_page_index}");
                let pages = data::get_pages(*root_page_index as usize, &raw_pages)?;
                println!("{:?}", pages);
            }
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
            let mut file = File::open(&args[1])?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let header_slice = buf[0..100].to_vec();
            let (_rest, header) =
                parser::parse_header(&header_slice).map_err(|e| anyhow::anyhow!("{e}"))?;
            let raw_pages = buf.chunks(header.page_size as usize).collect::<Vec<_>>();
            let (
                _rest,
                parser::Page {
                    header: page_header,
                    values: schema_page,
                },
            ) = parse_page(raw_pages[0], true).map_err(|e| anyhow!("{e}"))?;
            match parser::parse_select(s) {
                Ok((_rest, (names, table, where_))) => {
                    let root_page = get_root_page(table, &schema_page)?;
                    if let Some(&Data::Integer(n)) = root_page {
                        let leaf_pages = data::get_pages(n as usize, &raw_pages)?;
                        let mut values = Vec::new();
                        for leaf in &leaf_pages {
                            let (_rest, page) = parse_page(raw_pages[leaf - 1], false)
                                .map_err(|e| anyhow!("{e}"))?;
                            values.extend(page.values);
                        }
                        if names == ["count(*)"] {
                            println!("{}", values.len());
                        } else {
                            let create_table = get_create_table(table, &schema_page)?;
                            if let Some(Data::Text(s)) = create_table {
                                println!("{}", s);
                                let (_rest, columns) =
                                    parser::parse_create_table(s).map_err(|e| anyhow!("{e}"))?;
                                let rows = leaf_pages
                                    .iter()
                                    .map(|i| {
                                        let (_rest, page) = parse_page(raw_pages[i - 1], false)
                                            .map_err(|e| anyhow!("{e}"))?;
                                        get_rows(&page, &columns, where_.clone())
                                    })
                                    .collect::<Result<Vec<_>, _>>()?
                                    .concat();
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
