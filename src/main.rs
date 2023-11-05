mod parser;

use anyhow::{anyhow, bail, Result};
use parser::{parse_cell, parse_page};
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
            let (rest, cell_pointers) = parse_cell_pointers(rest, page_header.number_of_cells)
                .map_err(|e| anyhow::anyhow!("{e}"))?;

            println!("database page size: {}", header.page_size);
            println!("number of tables: {}", page_header.number_of_cells);
            println!("{:?}", cell_pointers);
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
            let mut pages = buf.chunks(header.page_size as usize);
            let (_rest, schema_page) = parse_page(
                &pages
                    .nth(0)
                    .ok_or(anyhow!("No first page in empty database"))?[100..],
                true,
            )
            .map_err(|e| anyhow!("{e}"))?;
            match parser::parse_select(s) {
                Ok((_rest, (names, table))) => {
                    let root_page = schema_page
                        .iter()
                        .find(|elem| {
                            if let parser::Data::Text(s) = &elem[2] {
                                s == table
                            } else {
                                false
                            }
                        })
                        .map(|v| &v[3]);
                    if let Some(&parser::Data::Integer(n)) = root_page {
                        let (_rest, page) = parse_page(
                            pages
                                .nth(n as usize - 2)
                                .ok_or(anyhow!("Page {n} not found"))?,
                            false,
                        )
                        .map_err(|e| anyhow!("{e}"))?;
                        if names == ["count(*)"] {
                            println!("{}", page.len());
                        } else {
                            let create_table = schema_page
                                .iter()
                                .find(|elem| {
                                    if let parser::Data::Text(s) = &elem[2] {
                                        s == table
                                    } else {
                                        false
                                    }
                                })
                                .map(|v| &v[4]);
                            if let Some(parser::Data::Text(s)) = create_table {
                                let (_rest, columns) =
                                    parser::parse_create_table(&s).map_err(|e| anyhow!("{e}"))?;
                                let indices: Result<Vec<usize>, _> = names
                                    .iter()
                                    .map(|name| {
                                        columns
                                            .iter()
                                            .position(|col| &col.name == name)
                                            .ok_or(anyhow!("Column {name} not found"))
                                    })
                                    .collect();
                                if let Ok(v) = indices {
                                    for row in page {
                                        println!(
                                            "{}",
                                            v.iter()
                                                .map(|i| row[*i].to_string())
                                                .collect::<Vec<_>>()
                                                .join("|")
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => bail!("{e}"),
            }

            // for (i, page) in buf.chunks_mut(header.page_size as usize).enumerate() {
            //     if i == 0 {
            //         let page = &page[100..];
            //         let (rest, page_header) =
            //             parser::parse_page_header(page).map_err(|e| anyhow!("{e}"))?;
            //         let (rest, cell_pointers) =
            //             parse_cell_pointers(rest, page_header.number_of_cells)
            //                 .map_err(|e| anyhow!("{e}"))?;
            //         for p in cell_pointers {
            //             let (_, cell) =
            //                 parse_cell(&page[p as usize - 100..], page_header.page_type)
            //                     .map_err(|e| anyhow!("{e}"))?;
            //             println!("{:?}", cell);
            //         }
            //     } else {
            //         let (rest, page_header) =
            //             parser::parse_page_header(page).map_err(|e| anyhow!("{e}"))?;
            //         let (rest, cell_pointers) =
            //             parse_cell_pointers(rest, page_header.number_of_cells)
            //                 .map_err(|e| anyhow!("{e}"))?;
            //         for p in cell_pointers {
            //             let (_rest, cell) = parse_cell(&page[p as usize..], page_header.page_type)
            //                 .map_err(|e| anyhow!("{e}"))?;
            //             println!("{:?}", cell);
            //         }
            //     }
            // }
        }
    }

    Ok(())
}
