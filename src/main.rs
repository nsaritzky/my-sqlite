mod parser;

use anyhow::{anyhow, bail, Result};
use parser::parse_cell;
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
            let header_slice = buf[0..100].to_vec();
            let (rest, header) =
                parser::parse_header(&header_slice).map_err(|e| anyhow::anyhow!("{e}"))?;
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
            let db = &buf[100..];
            let (rest, page_header) =
                parser::parse_page_header(db).map_err(|e| anyhow::anyhow!("{e}"))?;
            let (rest, cell_pointers) = parse_cell_pointers(rest, page_header.number_of_cells)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let mut res = Vec::new();
            for p in cell_pointers {
                let (rest, cell) = parse_cell(&db[p as usize - 100..], page_header.page_type)
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
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
