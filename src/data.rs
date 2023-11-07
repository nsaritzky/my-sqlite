use crate::parser;
use crate::parser::{Data, Page, PageValue};

use anyhow::{anyhow, bail};
use std::collections::HashMap;

const ROOT_PAGE_INDEX: usize = 3;
const CREATE_TABLE_INDEX: usize = 4;

// Given the schema page and a table name, return the root page of the table.
// The option fails if the table does not exist. The Result vails if the
// schema is not a leaf page.
fn get_schema_value_by_index<'a>(
    table_name: &'a str,
    schema: &'a [PageValue],
    i: usize,
) -> Result<Option<&'a Data>, anyhow::Error> {
    schema
        .iter()
        .find(|elem| {
            if let PageValue::Data(vec) = elem {
                let s = if let Data::Text(s) = &vec[2] {
                    Some(s)
                } else {
                    None
                };
                s.map(|s| s.as_str()) == Some(table_name)
            } else {
                false
            }
        })
        .map(|v| -> Result<&Data, _> {
            if let PageValue::Data(vec) = v {
                Ok(&vec[i])
            } else {
                Err(anyhow!("Not a data page"))
            }
        })
        .transpose()
}

pub fn get_root_page<'a>(
    table_name: &'a str,
    schema: &'a [PageValue],
) -> Result<Option<&'a Data>, anyhow::Error> {
    get_schema_value_by_index(table_name, schema, ROOT_PAGE_INDEX)
}

pub fn get_create_table<'a>(
    table_name: &'a str,
    schema: &'a [PageValue],
) -> Result<Option<&'a Data>, anyhow::Error> {
    get_schema_value_by_index(table_name, schema, CREATE_TABLE_INDEX)
}

pub fn get_pages(root_index: usize, book: &Vec<&[u8]>) -> Result<Vec<usize>, anyhow::Error> {
    let (_, root) = parser::parse_page(book[root_index - 1], false).map_err(|e| anyhow!("{e}"))?;
    // If the root page is a leaf page, it's the only page for the table.
    if root.header.page_type == parser::PageType::LeafTable {
        Ok(vec![root_index])
    } else if root.header.page_type == parser::PageType::InteriorTable {
        // If the root page is an interior page, it contains the page numbers
        // of the child pages.
        let mut child_pages = Vec::new();
        child_pages.push(root.header.right_most_pointer.ok_or(anyhow!(
            "Root page is interior but has no right most pointer"
        ))? as usize);
        for value in root.values {
            if let parser::PageValue::InteriorCell {
                left_child_page,
                row_id: _,
            } = value
            {
                child_pages.push(left_child_page as usize);
            }
        }
        let pages = child_pages
            .iter()
            .map(|p| get_pages(*p, book))
            .collect::<Result<Vec<_>, _>>()?
            .concat();
        Ok(pages)
    } else {
        Err(anyhow!("Root page is not a table"))
    }
}

pub fn get_rows<'a>(
    page: &'a Page,
    columns: &'a [parser::ColumnDef],
    where_: Option<parser::WhereClause>,
) -> Result<Vec<HashMap<String, parser::Data>>, anyhow::Error> {
    let mut rows = Vec::<HashMap<String, parser::Data>>::new();
    for val in &page.values {
        if let parser::PageValue::Data(vec) = val {
            let mut map = HashMap::new();
            for (i, col) in columns.iter().enumerate() {
                // If the column is the integer primary key, then it must be null,
                // and we substitute the row id.
                if col.ipk {
                    if let Data::Null(n) = vec[i] {
                        map.insert(col.name.clone(), Data::Integer(n));
                    } else {
                        bail!("IPK is not null")
                    }
                } else {
                    map.insert(col.name.clone(), vec[i].clone());
                }
            }
            rows.push(map);
        }
    }
    let rows = if let Some(where_) = where_ {
        rows.into_iter()
            .filter(|row| {
                let where_value = row.get(&where_.column);
                if let Some(where_value) = where_value {
                    where_value == &Data::Text(where_.value.clone())
                } else {
                    false
                }
            })
            .collect::<Vec<_>>()
    } else {
        rows
    };
    Ok(rows)
}
