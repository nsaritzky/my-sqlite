use crate::parser;
use crate::parser::{Data, Page, PageValue};

use anyhow::{anyhow, bail};
use regex::Regex;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

const ROOT_PAGE_INDEX: usize = 3;
const CREATE_TABLE_INDEX: usize = 4;

// Given the schema page and a table name, return the root page of the table.
// The option fails if the table does not exist. The Result vails if the
// schema is not a leaf page.
fn get_schema_value_by_index<'a>(
    table_name: &'a str,
    schema: &'a [PageValue],
    i: usize,
) -> Result<Option<Data>, anyhow::Error> {
    schema
        .iter()
        .find(|elem| {
            if let PageValue::LeafTableCell { payload: vec, .. } = elem {
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
        .map(|v| -> Result<Data, _> {
            if let PageValue::LeafTableCell { payload: vec, .. } = v {
                Ok(vec[i].clone())
            } else {
                Err(anyhow!("Not a data page"))
            }
        })
        .transpose()
}

pub fn get_root_page<'a>(
    table_name: &'a str,
    schema: &'a [PageValue],
) -> Result<Option<Data>, anyhow::Error> {
    get_schema_value_by_index(table_name, schema, ROOT_PAGE_INDEX)
}

pub fn get_create_table<'a>(
    table_name: &'a str,
    schema: &'a [PageValue],
) -> Result<Option<Data>, anyhow::Error> {
    get_schema_value_by_index(table_name, schema, CREATE_TABLE_INDEX)
}

pub fn get_pages(root_index: usize, db: &Database) -> Result<Vec<usize>, anyhow::Error> {
    let (_, root) = parser::parse_page(&db.read_page_at(root_index as u64)?, false)
        .map_err(|e| anyhow!("{e}"))?;
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
            if let parser::PageValue::InteriorTableCell {
                left_child_page,
                rowid: _,
            } = value
            {
                child_pages.push(left_child_page as usize);
            }
        }
        let pages = child_pages
            .iter()
            .map(|p| get_pages(*p, db))
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
        if let parser::PageValue::LeafTableCell {
            payload: vec,
            rowid,
        } = val
        {
            let mut map = HashMap::new();
            for (i, col) in columns.iter().enumerate() {
                // If the column is the integer primary key, then it must be null,
                // and we substitute the row id.
                if col.ipk {
                    map.insert(col.name.clone(), Data::Integer(*rowid as i64));
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

pub fn search_index(
    root_index: usize,
    value: Data,
    db: &Database,
) -> Result<Vec<i64>, anyhow::Error> {
    let (_, root) = parser::parse_page(&db.read_page_at(root_index as u64)?, false)
        .map_err(|e| anyhow!("{e}"))?;
    let m = root.values.partition_point(|v| {
        if let PageValue::InteriorIndexCell { payload, .. } | PageValue::LeafIndexCell { payload } =
            v
        {
            payload[0] < value
        } else {
            panic!("Not an index page")
        }
    });
    let n = root.values.partition_point(|v| {
        if let PageValue::InteriorIndexCell { payload, .. } | PageValue::LeafIndexCell { payload } =
            v
        {
            payload[0] <= value
        } else {
            panic!("Not an index page")
        }
    });
    let mut results = Vec::new();
    for i in m..=(n.min(root.values.len() - 1)) {
        match &root.values[i] {
            PageValue::InteriorIndexCell {
                left_child_page,
                payload,
            } => {
                if let Data::Integer(k) = payload[1] {
                    results.push(k);
                }
                results.extend(search_index(*left_child_page as usize, value.clone(), db)?);
            }
            PageValue::LeafIndexCell { payload } => {
                if let Data::Integer(k) = payload[1] {
                    // if i == m {
                    results.push(k);
                    // }
                }
            }
            _ => panic!("Not an index page"),
        }
    }
    if m == root.values.len() {
        if let Some(right_most_pointer) = root.header.right_most_pointer {
            results.extend(search_index(right_most_pointer as usize, value, db)?);
        } else if let Some(vec) = root.values[m - 1].get_payload() {
            if let Data::Integer(k) = vec[1] {
                results.push(k);
            }
        }
    }
    Ok(results)
}

pub fn search_by_rowid(
    db: &Database,
    root_page_number: u64,
    rowid_to_find: i64,
) -> Result<PageValue, anyhow::Error> {
    let root = db.read_page_at(root_page_number)?;
    let (_, root) = parser::parse_page(&root, false).map_err(|e| anyhow!("{e}"))?;
    let i = root.values.binary_search_by_key(&rowid_to_find, |v| {
        if let PageValue::LeafTableCell { rowid, .. } | PageValue::InteriorTableCell { rowid, .. } =
            v
        {
            *rowid as i64
        } else {
            panic!("Not a table age")
        }
    });
    if let Ok(i) = i {
        // If the page is a table leaf and the rowid is present, return the data
        if let PageValue::LeafTableCell { .. } = &root.values[i] {
            return Ok(root.values[i].clone());
        // If the page is a table interior and the rowid is present, that means
        // the rowid is not in the table.
        } else if let PageValue::InteriorTableCell { .. } = &root.values[i] {
            bail!("Rowid not in table");
        // This function should not be called on index pages
        } else {
            bail!("Not a table page")
        }
        // When the rowid is not present, the binary search returns the index
        // where the rowid should be inserted.
    } else if let Err(i) = i {
        // If the insertion index is equal to the length of the page, then the
        // rowid is greater than all the rowids in the page. Then the rightmost
        // pointer is followed.
        if i == root.values.len() {
            if let Some(right_most_pointer) = root.header.right_most_pointer {
                return search_by_rowid(db, right_most_pointer as u64, rowid_to_find);
            }
        }
        if let PageValue::InteriorTableCell {
            left_child_page,
            rowid: _,
        } = &root.values[i]
        {
            return search_by_rowid(db, *left_child_page as u64, rowid_to_find);
        } else {
            bail!("Not a table page")
        }
    } else {
        bail!("Not a table page")
    }
}

pub struct Database {
    filename: String,
    cursor: RefCell<u64>,
    page_size: u64,
    schema_page: Vec<PageValue>,
}

impl Database {
    pub fn new(filename: &str) -> Result<Self, anyhow::Error> {
        let mut raw_header = [0; 100];
        let mut file = File::open(filename)?;
        file.read_exact(&mut raw_header)?;
        let (_, header) = parser::parse_header(&raw_header).map_err(|e| anyhow::anyhow!("{e}"))?;
        let mut buf = vec![0; header.page_size as usize];
        file.rewind()?;
        file.read_exact(&mut buf)?;
        let (_, schema_page) =
            parser::parse_page(&buf, true).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(Self {
            filename: filename.to_string(),
            cursor: RefCell::new(0),
            page_size: header.page_size as u64,
            schema_page: schema_page.values,
        })
    }

    fn read_page(&self, file: &mut File) -> Result<Vec<u8>, anyhow::Error> {
        let mut buf = vec![0; self.page_size as usize];
        file.read_exact(&mut buf)?;
        self.cursor.replace_with(|&mut c| c + self.page_size);
        Ok(buf)
    }

    pub fn read_page_at(&self, page_number: u64) -> Result<Vec<u8>, anyhow::Error> {
        let mut file = File::open(&self.filename)?;
        file.seek(SeekFrom::Start((page_number - 1) * self.page_size))?;
        self.read_page(&mut file)
    }

    fn _read_pages(&self, page_numbers: &[u64]) -> Result<Vec<Vec<u8>>, anyhow::Error> {
        let mut pages = Vec::new();
        for page_number in page_numbers {
            pages.push(self.read_page_at(*page_number)?);
        }
        Ok(pages)
    }

    pub fn get_root_page<'a>(&'a self, table_name: &'a str) -> Result<Option<Data>, anyhow::Error> {
        get_root_page(table_name, &self.schema_page)
    }

    pub fn get_create_table<'a>(
        &'a self,
        table_name: &'a str,
    ) -> Result<Option<Data>, anyhow::Error> {
        get_create_table(table_name, &self.schema_page)
    }

    pub fn find_index_root(&self, column: &str, table: &str) -> Option<usize> {
        // TODO: This is a hack. We should parse the create table statement.
        let column_regex = Regex::new(&format!("(?i)on {table}\\s*\\({column}\\)")).unwrap();
        let index = self.schema_page.iter().find(|elem| {
            if let PageValue::LeafTableCell { payload: vec, .. } = elem {
                if let Data::Text(s) = &vec[0] {
                    if s == "index" {
                        if let Data::Text(sql) = &vec[4] {
                            column_regex.is_match(sql)
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        });
        index.map(|idx| {
            if let PageValue::LeafTableCell { payload: vec, .. } = idx {
                if let Data::Integer(n) = &vec[3] {
                    *n as usize
                } else {
                    panic!("Index is not an integer")
                }
            } else {
                panic!("Index is not a data page")
            }
        })
    }

    pub fn find_by_index(
        &self,
        column: &str,
        table: &str,
        value: Data,
    ) -> Result<Vec<PageValue>, anyhow::Error> {
        if let Some(index_root) = self.find_index_root(column, table) {
            let indices = search_index(index_root, value, self)?;
            let mut results = Vec::new();
            for row in indices {
                if let Some(Data::Integer(n)) = self.get_root_page(table)? {
                    results.push(search_by_rowid(self, n as u64, row)?);
                }
            }
            Ok(results)
        } else {
            bail!("No index found for {column} in table {table}")
        }
    }

    pub fn match_row_with_column_names(
        &self,
        row: &PageValue,
        table_name: &str,
    ) -> Result<HashMap<String, Data>, anyhow::Error> {
        let mut map = HashMap::new();
        let create_table = self.get_create_table(table_name)?;
        if let Some(Data::Text(sql)) = create_table {
            let (_, columns) = parser::parse_create_table(&sql).map_err(|e| anyhow!("{e}"))?;
            if let PageValue::LeafTableCell { payload, rowid } = row {
                map.insert("rowid".to_string(), Data::Integer(*rowid));
                for (i, col) in columns.iter().enumerate() {
                    // If the column is an integer primary key, insert rowid for value instead
                    if col.ipk {
                        map.insert(col.name.clone(), Data::Integer(*rowid));
                    } else {
                        map.insert(col.name.clone(), payload[i].clone());
                    }
                }
            }
        }
        Ok(map)
    }
}
