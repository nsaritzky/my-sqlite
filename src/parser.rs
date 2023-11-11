use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take, take_while1},
    character::complete::{anychar, char, multispace0},
    combinator::{cond, consumed, map, opt},
    error::ParseError,
    multi::{count, many0, many_till, separated_list0, separated_list1},
    number::complete::{
        be_f64, be_i16, be_i24, be_i32, be_i64, be_i8, be_u16, be_u24, be_u32, be_u8,
    },
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};
use regex::Regex;

const PAGE_TYPE_INTERIOR_INDEX: u8 = 2;
const PAGE_TYPE_INTERIOR_TABLE: u8 = 5;
const PAGE_TYPE_LEAF_INDEX: u8 = 10;
const PAGE_TYPE_LEAF_TABLE: u8 = 13;

#[derive(Debug, Clone)]
pub struct Header {
    pub magic: String,
    pub page_size: u16,
    pub file_format_write_version: u8,
    pub file_format_read_version: u8,
    pub bytes_reserved_at_end_of_each_page: u8,
    pub max_embedded_payload_fraction: u8,
    pub min_embedded_payload_fraction: u8,
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    pub database_size_in_pages: u32,
    pub first_freelist_page: u32,
    pub number_of_freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format_number: u32,
    pub default_page_cache_size: u32,
    pub largest_root_btree_page_number: u32,
    pub text_encoding: u32,
    pub user_version: u32,
    pub incremental_vacuum_mode: u32,
    pub application_id: u32,
    pub version_valid_for: u32,
    pub sqlite_version_number: u32,
}

type ParseResult<'a, T, I = &'a [u8], E = nom::error::Error<I>> = IResult<I, T, E>;

pub fn parse_header(input: &[u8]) -> ParseResult<Header> {
    let mut first_parser = tuple((
        terminated(tag("SQLite format 3"), tag("\0")),
        be_u16,
        be_u8,
        be_u8,
        be_u8,
        be_u8,
        be_u8,
        be_u8,
        be_u32,
        be_u32,
        be_u32,
        be_u32,
    ));
    let mut second_parser = tuple((
        be_u32,
        be_u32,
        be_u32,
        be_u32,
        be_u32,
        be_u32,
        be_u32,
        be_u32,
        take(20usize),
        be_u32,
        be_u32,
    ));
    let (rest, t1) = first_parser(input)?;
    let (rest, t2) = second_parser(rest)?;
    Ok((
        rest,
        Header {
            magic: String::from_utf8(t1.0.to_vec()).unwrap(),
            page_size: t1.1,
            file_format_write_version: t1.2,
            file_format_read_version: t1.3,
            bytes_reserved_at_end_of_each_page: t1.4,
            max_embedded_payload_fraction: t1.5,
            min_embedded_payload_fraction: t1.6,
            leaf_payload_fraction: t1.7,
            file_change_counter: t1.8,
            database_size_in_pages: t1.9,
            first_freelist_page: t1.10,
            number_of_freelist_pages: t1.11,
            schema_cookie: t2.0,
            schema_format_number: t2.1,
            default_page_cache_size: t2.2,
            largest_root_btree_page_number: t2.3,
            text_encoding: t2.4,
            user_version: t2.5,
            incremental_vacuum_mode: t2.6,
            application_id: t2.7,
            version_valid_for: t2.9,
            sqlite_version_number: t2.10,
        },
    ))
}

#[derive(Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    pub first_freeblock_offset: u16,
    pub number_of_cells: u16,
    pub cell_content_area_offset: u16,
    pub fragmented_free_bytes: u8,
    pub right_most_pointer: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

fn parse_page_type(input: &[u8]) -> ParseResult<PageType> {
    let (rest, x) = be_u8(input)?;
    match x {
        PAGE_TYPE_INTERIOR_INDEX => Ok((rest, PageType::InteriorIndex)),
        PAGE_TYPE_INTERIOR_TABLE => Ok((rest, PageType::InteriorTable)),
        PAGE_TYPE_LEAF_INDEX => Ok((rest, PageType::LeafIndex)),
        PAGE_TYPE_LEAF_TABLE => Ok((rest, PageType::LeafTable)),
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Fail,
        ))),
    }
}

pub fn parse_page_header(input: &[u8]) -> ParseResult<PageHeader> {
    let (_rest, page_type) = parse_page_type(input)?;
    match page_type {
        PageType::InteriorIndex | PageType::InteriorTable => {
            let (rest, t) = tuple((parse_page_type, be_u16, be_u16, be_u16, be_u8, be_u32))(input)?;
            Ok((
                rest,
                PageHeader {
                    page_type: t.0,
                    first_freeblock_offset: t.1,
                    number_of_cells: t.2,
                    cell_content_area_offset: t.3,
                    fragmented_free_bytes: t.4,
                    right_most_pointer: Some(t.5),
                },
            ))
        }
        _ => {
            let (rest, t) = tuple((parse_page_type, be_u16, be_u16, be_u16, be_u8))(input)?;

            Ok((
                rest,
                PageHeader {
                    page_type: t.0,
                    first_freeblock_offset: t.1,
                    number_of_cells: t.2,
                    cell_content_area_offset: t.3,
                    fragmented_free_bytes: t.4,
                    right_most_pointer: None,
                },
            ))
        }
    }
}

pub fn parse_cell_pointers(input: &[u8], number_of_cells: u16) -> ParseResult<Vec<u16>> {
    let parser = take(number_of_cells as usize * 2);
    let (rest, cells) = parser(input)?;
    let mut res = Vec::<u16>::new();
    for i in 0..number_of_cells {
        res.push(u16::from_be_bytes([
            cells[i as usize * 2],
            cells[i as usize * 2 + 1],
        ]));
    }
    Ok((rest, res))
}

// /// Parse a SQLite variable-length integer (varint).
// fn varint(input: &[u8]) -> IResult<&[u8], u64> {
//     // Each byte can contribute to at most 7 bits of the data.
//     // If the high bit of a byte is set, it means that the next byte is also part of the varint.
//     let mut value: u64 = 0;
//     let mut bytes_read = 0;

//     let (mut input, _) = take(1usize)(input)?;
//     for byte in input.iter().take(9) {
//         // Varints are at most 9 bytes long.
//         // Add the low 7 bits to value.
//         value |= (byte & 0x7F) as u64;
//         bytes_read += 1;

//         if byte & 0x80 == 0 {
//             // If the high bit is not set, this is the last byte.
//             break;
//         }

//         if bytes_read != 9 {
//             // Shift to make room for the next 7 bits, unless this is the last byte.
//             value <<= 7;
//         }

//         // Move to the next byte.
//         let (i, _) = take(1usize)(input)?;
//         input = i;
//     }

//     Ok((input, value))
// }

fn varint(input: &[u8]) -> ParseResult<i64> {
    let mut res = 0;
    let mut index = 0;
    while index < 9 {
        let (rest, byte) = be_u8(&input[index as usize..])?;
        let byte = i64::from_be_bytes([0, 0, 0, 0, 0, 0, 0, byte]);
        let add = byte & 0b0111_1111_i64;
        res = (res << 7) | add;
        if byte & 0b1000_0000 == 0 {
            return Ok((rest, res));
        }
        index += 1;
    }
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Fail,
    )))
}

#[derive(Debug)]
pub struct TableLeafCell {
    pub row_id: i64,
    pub payload: Vec<Data>,
    pub overflow_page: Option<u32>,
}

#[derive(Debug)]
pub struct TableInteriorCell {
    pub left_child_page: u32,
    pub row_id: i64,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexLeafCell {
    payload: Vec<Data>,
    overflow_page: Option<u32>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexInteriorCell {
    left_child_page: u32,
    payload: Vec<Data>,
    overflow_page: Option<u32>,
}

#[derive(Debug)]
pub enum Cell {
    TableLeaf(TableLeafCell),
    TableInterior(TableInteriorCell),
    IndexLeaf(IndexLeafCell),
    IndexInterior(IndexInteriorCell),
}

fn does_overflow(_u: usize, _p: usize) -> bool {
    false
}

pub fn parse_cell(input: &[u8], page_type: PageType) -> ParseResult<Cell> {
    match page_type {
        PageType::LeafTable => {
            let (rest, payload_size) = varint(input)?;
            let (rest, row_id) = varint(rest)?;
            let (rest, payload) = take(payload_size as usize)(rest)?;
            let (rest, overflow_page) = cond(does_overflow(0, 0), be_u32)(rest)?;
            Ok((
                rest,
                Cell::TableLeaf(TableLeafCell {
                    row_id,
                    payload: parse_record(payload)?.1,
                    overflow_page,
                }),
            ))
        }
        PageType::InteriorTable => {
            let (rest, left_child_page) = be_u32(input)?;
            let (rest, row_id) = varint(rest)?;
            Ok((
                rest,
                Cell::TableInterior(TableInteriorCell {
                    left_child_page,
                    row_id,
                }),
            ))
        }
        PageType::LeafIndex => {
            let (rest, payload_size) = varint(input)?;
            let (rest, payload) = take(payload_size as usize)(rest)?;
            let (_rest, overflow_page) = cond(does_overflow(0, 0), be_u32)(rest)?;

            let (rest, record) = parse_record(payload)?;
            Ok((
                rest,
                Cell::IndexLeaf(IndexLeafCell {
                    payload: record,
                    overflow_page,
                }),
            ))
        }
        PageType::InteriorIndex => {
            let (rest, left_child_page) = be_u32(input)?;
            let (rest, payload_size) = varint(rest)?;
            let (rest, payload) = take(payload_size as usize)(rest)?;
            let (_rest, overflow_page) = cond(does_overflow(0, 0), be_u32)(rest)?;

            let (rest, record) = parse_record(payload)?;

            Ok((
                rest,
                Cell::IndexInterior(IndexInteriorCell {
                    left_child_page,
                    // payload: all_consuming(parse_record)(payload)?.1,
                    payload: record,
                    overflow_page,
                }),
            ))
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Data {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl std::fmt::Display for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Data::Null => write!(f, "NULL"),
            Data::Integer(x) => write!(f, "{x}"),
            Data::Float(x) => write!(f, "{x}"),
            Data::Text(x) => write!(f, "{x}"),
            Data::Blob(x) => write!(f, "{x:?}"),
        }
    }
}

// #[derive(Debug)]
// pub struct SqliteSchemaRow {
//     pub type_: String,
//     pub name: String,
//     pub tbl_name: String,
//     pub rootpage: u32,
//     pub sql: String,
// }

// pub fn parse_schema(input: &[u8]) -> ParseResult<Vec<SqliteSchemaRow>> {
//     let (rest, page_header) = parse_page_header(input)?;
//     let (rest, cell_pointers) = parse_cell_pointers(rest, page_header.number_of_cells)?;
//     let res = Vec::new();
//     for p in cell_pointers {
//         let (rest, cell) = parse_cell(&input[p as usize..], page_header.page_type)?;
//         match cell {
//             Cell::TableLeaf(content) => {

//             }
//             _ => bail!("Not a table leaf"),
//         }
//     }
// }

pub fn parse_record(input: &[u8]) -> ParseResult<Vec<Data>> {
    let (mut rest_outer, (bytes_consumed, header_size)) = consumed(varint)(input)?;
    let mut remaining_in_header = header_size - bytes_consumed.len() as i64;
    let mut serial_types = Vec::new();
    while remaining_in_header > 0 {
        let (rest, (bytes_consumed, serial_type)) = consumed(varint)(rest_outer)?;
        rest_outer = rest;
        serial_types.push(serial_type);
        remaining_in_header -= bytes_consumed.len() as i64;
    }
    let mut res = Vec::new();
    for s in serial_types {
        match s {
            0 => res.push(Data::Null),
            1 => {
                let (rest, x) = be_i8(rest_outer)?;
                rest_outer = rest;
                res.push(Data::Integer(x as i64));
            }
            2 => {
                let (rest, x) = be_i16(rest_outer)?;
                rest_outer = rest;
                res.push(Data::Integer(x as i64));
            }
            3 => {
                let (rest, x) = be_i24(rest_outer)?;
                rest_outer = rest;
                res.push(Data::Integer(x as i64));
            }
            4 => {
                let (rest, x) = be_i32(rest_outer)?;
                rest_outer = rest;
                res.push(Data::Integer(x as i64));
            }
            5 => {
                let (rest, xs) = count(be_u24, 2)(rest_outer)?;
                let x = (*xs.first().unwrap() as u64 + (*xs.first().unwrap() as u64)) << 24;
                rest_outer = rest;
                res.push(Data::Integer(x as i64));
            }
            6 => {
                let (rest, x) = be_i64(rest_outer)?;
                rest_outer = rest;
                res.push(Data::Integer(x));
            }
            7 => {
                let (rest, x) = be_f64(rest_outer)?;
                rest_outer = rest;
                res.push(Data::Float(x));
            }
            8 => res.push(Data::Integer(0)),
            9 => res.push(Data::Integer(1)),
            s => {
                if (s % 2 == 0) & (s >= 12) {
                    let n = (s - 12) / 2;
                    let (rest, x) = take(n as usize)(rest_outer)?;
                    rest_outer = rest;
                    res.push(Data::Blob(x.to_vec()));
                } else if (s % 2 == 1) & (s >= 13) {
                    let n = (s - 13) / 2;
                    let (rest, x) = take(n as usize)(rest_outer)?;
                    rest_outer = rest;
                    res.push(Data::Text(String::from_utf8(x.to_vec()).unwrap()));
                } else {
                    Err(nom::Err::Error(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Fail,
                    )))?;
                }
            }
        }
    }
    Ok((rest_outer, res))
}

#[derive(Debug, Clone, PartialEq)]
pub enum PageValue {
    LeafTable {
        payload: Vec<Data>,
        rowid: i64,
    },
    InteriorTable {
        left_child_page: u32,
        rowid: i64,
    },
    InteriorIndex {
        left_child_page: u32,
        payload: Vec<Data>,
    },
    LeafIndex {
        payload: Vec<Data>,
    },
}

impl PageValue {
    pub fn get_payload(&self) -> Option<&Vec<Data>> {
        match self {
            PageValue::LeafTable { payload, .. } => Some(payload),
            PageValue::InteriorTable { .. } => None,
            PageValue::InteriorIndex { payload, .. } => Some(payload),
            PageValue::LeafIndex { payload } => Some(payload),
        }
    }
}

#[derive(Debug)]
pub struct Page {
    pub header: PageHeader,
    pub values: Vec<PageValue>,
}

pub fn parse_page(input: &[u8], is_first_page: bool) -> ParseResult<Page> {
    let offset = if is_first_page { 100 } else { 0 };
    let (rest, page_header) = parse_page_header(&input[offset..])?;
    let (rest, cell_pointers) = parse_cell_pointers(rest, page_header.number_of_cells)?;
    let mut res = Vec::new();
    for p in cell_pointers {
        let (_rest, cell) = parse_cell(&input[p as usize..], page_header.page_type)?;
        match cell {
            Cell::TableLeaf(content) => {
                res.push(PageValue::LeafTable {
                    payload: content.payload,
                    rowid: content.row_id,
                });
            }
            Cell::TableInterior(content) => {
                res.push(PageValue::InteriorTable {
                    left_child_page: content.left_child_page,
                    rowid: content.row_id,
                });
            }
            Cell::IndexLeaf(content) => {
                res.push(PageValue::LeafIndex {
                    payload: content.payload,
                });
            }
            Cell::IndexInterior(content) => {
                res.push(PageValue::InteriorIndex {
                    left_child_page: content.left_child_page,
                    payload: content.payload,
                });
            }
        }
    }
    Ok((
        rest,
        Page {
            header: page_header,
            values: res,
        },
    ))
}

fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub modifiers: String,
    pub ipk: bool, // is an integer primary key
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Clone)]
pub enum Comparator {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause {
    pub column: String,
    pub operator: Comparator,
    pub value: String,
}

fn parse_where(input: &str) -> ParseResult<WhereClause, &str> {
    let (rest, _) = ws(tag_no_case("where"))(input)?;
    let (rest, column) = ws(identifier)(rest)?;
    let (rest, operator) = alt((
        tag("="),
        tag("!="),
        tag("<"),
        tag(">"),
        tag("<="),
        tag(">="),
    ))(rest)?;
    let (rest, value) = ws(preceded(
        char('\''),
        map(many_till(anychar, char('\'')), |(chars, _)| chars),
    ))(rest)?;
    Ok((
        rest,
        WhereClause {
            column: column.to_string(),
            operator: match operator {
                "=" => Comparator::Eq,
                // "!=" => Comparator::Ne,
                // "<" => Comparator::Lt,
                // ">" => Comparator::Gt,
                // "<=" => Comparator::Le,
                // ">=" => Comparator::Ge,
                _ => {
                    return Err(nom::Err::Error(nom::error::Error {
                        input,
                        code: nom::error::ErrorKind::Fail,
                    }))
                }
            },
            value: value.iter().collect::<String>(),
        },
    ))
}

pub fn parse_select(input: &str) -> ParseResult<(Vec<&str>, &str, Option<WhereClause>), &str> {
    let columns = separated_list1(ws(char::<&str, nom::error::Error<_>>(',')), identifier);
    let (rest, (columns, _from, table, where_)) = preceded(
        ws(tag_no_case("select")),
        tuple((
            alt((
                map(tag_no_case("count(*)"), |s| vec![s]),
                // map(alphanumeric1, |s| vec![s]),
                columns,
            )),
            ws(tag_no_case("from")),
            identifier,
            opt(parse_where),
        )),
    )(input)?;
    Ok((rest, (columns, table, where_)))
}

fn identifier(input: &str) -> ParseResult<&str, &str> {
    alt((
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
        delimited(
            char('"'),
            take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == ' '),
            char('"'),
        ),
    ))(input)
}

pub fn parse_create_table(input: &str) -> ParseResult<Vec<ColumnDef>, &str> {
    let ipk_regex = Regex::new(r"(?i)integer primary key").unwrap();
    let list = delimited(
        ws(char('(')),
        separated_list0(ws(char(',')), many0(ws(identifier))),
        ws(char(')')),
    );
    let (rest, desc) = preceded(
        ws(tag_no_case("create table")),
        preceded(
            ws(delimited(opt(char('"')), identifier, opt(char('"')))),
            list,
        ),
    )(input)?;
    let columns: Vec<String> = desc.iter().map(|x| x.join(" ")).collect::<Vec<String>>();
    let res = columns
        .iter()
        .map(|x| ColumnDef {
            name: x.split(' ').collect::<Vec<_>>()[0].to_string(),
            modifiers: x.split(' ').collect::<Vec<_>>()[1..].join(" "),
            ipk: ipk_regex.is_match(x),
        })
        .collect();
    Ok((rest, res))
}

#[cfg(test)]
mod tests {
    use nom::{character::complete::alphanumeric1, UnspecializedInput};

    use super::*;

    #[test]
    fn test_ws_identifier() {
        let input = "hello there";
        let res = many0(ws(identifier))(input);
        println!("{:?}", res);
        assert_eq!(res, Ok(("", vec!["hello", "there"])));
    }

    #[test]
    fn test_create_table() {
        let input = "create table foo (id integer primary key, name text)";
        let res = parse_create_table(input);
        println!("{:?}", res);
        assert_eq!(
            res,
            Ok((
                "",
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        modifiers: "integer primary key".to_string(),
                        ipk: true,
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        modifiers: "text".to_string(),
                        ipk: false,
                    }
                ]
            ))
        );
    }

    #[test]
    fn test_varint() {
        let input = &[0b1000_0001, 0b1000_0001, 0b0000_0001];
        let res = varint(input);
        if let Ok((_, n)) = res {
            println!("{:b}", n);
        }
        assert_eq!(res, Ok((&Vec::new()[..], 0b100_0000_1000_0001)));
    }

    #[test]
    fn test_where() {
        let input = "where name_o = \'hello\'";
        let res = parse_where(input);
        println!("{:?}", res);
        assert_eq!(
            res,
            Ok((
                "",
                WhereClause {
                    column: "name_o".to_string(),
                    operator: Comparator::Eq,
                    value: "hello".to_string()
                }
            ))
        );
    }

    #[test]
    fn test_select() {
        let input = "SELECT id, name, eye_color FROM superheroes WHERE eye_color = \'Pink Eyes\'";
        let res = parse_select(input);
        println!("{:?}", res);
        assert_eq!(
            res,
            Ok((
                "",
                (
                    vec!["id", "name"],
                    "foo",
                    Some(WhereClause {
                        column: "name".to_string(),
                        operator: Comparator::Eq,
                        value: "hello".to_string()
                    })
                )
            ))
        );
    }

    #[test]
    fn test() {
        let input = "id integer, name text";
        let res = separated_list0(
            char(','),
            many0(ws(alphanumeric1::<&str, nom::error::Error<_>>)),
        )(input);
        println!("{:?}", res);
        assert_eq!(
            res,
            Ok(("", vec![vec!["id", "integer"], vec!["name", "text"]]))
        );
    }

    #[test]
    fn test_identifier() {
        let input = "hello_there";
        let res = identifier(input);
        println!("{:?}", res);
        assert_eq!(res, Ok(("", "hello_there")));
        let res2 = identifier("hello there");
        assert_eq!(res2, Ok((" there", "hello")));
    }
}
