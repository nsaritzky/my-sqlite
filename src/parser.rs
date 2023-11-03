use anyhow::bail;
use nom::{
    bytes::complete::{tag, take},
    number::complete::{be_u16, be_u32, be_u8},
    sequence::{terminated, tuple},
    IResult,
};

#[derive(Debug)]
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

type ParseResult<'a, T> = IResult<&'a [u8], T>;

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
    pub page_type: u8,
    pub first_freeblock_offset: u16,
    pub number_of_cells: u16,
    pub cell_content_area_offset: u16,
    pub fragmented_free_bytes: u8,
    pub right_most_pointer: u32,
}

pub fn parse_page_header(input: &[u8]) -> ParseResult<PageHeader> {
    let mut parser = tuple((be_u8, be_u16, be_u16, be_u16, be_u8, be_u32));
    let (rest, t) = parser(input)?;
    Ok((
        rest,
        PageHeader {
            page_type: t.0,
            first_freeblock_offset: t.1,
            number_of_cells: t.2,
            cell_content_area_offset: t.3,
            fragmented_free_bytes: t.4,
            right_most_pointer: t.5,
        },
    ))
}
