//! Minimal Parquet writer: plain encoding, uncompressed, one row group per call.
//! Enough for the two output schemas. Readers accept any integer width.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

const I32: u8 = 5;
const I64: u8 = 6;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const STRUCT: u8 = 12;

const TY_INT32: i32 = 1;
const TY_INT64: i32 = 2;
const TY_BYTE_ARRAY: i32 = 6;
const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;
const CONV_UTF8: i32 = 0;
const CONV_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const PAGE_DATA: i32 = 0;

struct Buf {
    data: Vec<u8>,
}

impl Buf {
    fn new() -> Self {
        Self { data: Vec::new() }
    }
    fn u8(&mut self, v: u8) {
        self.data.push(v);
    }
    fn raw(&mut self, b: &[u8]) {
        self.data.extend_from_slice(b);
    }
    fn varint(&mut self, mut v: u64) {
        while v > 0x7f {
            self.u8((v as u8) | 0x80);
            v >>= 7;
        }
        self.u8(v as u8);
    }
    fn zz32(&mut self, n: i32) {
        self.varint(((n << 1) ^ (n >> 31)) as u32 as u64);
    }
    fn zz64(&mut self, n: i64) {
        self.varint(((n << 1) ^ (n >> 63)) as u64);
    }
    fn field(&mut self, delta: u8, typ: u8) {
        debug_assert!(delta > 0 && delta < 16);
        self.u8((delta << 4) | typ);
    }
    fn bin(&mut self, bytes: &[u8]) {
        self.varint(bytes.len() as u64);
        self.raw(bytes);
    }
    fn stop(&mut self) {
        self.u8(0);
    }
    fn list_i32(&mut self, vals: &[i32]) {
        self.list_header(vals.len(), I32);
        for v in vals {
            self.zz32(*v);
        }
    }
    fn list_header(&mut self, n: usize, elem: u8) {
        if n < 15 {
            self.u8(((n as u8) << 4) | elem);
        } else {
            self.u8(0xf0 | elem);
            self.varint(n as u64);
        }
    }
}

fn schema_root(out: &mut Buf, children: i32) {
    out.field(3, I32);
    out.zz32(REP_REQUIRED);
    out.field(1, BINARY);
    out.bin(b"schema");
    out.field(1, I32);
    out.zz32(children);
    out.stop();
}

fn schema_i64(out: &mut Buf, name: &[u8]) {
    out.field(1, I32);
    out.zz32(TY_INT64);
    out.field(2, I32);
    out.zz32(REP_REQUIRED);
    out.field(1, BINARY);
    out.bin(name);
    out.stop();
}

fn schema_date(out: &mut Buf, name: &[u8]) {
    out.field(1, I32);
    out.zz32(TY_INT32);
    out.field(2, I32);
    out.zz32(REP_REQUIRED);
    out.field(1, BINARY);
    out.bin(name);
    out.field(2, I32);
    out.zz32(CONV_DATE);
    out.field(4, STRUCT);
    // LogicalType.DATE = field 6, empty DateType.
    out.field(6, STRUCT);
    out.stop();
    out.stop();
    out.stop();
}

fn schema_string(out: &mut Buf, name: &[u8], optional: bool) {
    out.field(1, I32);
    out.zz32(TY_BYTE_ARRAY);
    out.field(2, I32);
    out.zz32(if optional { REP_OPTIONAL } else { REP_REQUIRED });
    out.field(1, BINARY);
    out.bin(name);
    out.field(2, I32);
    out.zz32(CONV_UTF8);
    out.field(4, STRUCT);
    // LogicalType.STRING = field 1, empty StringType.
    out.field(1, STRUCT);
    out.stop();
    out.stop();
    out.stop();
}

fn page_header(num_values: i32, uncompressed: i32, compressed: i32) -> Vec<u8> {
    let mut h = Buf::new();
    h.field(1, I32);
    h.zz32(PAGE_DATA);
    h.field(1, I32);
    h.zz32(uncompressed);
    h.field(1, I32);
    h.zz32(compressed);
    h.field(2, STRUCT);
    h.field(1, I32);
    h.zz32(num_values);
    h.field(1, I32);
    h.zz32(ENC_PLAIN);
    h.field(1, I32);
    h.zz32(ENC_RLE);
    h.field(1, I32);
    h.zz32(ENC_RLE);
    h.stop();
    h.stop();
    h.data
}

fn plain_i64(values: &[i64]) -> Vec<u8> {
    let mut body = Vec::with_capacity(values.len() * 8);
    for v in values {
        body.extend_from_slice(&v.to_le_bytes());
    }
    finish_required_page(values.len(), body)
}

fn plain_i32(values: &[i32]) -> Vec<u8> {
    let mut body = Vec::with_capacity(values.len() * 4);
    for v in values {
        body.extend_from_slice(&v.to_le_bytes());
    }
    finish_required_page(values.len(), body)
}

fn finish_required_page(n: usize, body: Vec<u8>) -> Vec<u8> {
    let size = body.len() as i32;
    let mut page = page_header(n as i32, size, size);
    page.extend_from_slice(&body);
    page
}

/// Definition levels for an optional column, 1 bit, LSB first, one bit-packed run.
fn def_levels(present: &[bool]) -> Vec<u8> {
    if present.is_empty() {
        let mut out = Vec::new();
        out.extend_from_slice(&0u32.to_le_bytes());
        return out;
    }
    let groups = (present.len() + 7) / 8;
    let mut encoded = Vec::new();
    let header = ((groups as u64) << 1) | 1;
    let mut v = header;
    while v > 0x7f {
        encoded.push((v as u8) | 0x80);
        v >>= 7;
    }
    encoded.push(v as u8);
    for g in 0..groups {
        let mut byte = 0u8;
        for b in 0..8 {
            let i = g * 8 + b;
            if i < present.len() && present[i] {
                byte |= 1 << b;
            }
        }
        encoded.push(byte);
    }
    let mut out = Vec::with_capacity(4 + encoded.len());
    out.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    out.extend_from_slice(&encoded);
    out
}

fn plain_bytes(values: &[&[u8]]) -> Vec<u8> {
    let mut body = Vec::new();
    for v in values {
        body.extend_from_slice(&(v.len() as u32).to_le_bytes());
        body.extend_from_slice(v);
    }
    finish_required_page(values.len(), body)
}

fn plain_bytes_optional(values: &[Option<&[u8]>]) -> Vec<u8> {
    let mut present = Vec::with_capacity(values.len());
    let mut body = Vec::new();
    let mut defined = 0usize;
    for v in values {
        match v {
            Some(s) => {
                present.push(true);
                body.extend_from_slice(&(s.len() as u32).to_le_bytes());
                body.extend_from_slice(s);
                defined += 1;
            }
            None => present.push(false),
        }
    }
    let _ = defined;
    let levels = def_levels(&present);
    let mut payload = levels;
    payload.extend_from_slice(&body);
    let size = payload.len() as i32;
    let mut page = page_header(values.len() as i32, size, size);
    page.extend_from_slice(&payload);
    page
}

struct BuiltCol {
    meta_type: i32,
    name: &'static str,
    encodings: Vec<i32>,
    page: Vec<u8>,
    num_values: i64,
}

fn col_required_i64(name: &'static str, values: &[i64]) -> BuiltCol {
    BuiltCol {
        meta_type: TY_INT64,
        name,
        encodings: vec![ENC_PLAIN],
        page: plain_i64(values),
        num_values: values.len() as i64,
    }
}

fn col_required_i32(name: &'static str, values: &[i32]) -> BuiltCol {
    BuiltCol {
        meta_type: TY_INT32,
        name,
        encodings: vec![ENC_PLAIN],
        page: plain_i32(values),
        num_values: values.len() as i64,
    }
}

fn col_required_bytes(name: &'static str, values: &[&[u8]]) -> BuiltCol {
    BuiltCol {
        meta_type: TY_BYTE_ARRAY,
        name,
        encodings: vec![ENC_PLAIN],
        page: plain_bytes(values),
        num_values: values.len() as i64,
    }
}

fn col_optional_bytes(name: &'static str, values: &[Option<&[u8]>]) -> BuiltCol {
    BuiltCol {
        meta_type: TY_BYTE_ARRAY,
        name,
        encodings: vec![ENC_RLE, ENC_PLAIN],
        page: plain_bytes_optional(values),
        num_values: values.len() as i64,
    }
}

fn write_file(path: &Path, schema: &[u8], cols: &[BuiltCol]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut body = Vec::new();
    body.extend_from_slice(b"PAR1");
    let mut chunks: Vec<(i64, i64, i64, i64)> = Vec::new();
    // (file_offset, data_page_offset, uncompressed, compressed) per column.
    // Page buffer already includes its header, so the whole page is the chunk.
    for col in cols {
        let offset = body.len() as i64;
        body.extend_from_slice(&col.page);
        let size = col.page.len() as i64;
        chunks.push((offset, offset, size, size));
    }
    let footer_start = body.len();
    let mut footer = Buf::new();
    footer.field(1, I32);
    footer.zz32(1);
    footer.field(1, LIST);
    footer.list_header(1 + cols.len(), STRUCT);
    footer.raw(schema);
    let rows = cols.first().map(|c| c.num_values).unwrap_or(0);
    footer.field(1, I64);
    footer.zz64(rows);
    footer.field(1, LIST);
    if rows == 0 && cols.iter().all(|c| c.page.is_empty()) {
        footer.list_header(0, STRUCT);
    } else {
        footer.list_header(1, STRUCT);
        write_row_group(&mut footer, cols, &chunks, rows);
    }
    footer.field(2, BINARY);
    footer.bin(b"solve");
    footer.stop();
    body.extend_from_slice(&footer.data);
    let footer_len = (body.len() - footer_start) as u32;
    body.extend_from_slice(&footer_len.to_le_bytes());
    body.extend_from_slice(b"PAR1");
    let mut file = BufWriter::new(File::create(path)?);
    file.write_all(&body)?;
    file.flush()
}

fn write_row_group(out: &mut Buf, cols: &[BuiltCol], chunks: &[(i64, i64, i64, i64)], rows: i64) {
    out.field(1, LIST);
    out.list_header(cols.len(), STRUCT);
    let mut total = 0i64;
    for (col, &(file_off, data_off, uncomp, comp)) in cols.iter().zip(chunks) {
        total += uncomp;
        // ColumnChunk
        out.field(2, I64);
        out.zz64(file_off);
        out.field(1, STRUCT);
        write_column_meta(out, col, data_off, uncomp, comp);
        out.stop();
    }
    out.field(1, I64);
    out.zz64(total);
    out.field(1, I64);
    out.zz64(rows);
    out.field(2, I64);
    out.zz64(total);
    out.stop();
}

fn write_column_meta(out: &mut Buf, col: &BuiltCol, data_off: i64, uncomp: i64, comp: i64) {
    out.field(1, I32);
    out.zz32(col.meta_type);
    out.field(1, LIST);
    out.list_i32(&col.encodings);
    out.field(1, LIST);
    out.list_header(1, BINARY);
    out.bin(col.name.as_bytes());
    out.field(1, I32);
    out.zz32(0); // UNCOMPRESSED
    out.field(1, I64);
    out.zz64(col.num_values);
    out.field(1, I64);
    out.zz64(uncomp);
    out.field(1, I64);
    out.zz64(comp);
    out.field(2, I64);
    out.zz64(data_off);
    out.stop();
}

pub struct UserFields<'a> {
    pub user_id: Vec<&'a [u8]>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub events: Vec<i64>,
    pub revenue: Vec<i64>,
    pub top: Vec<Option<&'a [u8]>>,
}

pub fn write_user_daily(path: &Path, rows: &UserFields<'_>) -> std::io::Result<()> {
    let mut schema = Buf::new();
    schema_root(&mut schema, 6);
    schema_string(&mut schema, b"user_id", false);
    schema_date(&mut schema, b"day");
    schema_i64(&mut schema, b"sessions");
    schema_i64(&mut schema, b"events");
    schema_i64(&mut schema, b"revenue_cents");
    schema_string(&mut schema, b"top_category", true);
    let cols = [
        col_required_bytes("user_id", &rows.user_id),
        col_required_i32("day", &rows.day),
        col_required_i64("sessions", &rows.sessions),
        col_required_i64("events", &rows.events),
        col_required_i64("revenue_cents", &rows.revenue),
        col_optional_bytes("top_category", &rows.top),
    ];
    write_file(path, &schema.data, &cols)
}

pub struct CampFields<'a> {
    pub campaign_id: Vec<&'a [u8]>,
    pub channel: Vec<&'a [u8]>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub purchasing: Vec<i64>,
    pub revenue: Vec<i64>,
    pub assisted: Vec<i64>,
}

pub fn write_campaign_daily(path: &Path, rows: &CampFields<'_>) -> std::io::Result<()> {
    let mut schema = Buf::new();
    schema_root(&mut schema, 7);
    schema_string(&mut schema, b"campaign_id", false);
    schema_string(&mut schema, b"channel", false);
    schema_date(&mut schema, b"day");
    schema_i64(&mut schema, b"sessions");
    schema_i64(&mut schema, b"purchasing_sessions");
    schema_i64(&mut schema, b"revenue_cents");
    schema_i64(&mut schema, b"assisted_sessions");
    let cols = [
        col_required_bytes("campaign_id", &rows.campaign_id),
        col_required_bytes("channel", &rows.channel),
        col_required_i32("day", &rows.day),
        col_required_i64("sessions", &rows.sessions),
        col_required_i64("purchasing", &rows.purchasing),
        col_required_i64("revenue_cents", &rows.revenue),
        col_required_i64("assisted_sessions", &rows.assisted),
    ];
    write_file(path, &schema.data, &cols)
}
