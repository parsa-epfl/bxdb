use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::chunk::{FORMAT_VERSION, HEADER_SIZE, MAGIC_IDX, MAGIC_LOG};

pub fn write_log_header<W: Write>(w: &mut W) -> io::Result<()> {
    write_header(w, &MAGIC_LOG)
}

pub fn write_index_header<W: Write>(w: &mut W) -> io::Result<()> {
    write_header(w, &MAGIC_IDX)
}

fn write_header<W: Write>(w: &mut W, magic: &[u8; 8]) -> io::Result<()> {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..8].copy_from_slice(magic);
    buf[8] = FORMAT_VERSION;
    w.write_all(&buf)
}

pub fn read_and_verify_header<R: Read>(r: &mut R, expected_magic: &[u8; 8]) -> io::Result<()> {
    let mut buf = [0u8; HEADER_SIZE];
    r.read_exact(&mut buf)?;
    if &buf[0..8] != expected_magic {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad magic",
        ));
    }
    if buf[8] != FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported format version {}", buf[8]),
        ));
    }
    Ok(())
}

pub fn peek_magic<R: Read + Seek>(r: &mut R) -> io::Result<[u8; 8]> {
    let pos = r.stream_position()?;
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    r.seek(SeekFrom::Start(pos))?;
    Ok(buf)
}
