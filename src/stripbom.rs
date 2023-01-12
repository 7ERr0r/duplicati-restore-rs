use eyre::Result;
use std::io::{BufRead, Read};

pub trait StripBom {
    fn strip_bom(&self) -> &str;
}

impl StripBom for str {
    fn strip_bom(&self) -> &str {
        if let Some(stripped) = self.strip_prefix('\u{feff}') {
            stripped
        } else {
            self
        }
    }
}

impl StripBom for String {
    fn strip_bom(&self) -> &str {
        self[..].strip_bom()
    }
}

fn starts_with_bom_bytes(s: &[u8]) -> bool {
    s.starts_with(&[0xEF, 0xBB, 0xBF])
}

pub trait StripBomBytes {
    fn strip_bom(&self) -> &[u8];
}

impl StripBomBytes for [u8] {
    fn strip_bom(&self) -> &[u8] {
        if let Some(stripped) = self.strip_prefix(&[0xEF, 0xBB, 0xBF]) {
            stripped
        } else {
            self
        }
    }
}

pub fn strip_bom_from_bufread<R: BufRead>(mut inner: R) -> Result<()> {
    let buf = inner.fill_buf()?;
    if buf.len() >= 3 && starts_with_bom_bytes(buf) {
        //println!("removing bom");
        inner.consume(3);
    }

    // here we ignore case of 1 or 2 bytes of BOM
    Ok(())
}

pub struct StripBomReader<R: BufRead> {
    pub first_bytes: bool,
    pub inner: R,
}

impl<R: BufRead> StripBomReader<R> {
    #[allow(unused)]
    pub fn new(mut inner: R) -> Result<StripBomReader<R>> {
        strip_bom_from_bufread(&mut inner)?;

        Ok(Self {
            first_bytes: true,
            inner,
        })
    }
}

impl<R: BufRead> Read for StripBomReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}
