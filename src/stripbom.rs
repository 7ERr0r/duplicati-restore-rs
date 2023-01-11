pub trait StripBom {
    fn strip_bom(&self) -> &str;
}

impl StripBom for str {
    fn strip_bom(&self) -> &str {
        if self.starts_with("\u{feff}") {
            &self[3..]
        } else {
            &self[..]
        }
    }
}

impl StripBom for String {
    fn strip_bom(&self) -> &str {
        &self[..].strip_bom()
    }
}

pub trait StripBomBytes {
    fn strip_bom(&self) -> &[u8];
}

impl StripBomBytes for [u8] {
    fn strip_bom<'a>(&'a self) -> &'a [u8] {
        if self.starts_with(&[0xEF, 0xBB, 0xBF]) {
            &self[3..]
        } else {
            &self[..]
        }
    }
}
