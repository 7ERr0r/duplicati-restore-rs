pub struct HexDisplayBytes<'a>(pub &'a [u8]);
impl<'a> std::fmt::Display for HexDisplayBytes<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0.as_ref().iter() {
            let (high, low) = byte2hex(*byte, HEX_CHARS_LOWER);

            write!(f, "{}{}", high as char, low as char)?;
        }

        Ok(())
    }
}

pub struct EscapeWholeString<'a>(pub &'a [u8]);
impl<'a> std::fmt::Display for EscapeWholeString<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0.as_ref().iter() {
            let (high, low) = byte2hex(*byte, HEX_CHARS_LOWER);

            write!(f, "\\x{}{}", high as char, low as char)?;
        }

        Ok(())
    }
}

pub struct EscapeRawString<'a>(pub &'a [u8]);
impl<'a> std::fmt::Display for EscapeRawString<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"")?;
        for &b in self.0.as_ref().iter() {
            escape_byte_maybe(f, b)?;
        }
        write!(f, "\"")?;

        Ok(())
    }
}

fn escape_byte_maybe(f: &mut std::fmt::Formatter<'_>, b: u8) -> std::fmt::Result {
    if b > 32 && b < 126 && b != b'"' {
        write!(f, "{}", b as char)?;
    } else {
        let (high, low) = byte2hex(b, HEX_CHARS_LOWER);

        write!(f, "\\x{}{}", high as char, low as char)?;
    }
    Ok(())
}

const HEX_CHARS_LOWER: &[u8; 16] = b"0123456789abcdef";

/// returns 2 chars representing byte in hex
fn byte2hex(byte: u8, table: &[u8; 16]) -> (u8, u8) {
    let high = table[((byte & 0xf0) >> 4) as usize];
    let low = table[(byte & 0x0f) as usize];

    (high, low)
}
