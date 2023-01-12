use std::{cell::RefCell, fmt::Display};

use base64::engine::general_purpose;
use smallvec::SmallVec;

use crate::hexdisplay::HexDisplayBytes;
thread_local! {
    pub static BASE64_DECODE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64));

}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct BlockIdHash {
    pub hash: SmallVec<[u8; 32]>,
}

impl Display for BlockIdHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", HexDisplayBytes(self.hash.as_slice()))
    }
}

impl BlockIdHash {
    pub fn from_bytes(b: &[u8]) -> Option<BlockIdHash> {
        if b.len() != 32 {
            return None;
        }
        Some(BlockIdHash {
            hash: SmallVec::from_slice(b),
        })
    }

    pub fn from_base64(block_id_str: &str) -> Option<BlockIdHash> {
        Self::from_base64_config(block_id_str, general_purpose::STANDARD)
    }
    #[allow(unused)]
    pub fn from_base64_urlsafe(block_id_str: &str) -> Option<BlockIdHash> {
        Self::from_base64_config(block_id_str, general_purpose::URL_SAFE)
    }

    pub fn from_base64_config<E: base64::Engine>(
        block_id_str: &str,
        engine: E,
    ) -> Option<BlockIdHash> {
        BASE64_DECODE_BUF.with(|b| -> Option<BlockIdHash> {
            let buffer: &mut Vec<u8> = &mut b.borrow_mut();
            assert!(block_id_str.len() < buffer.capacity());
            engine.decode_slice(block_id_str, buffer).ok()?;
            let hash = BlockIdHash {
                hash: SmallVec::from_slice(buffer),
            };
            buffer.clear();
            Some(hash)
        })
    }

    #[allow(unused)]
    pub fn as_base64<'a>(&self, buf: &'a mut [u8]) -> &'a str {
        self.as_base64_config(general_purpose::STANDARD, buf)
    }
    pub fn as_base64_urlsafe<'a>(&self, buf: &'a mut [u8]) -> &'a str {
        self.as_base64_config(general_purpose::URL_SAFE, buf)
    }
    pub fn as_base64_config<'a, E: base64::Engine>(&self, engine: E, buf: &'a mut [u8]) -> &'a str {
        let encoded_len = engine
            .encode_slice(self.hash.as_slice(), &mut buf[..])
            .unwrap();
        //debug_assert_eq!(encoded_len, buf.len());

        std::str::from_utf8(&buf[..encoded_len]).expect("Invalid UTF8")
    }
}
