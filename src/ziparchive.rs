use std::{
    fs::File,
    io::{BufRead, BufReader, IoSliceMut, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{atomic::AtomicU32, Arc},
};

use eyre::Result;
use zip::ZipArchive;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
/// Path to dblock.zip
pub struct ZipLocation {
    pub path_str: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct BlockLocation {
    /// Which file inside the zip
    pub file_index: u32,

    /// Which dblock.zip file
    pub zip_path: Arc<ZipLocation>,
}

pub struct ZipArchiveWrapper {
    pub zip_path: Arc<ZipLocation>,
    pub archive: ZipArchive<MyCloneFileReader>,
}

pub struct MyCloneFileConfig {
    pub path: PathBuf,
    /// Changes after the files are indexed.
    /// Bigger buf helps with large file reads.
    /// Smaller buf does less redundant byte reads from disk when indexing.
    pub buf_capacity: AtomicU32,
}
pub struct MyCloneFileReader {
    pub config: Arc<MyCloneFileConfig>,
    buf_reader: BufReader<File>,
}

impl Clone for MyCloneFileReader {
    fn clone(&self) -> Self {
        Self::new(self.config.clone()).unwrap()
    }
}

impl MyCloneFileReader {
    pub fn new(config: Arc<MyCloneFileConfig>) -> Result<Self> {
        let target_file = File::open(&config.path)?;
        let cap = config
            .buf_capacity
            .load(std::sync::atomic::Ordering::Relaxed);
        let filebuf = BufReader::with_capacity(cap as usize, target_file);

        Ok(Self {
            config: config.clone(),
            buf_reader: filebuf,
        })
    }
}

impl Read for MyCloneFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.buf_reader.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.buf_reader.read_exact(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.buf_reader.read_vectored(bufs)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        self.buf_reader.read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> std::io::Result<usize> {
        self.buf_reader.read_to_string(buf)
    }
}

impl Seek for MyCloneFileReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.buf_reader.seek(pos)
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        self.buf_reader.stream_position()
    }
}

impl BufRead for MyCloneFileReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.buf_reader.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.buf_reader.consume(amt)
    }
}
