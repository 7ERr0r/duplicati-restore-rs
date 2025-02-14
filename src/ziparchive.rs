use eyre::Result;
use std::{
    fs::File,
    io::{BufRead, BufReader, IoSliceMut, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{atomic::AtomicU32, Arc},
};
use zip::ZipArchive;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
/// Path to dblock.zip
pub struct ZipLocation {
    // pub path_str: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockLocation {
    /// Which dblock.zip file
    pub ziplocation: Arc<ZipLocation>,

    /// Which file inside the zip
    pub file_index: u32,
}

impl Ord for BlockLocation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First zip_path (which dblock.zip it is)
        // then file_index inside the ZIP file
        self.ziplocation
            .cmp(&other.ziplocation)
            .then_with(|| self.file_index.cmp(&other.file_index))
    }
}

impl PartialOrd for BlockLocation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct ZipArchiveWrapper {
    pub ziplocation: Arc<ZipLocation>,
    pub archive: ZipArchive<MyCloneFileReader>,
}

impl ZipArchiveWrapper {
    pub fn get_block_location(&self, block_base64: &str) -> Option<BlockLocation> {
        self.archive
            .get_file_index(block_base64)
            .map(|index| BlockLocation {
                file_index: index as u32,
                ziplocation: self.ziplocation.clone(),
            })
    }

    pub fn contains_file_name(&self, block_base64: &str) -> bool {
        self.archive.contains_file_name(block_base64)
    }
}

pub struct MyCloneFileConfig {
    pub path: PathBuf,
    /// Changes after the files are indexed.
    /// Bigger buf helps with large file reads.
    /// Smaller buf does less redundant byte reads from disk when indexing.
    pub buf_capacity: AtomicU32,
}

/// Used to share ZipArchive across many threads
///
/// Multiple ZipArchive structs would allocate too much Vec<Files> in rayon threads
///
/// Therefore we open file again on every .clone()
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
            config,
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
