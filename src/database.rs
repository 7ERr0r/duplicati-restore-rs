use crate::blockhash::BlockIdHash;
use crate::ziparchive::BlockLocation;
use crate::ziparchive::MyCloneFileConfig;
use crate::ziparchive::MyCloneFileReader;
use crate::ziparchive::ZipArchiveWrapper;
use crate::ziparchive::ZipLocation;
use base64::engine::general_purpose;
use base64::Engine;
use eyre::Context;
use eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelIterator;
use serde::Deserialize;

use smallvec::SmallVec;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::Mutex;
use zip::ZipArchive;

#[derive(Deserialize)]
#[allow(dead_code)] // Will use all these fields in the future
struct Manifest {
    #[serde(rename = "Version")]
    pub(self) version: i64,
    #[serde(rename = "Created")]
    pub(self) created: String,
    #[serde(rename = "Encoding")]
    pub(self) encoding: String,
    #[serde(rename = "Blocksize")]
    pub(self) block_size: i64,
    #[serde(rename = "BlockHash")]
    pub(self) block_hash: String,
    #[serde(rename = "FileHash")]
    pub(self) file_hash: String,
    #[serde(rename = "AppVersion")]
    pub(self) app_version: String,
}

pub struct HashToPath {
    /// Maps hash (without base64) to location in dblock.zip
    ///
    /// May be faster, but it's memory-intensive
    hash2path: HashMap<SmallVec<[u8; 32]>, BlockLocation>,
}
impl HashToPath {
    pub fn new() -> Self {
        Self {
            hash2path: HashMap::new(),
        }
    }

    pub fn get_zip_path_by_block_id(&self, block_id: &BlockIdHash) -> Option<PathBuf> {
        self.hash2path
            .get(&block_id.hash)
            .map(|v| v.zip_path.path.clone())
    }

    pub fn get_location_by_block_id(&self, block_id: &BlockIdHash) -> Option<BlockLocation> {
        self.hash2path.get(&block_id.hash).cloned()
    }
}
pub struct HashToBlocks {
    /// Maps zip file name to a singleton zip reader
    zip2ziparchive: HashMap<String, ZipArchiveWrapper>,

    /// zip_entry_name -> zip_name
    ///
    /// takes a lot of RAM so it's not used by default
    hash2path: Option<HashToPath>,
}

impl HashToBlocks {
    pub fn new(use_hash_to_path: bool) -> Self {
        let hash2path = if use_hash_to_path {
            Some(HashToPath::new())
        } else {
            None
        };
        let zip2ziparchive = HashMap::new();
        Self {
            hash2path,
            zip2ziparchive,
        }
    }

    pub fn get_location_by_block_id(&self, block_id: &BlockIdHash) -> Option<BlockLocation> {
        if let Some(hash2path) = &self.hash2path {
            hash2path.get_location_by_block_id(block_id)
        } else {
            self.get_location_by_block_id_purezip(block_id)
        }
    }
    pub fn get_location_by_block_id_purezip(
        &self,
        block_id: &BlockIdHash,
    ) -> Option<BlockLocation> {
        let buf = &mut [0u8; 48];
        let name_reencoded = block_id.as_base64_urlsafe(buf);
        for ziparch in self.zip2ziparchive.values() {
            let location =
                ziparch
                    .archive
                    .get_file_index(name_reencoded)
                    .map(|index| BlockLocation {
                        file_index: index as u32,
                        zip_path: ziparch.zip_path.clone(),
                    });
            if location.is_some() {
                return location;
            }
        }
        None
    }

    pub fn get_zip_archive(&self, zip_filename: &str) -> Option<ZipArchive<MyCloneFileReader>> {
        let zip = self.zip2ziparchive.get(zip_filename);

        zip.map(|zip| zip.archive.clone())
    }

    pub fn get_zip_by_block_id(
        &self,
        block_id: &BlockIdHash,
    ) -> Option<ZipArchive<MyCloneFileReader>> {
        if let Some(hash2path) = &self.hash2path {
            let zname = hash2path.get_zip_path_by_block_id(block_id);
            let zname = zname.map(|n| n.to_string_lossy().to_string());
            zname.and_then(|zname| self.get_zip_archive(&zname))
        } else {
            self.get_zip_by_block_id_purezip(block_id)
        }
    }

    pub fn get_zip_by_block_id_purezip(
        &self,
        block_id: &BlockIdHash,
    ) -> Option<ZipArchive<MyCloneFileReader>> {
        let buf = &mut [0u8; 48];
        let name_reencoded = block_id.as_base64_urlsafe(buf);
        for ziparch in self.zip2ziparchive.values() {
            if ziparch.archive.contains_file_name(name_reencoded) {
                return Some(ziparch.archive.clone());
            }
        }
        None
    }
}

pub struct DFileDatabase {
    inner: Arc<Mutex<HashToBlocks>>,
    manifest: Manifest,
}

impl DFileDatabase {
    pub fn new(manifest_bytes: &[u8], use_hash_to_path: bool) -> Result<Self> {
        // let conn = UnQLite::create(file);
        // conn.kv_store("test_key_name", "test_key_value").map_err(|_| eyre!("can't write to database"))?;
        let manifest: Manifest = serde_json::from_slice(manifest_bytes)?;

        let inner = Arc::new(Mutex::new(HashToBlocks::new(use_hash_to_path)));
        let db = Self { inner, manifest };
        Ok(db)
    }

    pub fn create_block_id_to_filenames(&self, paths: &[PathBuf]) -> Result<()> {
        // Iterate through dblocks, adding them to the db
        let pb = ProgressBar::new(paths.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] {wide_bar:40.cyan/blue} {pos:>7}/{len:7} {msg} [{eta_precise}]",
                )?
                .progress_chars("##-"),
        );
        paths.par_iter().try_for_each(|zip_path| {
            self.import_from_zip(zip_path)
                .wrap_err_with(|| format!("import_from_zip: {:?}", zip_path))
        })?;

        Ok(())
    }

    pub fn import_from_zip(&self, zip_path: &PathBuf) -> Result<()> {
        // In this stage, open the file
        let zip_path = Path::new(&zip_path).to_path_buf();
        let config = Arc::new(MyCloneFileConfig {
            path: zip_path.clone(),
            buf_capacity: AtomicU32::new(1024),
        });
        let zipbuf = MyCloneFileReader::new(config.clone())?;
        let ziparch = zip::ZipArchive::new(zipbuf)?;

        let arc_ziploc = Arc::new(ZipLocation { path: zip_path });

        let mut inner = self.inner.lock().unwrap();

        if let Some(hash2path) = &mut inner.hash2path {
            self.register_hash_to_path(hash2path, &ziparch, arc_ziploc.clone())?;
        }

        self.register_zip_archive(config, arc_ziploc, ziparch);

        Ok(())
    }
    /// Remembers zip file names in a hashmap
    ///
    /// zip_entry_name -> zip_name
    pub fn register_hash_to_path(
        &self,
        hash2path: &mut HashToPath,
        ziparch: &ZipArchive<MyCloneFileReader>,
        arc_ziploc: Arc<ZipLocation>,
    ) -> Result<()> {
        for (index, file_name) in ziparch.file_names_ordered().enumerate() {
            // file_name is a hash in base64
            let hash = general_purpose::URL_SAFE.decode(file_name)?;

            if hash.len() > 32 {
                println!("warn: hash len:{} requires heap alloc", hash.len());
            }

            hash2path.hash2path.insert(
                hash.into(),
                BlockLocation {
                    zip_path: arc_ziploc.clone(),
                    file_index: index as u32,
                },
            );
        }
        Ok(())
    }

    pub fn register_zip_archive(
        &self,
        config: Arc<MyCloneFileConfig>,
        arc_ziploc: Arc<ZipLocation>,
        ziparch: ZipArchive<MyCloneFileReader>,
    ) {
        use std::sync::atomic::Ordering;
        config.buf_capacity.store(32 * 1024, Ordering::Relaxed);
        let mut inner = self.inner.lock().unwrap();
        let wrapper = ZipArchiveWrapper {
            zip_path: arc_ziploc.clone(),
            archive: ziparch,
        };
        let path_str = arc_ziploc.path.to_string_lossy().to_string();
        inner.zip2ziparchive.insert(path_str, wrapper);
    }

    pub fn get_block_id_location(&self, block_id: &BlockIdHash) -> Option<BlockLocation> {
        self.inner
            .lock()
            .unwrap()
            .get_location_by_block_id(block_id)
    }

    pub fn get_zip_by_block_id(
        &self,
        block_id: &BlockIdHash,
    ) -> Option<ZipArchive<MyCloneFileReader>> {
        self.inner.lock().unwrap().get_zip_by_block_id(block_id)
    }

    pub fn get_content_block(
        &self,
        block_id: &BlockIdHash,
        block_buf: &mut Vec<u8>,
    ) -> Result<Option<usize>> {
        let ziparch = self.get_zip_by_block_id(block_id);

        if let Some(mut ziparch) = ziparch {
            let base64_buf = &mut [0u8; 48];
            let name_reencoded = block_id.as_base64_urlsafe(base64_buf);
            let mut block = ziparch
                .by_name(name_reencoded)
                .wrap_err("block file by name not found even though we indexed it before")?;
            let n = block
                .read_to_end(block_buf)
                .wrap_err_with(|| format!("reading block file {:?}", block_id))?;

            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    pub fn block_size(&self) -> usize {
        self.manifest.block_size as usize
    }

    pub fn offset_size(&self) -> usize {
        // opts['hashes-per-block'] * opts['blocksize']
        let hashes_per_block = self.manifest.block_size / 32; // Assumes SHA-256
        (hashes_per_block * self.manifest.block_size) as usize
    }

    pub fn hash_size(&self) -> usize {
        32
    }
}
