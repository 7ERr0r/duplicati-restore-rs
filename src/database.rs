use crate::blockhash::BlockIdHash;
use crate::ziparchive::BlockLocation;
use crate::ziparchive::MyCloneFileConfig;
use crate::ziparchive::MyCloneFileReader;
use crate::ziparchive::ZipArchiveWrapper;
use crate::ziparchive::ZipLocation;
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

    // pub fn get_zip_path_by_base64(&self, block_id: &str) -> Option<PathBuf> {
    //     if let Some(hash2path) = &self.hash2path {
    //         hash2path.get_zip_path_by_base64(block_id)
    //     } else {
    //         None
    //     }
    // }

    pub fn get_location_by_block_id(&self, block_id: &BlockIdHash) -> Option<BlockLocation> {
        if let Some(hash2path) = &self.hash2path {
            hash2path.get_location_by_block_id(block_id)
        } else {
            let buf = &mut [0u8; 48];
            let name_reencoded = block_id.as_base64_urlsafe(buf);
            for ziparch in self.zip2ziparchive.values() {
                if let Some(index) = ziparch.archive.get_file_index(name_reencoded) {
                    return Some(BlockLocation {
                        file_index: index as u32,
                        zip_path: ziparch.zip_path.clone(),
                    });
                }
            }
            None
        }
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
                )
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
        let zip = zip::ZipArchive::new(zipbuf)?;

        let arc_zippath = Arc::new(ZipLocation {
            path: zip_path.clone(),
            path_str: zip_path.to_string_lossy().to_string(),
        });
        // Convert to a list of paths

        for (index, file_name) in zip.file_names_ordered().enumerate() {
            //let file_in_zip = zip.by_index_raw(file_index)?;
            //let file_name = file_in_zip.name().to_string();
            let hash_path = file_name;
            let hash = base64::decode_config(&hash_path, base64::URL_SAFE)?;
            {
                if hash.len() > 32 {
                    println!("warn: hash len:{} requires heap alloc", hash.len());
                }
                let mut inner = self.inner.lock().unwrap();
                if let Some(hash2path) = &mut inner.hash2path {
                    hash2path.hash2path.insert(
                        hash.into(),
                        BlockLocation {
                            zip_path: arc_zippath.clone(),
                            file_index: index as u32,
                        },
                    );
                }
            }
        }

        {
            use std::sync::atomic::Ordering;
            config.buf_capacity.store(32 * 1024, Ordering::Relaxed);
            let mut inner = self.inner.lock().unwrap();
            let wrapper = ZipArchiveWrapper {
                zip_path: arc_zippath.clone(),
                archive: zip,
            };
            let path_str = arc_zippath.path_str.clone();
            inner.zip2ziparchive.insert(path_str, wrapper);
        }

        Ok(())
    }

    pub fn get_block_id_location(&self, block_id: &BlockIdHash) -> Option<BlockLocation> {
        self.inner
            .lock()
            .unwrap()
            .get_location_by_block_id(block_id)
    }

    // pub fn get_zip_path_from_block_id(&self, block_id: &str) -> Option<PathBuf> {
    //     self.inner.lock().unwrap().get_zip_path_by_base64(block_id)
    // }

    // pub fn get_zip_archive(&self, zip_filename: &str) -> Option<ZipArchive<MyCloneFileReader>> {
    //     self.inner.lock().unwrap().get_zip_archive(zip_filename)

    // }

    pub fn get_zip_by_block_id(
        &self,
        block_id: &BlockIdHash,
    ) -> Option<ZipArchive<MyCloneFileReader>> {
        self.inner.lock().unwrap().get_zip_by_block_id(block_id)
    }

    pub fn get_content_block(&self, block_id: &BlockIdHash) -> Result<Option<Vec<u8>>> {
        let mut output = Vec::new();

        //let mut zip = zip::ZipArchive::new(File::open(filename).unwrap()).unwrap();
        let ziparch = self.get_zip_by_block_id(block_id);

        if let Some(mut ziparch) = ziparch {
            let buf = &mut [0u8; 48];
            let name_reencoded = block_id.as_base64_urlsafe(buf);
            let mut block = ziparch
                .by_name(name_reencoded)
                .wrap_err("block file by name not found even though we indexed it before")?;
            block
                .read_to_end(&mut output)
                .wrap_err_with(|| format!("reading block file {:?}", block_id))?;

            Ok(Some(output))
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
