use base64;
use pbr::ProgressBar;
use rayon::prelude::*;
use serde::Deserialize;
use serde_json;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use unqlite::{UnQLite, KV};
use zip;

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

pub struct DB {
    conn: UnQLite,
    manifest: Manifest,
}

impl DB {
    pub fn new(file: &str, manifest: &str) -> DB {
        let conn = UnQLite::create(file);
        let manifest: Manifest = serde_json::from_str(manifest).unwrap();
        DB { conn, manifest }
    }

    pub fn create_block_id_to_filenames(self, paths: &[String]) -> Self {
        // Iterate through dblocks, adding them to the db
        let pb = Arc::new(Mutex::new(ProgressBar::new(paths.len() as u64)));
        paths.par_iter().for_each(|path| {
            // Open zip file
            let file = File::open(&Path::new(path)).unwrap();
            let mut zip = zip::ZipArchive::new(file).unwrap();
            // Iterate through contents and collect items to add to database
            let mut cache: Vec<(Vec<u8>, String)> = Vec::new();
            for i in 0..zip.len() {
                let inner_file = zip.by_index(i).unwrap();
                let hash = base64::decode_config(inner_file.name(), base64::URL_SAFE).unwrap();
                cache.push((hash, path.clone()));
            }
            // Load items from cache into databse

            let conn = &self.conn;
            for (hash, path) in cache.iter() {
                conn.kv_store(hash, path.as_bytes()).unwrap();
            }
            pb.lock().unwrap().inc();
        });

        self
    }

    pub fn get_filename_from_block_id(&self, block_id: &str) -> Option<String> {
        let conn = &self.conn;
        //        println!("{}", block_id);
        //        let converted_block_id = base64_url_to_plain(block_id);
        let result = conn.kv_fetch(base64::decode_config(block_id, base64::STANDARD).unwrap());
        if let Ok(path_bytes) = result {
            Some(String::from_utf8(path_bytes).unwrap())
        } else {
            None
        }
    }

    pub fn get_content_block(&self, block_id: &str) -> Option<Vec<u8>> {
        let mut output = Vec::new();
        if let Some(filename) = self.get_filename_from_block_id(block_id) {
            let mut zip = zip::ZipArchive::new(File::open(filename).unwrap()).unwrap();
            let mut block = zip
                .by_name(&base64::encode_config(
                    &base64::decode(block_id).unwrap(),
                    base64::URL_SAFE,
                ))
                .unwrap();
            block.read_to_end(&mut output).unwrap();

            Some(output)
        } else {
            None
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
