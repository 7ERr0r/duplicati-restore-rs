use base64;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use unqlite::{Transaction, UnQLite, KV};
use zip;
use eyre::Result;
use eyre::eyre;

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
    pub fn new(file: &str, manifest: &str) -> Result<DB> {
        let conn = UnQLite::create(file);
        conn.kv_store("test_key_name", "test_key_value").map_err(|_| eyre!("can't write to database"))?;
        let manifest: Manifest = serde_json::from_str(manifest)?;
        let db = DB { conn, manifest };
        Ok(db)
    }

    pub fn create_block_id_to_filenames(self, paths: &[String]) -> Result<Self> {
        // Iterate through dblocks, adding them to the db
        let pb = ProgressBar::new(paths.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] {wide_bar:40.cyan/blue} {pos:>7}/{len:7} {msg} [{eta_precise}]",
                )
                .progress_chars("##-"),
        );
        let conn = &self.conn;
        paths
            .par_iter()
            .map(|zippath| {
                // In this stage, open the file
                let zipfile = File::open(&Path::new(zippath)).unwrap();
                let zipbuf = BufReader::new(zipfile);
                let zip = zip::ZipArchive::new(zipbuf).unwrap();
                (zip, zippath)
            })
            .map(|(mut zip, zippath)| {
                // Convert to a list of paths
                let paths: Vec<String> = (0..zip.len())
                    .map(|i| zip.by_index(i).unwrap().name().to_string())
                    .collect();
                (paths, zippath)
            })
            .for_each(|(paths, zippath)| {
                let bytes = zippath.as_bytes();
                for p in paths {
                    println!("zippath:{} p:{}", zippath, p);
                    let hash = base64::decode_config(&p, base64::URL_SAFE).unwrap();
                    conn.kv_store(hash, bytes).unwrap();
                }
                conn.commit().unwrap();
                pb.inc(1);
            });

        Ok(self)
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
