use base64;
use eyre::eyre;
use eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::io::IoSliceMut;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use unqlite::{Transaction, UnQLite, KV};
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

pub struct ZipFilePath {
    path: String,
}

pub struct HashToPath {
    hash2path: HashMap<SmallVec<[u8; 32]>, Arc<ZipFilePath>>,
}

impl HashToPath {
    pub fn new() -> Self {
        let hash2path = HashMap::new();

        Self { hash2path }
    }
}

pub struct DB {
    // conn: UnQLite,
    inner: Arc<Mutex<HashToPath>>,
    manifest: Manifest,
}

impl DB {
    pub fn new(file: &str, manifest: &str) -> Result<DB> {
        // let conn = UnQLite::create(file);
        // conn.kv_store("test_key_name", "test_key_value").map_err(|_| eyre!("can't write to database"))?;
        let manifest: Manifest = serde_json::from_str(manifest)?;

        let inner = Arc::new(Mutex::new(HashToPath::new()));
        let db = DB { inner, manifest };
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
        //let inner = &self.inner;
        for zippath in paths {
            // In this stage, open the file

            let zipbuf = MyCloneFileReader::new(Path::new(&zippath).to_path_buf())?;
            let zip = zip::ZipArchive::new(zipbuf).unwrap();

            let zip_len = zip.len();

            //for zipfile in zip.file_names()

            // Convert to a list of paths

            let mut hvec = Vec::new();
            let (sender, receiver) = crossbeam_channel::unbounded();

            // make workers
            for t in 0..4 {
                println!("Make worker {}", t);

                let receiver = receiver.clone(); // clone for this thread

                let azippath = Arc::new(ZipFilePath {
                    path: zippath.clone(),
                });
                let zippath = zippath.clone();
                let inner = self.inner.clone();
                let mut zip = zip.clone();
                let handler = std::thread::spawn(move || {
                    //let mut rng = rand::thread_rng(); // each thread have one
                    // let zipfile = File::open(&Path::new(&zippath)).unwrap();
                    // let zipbuf = BufReader::with_capacity(1 * 1024, zipfile);
                    // let mut zip = zip::ZipArchive::new(zipbuf).unwrap();

                    loop {
                        let r = receiver.recv();
                        match r {
                            Ok(file_index) => {
                                //let s = rng.gen_range(100..1000);

                                //thread::sleep(Duration::from_millis(s));

                                let hash_path =
                                    zip.by_index(file_index).unwrap().name().to_string();
                                let hash =
                                    base64::decode_config(&hash_path, base64::URL_SAFE).unwrap();
                                {
                                    let mut inner = inner.lock().unwrap();
                                    inner.hash2path.insert(hash.into(), azippath.clone());
                                }
                            }
                            _ => break,
                        }
                    }
                });

                hvec.push(handler);
            }

            for i in 0..zip_len {
                sender.send(i).unwrap();
            }
            drop(sender);
            // let paths: Vec<String> = (0..zip_len)
            //     .into_par_iter()
            //     .map_init(
            //         || {

            //             zip
            //         },
            //         |zip, i| ,
            //     )
            //     .collect();

            // let bytes = zippath.as_bytes();
            // for p in paths {

            //     //conn.kv_store(hash, bytes).unwrap();

            //     //println!("len: {}", hash.len());
            //     //println!("zippath:{} hash:{}", zippath, p);

            // }
            //conn.commit().unwrap();
            for h in hvec {
                h.join().unwrap();
            }

            pb.inc(1);
        }

        Ok(self)
    }

    pub fn get_filename_from_block_id(&self, block_id: &str) -> Option<String> {
        //let conn = &self.conn;
        //        println!("{}", block_id);
        //        let converted_block_id = base64_url_to_plain(block_id);
        let key = base64::decode_config(block_id, base64::STANDARD).unwrap();
        let key = SmallVec::from(key);

        self.inner
            .lock()
            .unwrap()
            .hash2path
            .get(&key)
            .map(|v| v.path.clone())
        // let result = conn.kv_fetch(key);
        // if let Ok(path_bytes) = result {
        //     Some(String::from_utf8(path_bytes).unwrap())
        // } else {
        //     None
        // }
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

struct MyCloneFileReader {
    path: PathBuf,
    buf_reader: BufReader<File>,
}

impl Clone for MyCloneFileReader {
    fn clone(&self) -> Self {
        Self::new(self.path.clone()).unwrap()
    }
}

impl MyCloneFileReader {
    pub fn new(path: PathBuf) -> Result<Self> {
        let target_file = File::open(&path)?;
        let filebuf = BufReader::with_capacity(2 * 1024, target_file);

        Ok(Self {
            path,
            buf_reader: filebuf,
        })
    }
}

impl Read for MyCloneFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.buf_reader.read(buf)
    }

    // fn read_buf(&mut self, mut cursor: BorrowedCursor<'_>) -> std::io::Result<()> {
    //     self.buf_reader.read_buf(buf)
    // }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.buf_reader.read_exact(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.buf_reader.read_vectored(bufs)
    }

    // fn is_read_vectored(&self) -> bool {
    //     self.buf_reader.is_read_vectored()
    // }

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
