use base64;
use eyre::Context;
use eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelIterator;
use serde::Deserialize;
use serde_json;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::IoSliceMut;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::Mutex;
use zip;
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
    archive: ZipArchive<MyCloneFileReader>,
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

    pub fn get_zip_path_by_base64(&self, block_id: &str) -> Option<PathBuf> {
        let key = base64::decode_config(block_id, base64::STANDARD).ok()?;
        let key = SmallVec::from(key);

        self.hash2path.get(&key).map(|v| v.zip_path.path.clone())
    }

    pub fn get_location_by_base64(&self, block_id: &str) -> Option<BlockLocation> {
        let key = base64::decode_config(block_id, base64::STANDARD).ok()?;
        let key = SmallVec::from(key);
        self.hash2path.get(&key).map(|v| v.clone())
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

    pub fn get_location_by_base64(&self, block_id: &str) -> Option<BlockLocation> {
        if let Some(hash2path) = &self.hash2path {
            hash2path.get_location_by_base64(block_id)
        } else {
            None
        }
    }

    pub fn get_zip_archive(&self, zip_filename: &str) -> Option<ZipArchive<MyCloneFileReader>> {
        let zip = self.zip2ziparchive.get(zip_filename);

        zip.map(|zip| zip.archive.clone())
    }

    pub fn get_zip_by_block_id(&self, block_id: &str) -> Option<ZipArchive<MyCloneFileReader>> {
        if let Some(hash2path) = &self.hash2path {
            let zname = hash2path.get_zip_path_by_base64(block_id);
            let zname = zname.map(|n| n.to_string_lossy().to_string());
            let zipa = zname.map(|zname| self.get_zip_archive(&zname)).flatten();
            zipa
        } else {
            for ziparch in self.zip2ziparchive.values() {
                if ziparch.archive.contains_file_name(block_id) {
                    return Some(ziparch.archive.clone());
                }
            }
            None
        }
    }
}

pub struct DB {
    inner: Arc<Mutex<HashToBlocks>>,
    manifest: Manifest,
}

impl DB {
    pub fn new(manifest_bytes: &[u8], use_hash_to_path: bool) -> Result<DB> {
        // let conn = UnQLite::create(file);
        // conn.kv_store("test_key_name", "test_key_value").map_err(|_| eyre!("can't write to database"))?;
        let manifest: Manifest = serde_json::from_slice(manifest_bytes)?;

        let inner = Arc::new(Mutex::new(HashToBlocks::new(use_hash_to_path)));
        let db = DB { inner, manifest };
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
        paths
            .par_iter()
            .try_for_each(|zip_path| self.import_from_zip(zip_path))?;

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

        let zip_len = zip.len();

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

        if false {
            let mut hvec = Vec::new();
            let (sender, receiver) = crossbeam_channel::bounded(zip_len + 1);

            // make workers
            for _t in 0..16 {
                //println!("Make worker {}", t);

                let receiver = receiver.clone(); // clone for this thread

                //let zip_path = zip_path.clone();
                let inner = self.inner.clone();
                let mut zip = zip.clone();
                let arc_zippath = arc_zippath.clone();
                let handler = std::thread::spawn(move || {
                    let mut progress_burst = 0;

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

                                    if let Some(hash2path) = &mut inner.hash2path {
                                        hash2path.hash2path.insert(
                                            hash.into(),
                                            BlockLocation {
                                                zip_path: arc_zippath.clone(),
                                                file_index: file_index as u32,
                                            },
                                        );
                                    }
                                }

                                progress_burst += 1;
                                if progress_burst > 1000 {
                                    //pb.inc(progress_burst);
                                    println!(
                                        "found hash {}/{} in {:?}",
                                        file_index, zip_len, arc_zippath.path
                                    );
                                    progress_burst = 0;
                                }
                            }
                            _ => break,
                        }
                    }
                    //pb.inc(progress_burst);
                });

                hvec.push(handler);
            }

            for i in 0..zip_len {
                sender.send(i).unwrap();
            }
            drop(sender);

            for h in hvec {
                h.join().unwrap();
            }
        }
        {
            config
                .buf_capacity
                .store(32 * 1024, std::sync::atomic::Ordering::Relaxed);
            let mut inner = self.inner.lock().unwrap();
            let wrapper = ZipArchiveWrapper { archive: zip };
            let path_str = arc_zippath.path_str.clone();
            inner.zip2ziparchive.insert(path_str, wrapper);
        }
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
        Ok(())
    }

    pub fn get_block_id_location(&self, block_id: &str) -> Option<BlockLocation> {
        self.inner.lock().unwrap().get_location_by_base64(block_id)
    }

    // pub fn get_zip_path_from_block_id(&self, block_id: &str) -> Option<PathBuf> {
    //     self.inner.lock().unwrap().get_zip_path_by_base64(block_id)
    // }

    // pub fn get_zip_archive(&self, zip_filename: &str) -> Option<ZipArchive<MyCloneFileReader>> {
    //     self.inner.lock().unwrap().get_zip_archive(zip_filename)

    // }

    pub fn get_zip_by_block_id(&self, block_id: &str) -> Option<ZipArchive<MyCloneFileReader>> {
        self.inner.lock().unwrap().get_zip_by_block_id(block_id)
    }

    pub fn get_content_block(&self, block_id: &str) -> Result<Option<Vec<u8>>> {
        let mut output = Vec::new();

        let name_reencoded: String = base64::encode_config(
            &base64::decode(block_id).expect("wrong base64 block_id"),
            base64::URL_SAFE,
        );

        //let mut zip = zip::ZipArchive::new(File::open(filename).unwrap()).unwrap();
        let ziparch = self.get_zip_by_block_id(&name_reencoded);

        if let Some(mut ziparch) = ziparch {
            let mut block = ziparch
                .by_name(&name_reencoded)
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
