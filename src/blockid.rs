use crate::database::DB;
use crate::stripbom::StripBomBytes;
use base64;
use eyre::Context;
use eyre::Result;
use serde::Deserialize;
use serde_json;
use serde_json::de::SliceRead;
use serde_json::Deserializer;

use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

#[derive(Debug)]
pub enum FileType {
    File {
        hash: String,
        size: i64,
        time: String,
    },
    Folder {
        metablockhash: String,
    },
    SymLink,
}

#[derive(Debug)]
pub struct FileEntry {
    path: String,
    #[allow(unused)]
    metahash: String,
    #[allow(unused)]
    metasize: i64,
    file_type: FileType,
    block_lists: Vec<String>,
}

impl FileEntry {
    pub(self) fn from_ientry(ientry: &IEntry) -> FileEntry {
        let path = ientry.path.clone();
        let metahash = ientry.metahash.clone();
        let metasize = ientry.metasize;
        let block_lists = if let Some(blocks) = &ientry.blocklists {
            blocks.clone()
        } else {
            Vec::new()
        };
        let file_type = match ientry.filetype.as_ref() {
            "File" => FileType::File {
                hash: ientry.hash.clone().unwrap(),
                size: ientry.size.unwrap(),
                time: ientry.time.clone().unwrap(),
            },
            "Folder" => FileType::Folder {
                metablockhash: ientry.metablockhash.clone().unwrap(),
            },
            _ => FileType::SymLink,
        };

        FileEntry {
            path,
            metahash,
            metasize,
            file_type,
            block_lists,
        }
    }

    pub fn is_file(&self) -> bool {
        match self.file_type {
            FileType::File { .. } => true,
            _ => false,
        }
    }

    pub fn is_folder(&self) -> bool {
        match self.file_type {
            FileType::Folder { .. } => true,
            _ => false,
        }
    }

    pub fn restore_file(&self, db: &DB, restore_path: &str) {
        let root_path = Path::new(restore_path);
        let file_path = Path::new(&self.path[1..]);
        let path = Path::join(root_path, file_path);

        match &self.file_type {
            FileType::Folder { .. } => {
                fs::create_dir_all(path).unwrap();
            }
            FileType::File { hash, size, .. } => {
                // Small files only have one block
                if self.block_lists.is_empty() {
                    let mut file = File::create(path.clone()).unwrap();
                    let block = db.get_content_block(hash);
                    if let Some(block) = block {
                        file.write_all(block.as_ref()).unwrap();
                    } else if *size > 0 {
                        println!("Missing block {} for {}", hash, path.to_str().unwrap());
                    }
                } else {
                    let mut file = File::create(path.clone()).unwrap();
                    // Each blockid points to a list of blockids
                    for (blhi, blh) in self.block_lists.iter().enumerate() {
                        let blockhashoffset = blhi * db.offset_size();
                        let binary_hashes = db.get_content_block(blh);
                        if let Some(binary_hashes) = binary_hashes {
                            for (bi, hash) in binary_hashes.chunks(db.hash_size()).enumerate() {
                                let hash = base64::encode(hash);
                                let block = db.get_content_block(&hash);

                                if let Some(block) = block {
                                    file.seek(SeekFrom::Start(
                                        (blockhashoffset + bi * db.block_size()) as u64,
                                    ))
                                    .unwrap();
                                    file.write_all(&block).unwrap();
                                } else {
                                    println!(
                                        "Failed to find block {} for {}",
                                        hash,
                                        path.to_str().unwrap()
                                    );
                                }
                            }
                        } else {
                            println!(
                                "Failed to find blocklist {} for {}",
                                blh,
                                path.to_str().unwrap()
                            );
                        }
                    }
                }
            }
            _ => (),
        }
    }
}

#[derive(Deserialize)]
pub(self) struct IEntry {
    #[serde(rename = "type")]
    pub(self) filetype: String,
    pub(self) path: String,
    pub(self) hash: Option<String>,
    pub(self) size: Option<i64>,

    pub(self) metablockhash: Option<String>,
    pub(self) metahash: String,
    pub(self) metasize: i64,

    pub(self) time: Option<String>,
    pub(self) blocklists: Option<Vec<String>>,
}

/// Accepts the dlist as a string (must be read in first)
/// Returns a Vec of FileEntrys
pub fn parse_dlist(dlist: &[u8]) -> Result<Vec<FileEntry>> {
    let mut file_entries = Vec::new();
    // {
    //     let debug_dlist = {
    //         let mut dfile = File::open("debug_dlist_pretty.json")?;
    //         let mut contents = String::new();
    //         dfile.read_to_string(&mut contents)?;
    //         contents
    //     };
    //     let read = SliceRead::new(debug_dlist.as_bytes());
    //     let mut de = Deserializer::new(read);
    //     let obj: serde_json::Value = Deserialize::deserialize(&mut de).wrap_err("from_slice debug")?;
    //     //let obj: serde_json::Value = serde_json::from_slice(dlist)

    //     let mut writer = Vec::new();
    //     let mut ser = Serializer::with_formatter(&mut writer, PrettyFormatter::new());
    //     let pretty = obj.serialize(&mut ser).wrap_err("to_string_pretty debug")?;
    //     let mut file = File::create("debug_dlist.json")?;
    //     file.write_all(&writer)?;
    // }

    // {
    //     let mut file = File::create("debug_dlist.json")?;
    //     file.write_all(&dlist)?;
    // }

    let read = SliceRead::new(dlist.strip_bom());
    let mut de = Deserializer::new(read);
    let entry_list: Vec<IEntry> =
        serde_path_to_error::deserialize(&mut de).wrap_err("deserialize entry_list")?;

    for entry in entry_list {
        file_entries.push(FileEntry::from_ientry(&entry));
    }

    Ok(file_entries)
}

