use crate::database::BlockLocation;
use crate::database::DB;
use crate::stripbom::StripBomBytes;
use base64;
use eyre::Context;
use eyre::Result;
use serde::Deserialize;
use serde_json;
use serde_json::de::SliceRead;
use serde_json::Deserializer;

use eyre::eyre;
use std::cmp::Ordering;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
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

impl FileType {
    pub fn is_file(&self) -> bool {
        match self {
            FileType::File { .. } => true,
            _ => false,
        }
    }

    #[allow(unused)]
    pub fn is_nonzero_file(&self) -> bool {
        match self {
            FileType::File { size, .. } => *size > 0,
            _ => false,
        }
    }

    pub fn is_folder(&self) -> bool {
        match self {
            FileType::Folder { .. } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
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
    pub(self) fn from_ientry(ientry: &IEntry) -> Result<FileEntry> {
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
                hash: ientry.hash.clone().ok_or_else(|| eyre!("hash not found"))?,
                size: ientry.size.clone().ok_or_else(|| eyre!("size not found"))?,
                time: ientry.time.clone().ok_or_else(|| eyre!("time not found"))?,
            },
            "Folder" => FileType::Folder {
                metablockhash: ientry
                    .metablockhash
                    .clone()
                    .ok_or_else(|| eyre!("metablockhash not found"))?,
            },
            _ => FileType::SymLink,
        };

        Ok(FileEntry {
            path,
            metahash,
            metasize,
            file_type,
            block_lists,
        })
    }

    pub fn is_file(&self) -> bool {
        self.file_type.is_file()
    }

    pub fn is_folder(&self) -> bool {
        self.file_type.is_folder()
    }

    /// Optional. Used for sorting.
    pub fn get_first_bytes_location(&self, db: &DB) -> Option<BlockLocation> {
        match &self.file_type {
            FileType::File { hash, .. } => {
                if self.block_lists.is_empty() {
                    db.get_block_id_location(&hash)
                } else {
                    let first = self.block_lists.first();

                    first.map(|bid| db.get_block_id_location(bid)).flatten()
                }
            }
            _ => None,
        }
    }

    /// Optional. Used for sorting.
    pub fn compare(&self, othr: &FileEntry, db: &DB) -> Ordering {
        let a = self.get_first_bytes_location(db);
        let b = othr.get_first_bytes_location(db);

        a.cmp(&b)
    }

    pub fn restore_file(&self, db: &DB, restore_path: &str) -> Result<()> {
        let root_path = Path::new(restore_path);
        let dfile_path = &self.path[0..];
        let dfile_path = dfile_path.replacen(":\\", "\\", 1);
        let dfile_path = dfile_path.replace("\\", "/");
        let relative_file_path = Path::new(&dfile_path);

        let path = Path::join(root_path, relative_file_path);

        match &self.file_type {
            FileType::Folder { .. } => {
                fs::create_dir_all(path)?;
            }
            FileType::File { hash, size, .. } => {
                // Small files only have one block
                if self.block_lists.is_empty() {
                    let loc = db.get_block_id_location(hash);
                    println!(
                        "restoring file {:?}, index:{:?}",
                        relative_file_path,
                        loc.map(|loc| loc.file_index)
                    );

                    let mut out_file = File::create(path.clone())?;
                    let block = db.get_content_block(hash)?;
                    if let Some(block) = block {
                        out_file
                            .write_all(block.as_ref())
                            .wrap_err("write single-block file")?;
                    } else if *size > 0 {
                        println!(
                            "Missing block {} for {}",
                            hash,
                            path.to_str().unwrap_or("not utf8?")
                        );
                    }
                } else {
                    let loc = self
                        .block_lists
                        .first()
                        .map(|hash| db.get_block_id_location(hash))
                        .flatten();
                    println!(
                        "restoring file {:?}, index:{:?}",
                        relative_file_path,
                        loc.map(|loc| loc.file_index)
                    );
                    let mut out_file = File::create(path.clone())?;
                    // Each blockid points to a list of blockids
                    for (blhi, blh) in self.block_lists.iter().enumerate() {
                        let blockhashoffset = blhi * db.offset_size();
                        let binary_hashes = db.get_content_block(blh)?;
                        if let Some(binary_hashes) = binary_hashes {
                            for (bi, bhash) in binary_hashes.chunks(db.hash_size()).enumerate() {
                                let bhash = base64::encode(bhash);
                                let block = db.get_content_block(&bhash)?;

                                if let Some(block) = block {
                                    out_file
                                        .seek(SeekFrom::Start(
                                            (blockhashoffset + bi * db.block_size()) as u64,
                                        ))
                                        .wrap_err("seek blockhashoffset + bi * db.block_size()")?;
                                    out_file.write_all(&block).wrap_err("write block")?;
                                } else {
                                    println!(
                                        "Failed to find block {} for {}",
                                        bhash,
                                        path.to_str().unwrap_or("not utf8?")
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
        Ok(())
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

    let read = SliceRead::new(dlist.strip_bom());
    let mut de = Deserializer::new(read);
    let entry_list: Vec<IEntry> =
        serde_path_to_error::deserialize(&mut de).wrap_err("deserialize entry_list")?;

    for entry in entry_list {
        let entry = FileEntry::from_ientry(&entry).wrap_err("FileEntry::from_ientry")?;
        file_entries.push(entry);
    }

    Ok(file_entries)
}
