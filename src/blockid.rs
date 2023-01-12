use crate::blockhash::BlockIdHash;
use crate::stripbom::strip_bom_from_bufread;
use eyre::eyre;
use eyre::Context;
use eyre::Result;
use serde::Deserialize;
use serde_json;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::prelude::*;

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum FileType {
    File {
        hash: BlockIdHash,
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
    pub path: String,
    #[allow(unused)]
    pub metahash: String,
    #[allow(unused)]
    pub metasize: i64,
    pub file_type: FileType,
    pub block_lists: Vec<BlockIdHash>,
}

impl FileEntry {
    pub(self) fn from_ientry(ientry: &IEntry) -> Result<FileEntry> {
        let path = ientry.path.clone();
        let metahash = ientry.metahash.clone();
        let metasize = ientry.metasize;
        let mut block_lists = Vec::new();

        if let Some(blocks) = &ientry.blocklists {
            for block in blocks {
                block_lists.push(
                    BlockIdHash::from_base64(&block)
                        .ok_or_else(|| eyre!("blocklists BlockIdHash::from_base64 fail"))?,
                );
            }
        };
        let file_type = match ientry.filetype.as_ref() {
            "File" => FileType::File {
                hash: ientry
                    .hash
                    .as_ref()
                    .map(|hash| {
                        BlockIdHash::from_base64(&hash)
                            .ok_or_else(|| eyre!("ientry.hash BlockIdHash::from_base64 fail"))
                    })
                    .ok_or_else(|| eyre!("hash not found"))??,
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

#[allow(unused)]
/// Accepts the dlist as a string (must be read in first)
/// Returns a Vec of FileEntrys
pub fn parse_dlist(dlist: &[u8]) -> Result<Vec<FileEntry>> {
    let file_entries = parse_dlist_read(dlist)?;

    Ok(file_entries)
}

/// Accepts the dlist as a Read trait
/// Returns a Vec of FileEntrys
pub fn parse_dlist_read<'a, R: BufRead>(mut rdr: R) -> Result<Vec<FileEntry>> {
    let mut file_entries = Vec::new();

    strip_bom_from_bufread(&mut rdr)?;

    let iread = IoRead::new(rdr);
    let mut de = Deserializer::new(iread);
    let entry_list: Vec<IEntry> =
        serde_path_to_error::deserialize(&mut de).wrap_err("deserialize entry_list")?;

    for entry in entry_list {
        let entry = FileEntry::from_ientry(&entry).wrap_err("FileEntry::from_ientry")?;
        file_entries.push(entry);
    }

    Ok(file_entries)
}
