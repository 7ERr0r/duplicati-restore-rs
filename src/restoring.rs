use std::{
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use crate::{
    blockhash::BlockIdHash,
    blockid::{FileEntry, FileType},
    database::DFileDatabase,
};
use eyre::eyre;
use eyre::{Context, Result};

pub fn restore_file(entry: &FileEntry, db: &DFileDatabase, restore_path: &str) -> Result<()> {
    let root_path = Path::new(restore_path);
    let dfile_path = &entry.path[0..];
    let dfile_path = dfile_path.replacen(":\\", "\\", 1);
    let dfile_path = dfile_path.replace("\\", "/");
    let relative_file_path = Path::new(&dfile_path);

    let path = Path::join(root_path, relative_file_path);

    match &entry.file_type {
        FileType::Folder { .. } => {
            fs::create_dir_all(path)?;
        }
        FileType::File { hash, size, .. } => {
            // Small files only have one block
            if entry.block_lists.is_empty() {
                let loc = db.get_block_id_location(hash);
                println!(
                    "restoring file (single) {:?}, index:{:?}",
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
                        "Missing block {:?} for {}",
                        hash,
                        path.to_str().unwrap_or("not utf8?")
                    );
                }
            } else {
                let loc = entry
                    .block_lists
                    .first()
                    .map(|hash| db.get_block_id_location(hash))
                    .flatten();
                println!(
                    "restoring file (blocks) {:?}, index:{:?}",
                    relative_file_path,
                    loc.map(|loc| loc.file_index)
                );
                let mut out_file = File::create(path.clone())?;
                // Each blockid points to a list of blockids
                for (blhi, blh) in entry.block_lists.iter().enumerate() {
                    let blockhashoffset = blhi * db.offset_size();
                    let binary_hashes = db.get_content_block(blh)?;
                    if let Some(binary_hashes) = binary_hashes {
                        for (bi, bhash) in binary_hashes.chunks(db.hash_size()).enumerate() {
                            //let bhash = base64::encode(bhash);
                            let bhash = BlockIdHash::from_bytes(bhash)
                                .ok_or_else(|| eyre!("binary hash len is not 32 bytes"))?;
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
