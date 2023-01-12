use std::{
    cell::RefCell,
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    blockhash::BlockIdHash,
    blockid::{FileEntry, FileType},
    database::DFileDatabase,
    hexdisplay::HexDisplayBytes,
};
use eyre::eyre;
use eyre::{Context, Result};
use sha2::{Digest, Sha256};

struct RestoreFileContext<'a> {
    db: &'a DFileDatabase,

    entry: &'a FileEntry,
    hash: &'a BlockIdHash,
    size: i64,

    debug_location: bool,
    hasher: RefCell<Option<sha2::Sha256>>,
    path: PathBuf,
    relative_file_path: PathBuf,
}

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
            let hasher = Sha256::new();
            let context = RestoreFileContext {
                entry,
                db,
                debug_location: false,
                hash,
                size: *size,
                hasher: RefCell::new(Some(hasher)),
                path: path.to_path_buf(),
                relative_file_path: relative_file_path.to_path_buf(),
            };

            // Small files only have one block
            if entry.block_lists.is_empty() {
                restore_file_singleblock(&context)?;
            } else {
                restore_file_multiblock(&context)?;
            }

            check_file_hash(&context)?;
        }
        _ => (),
    }
    Ok(())
}

fn restore_file_singleblock<'a>(ctx: &RestoreFileContext<'a>) -> Result<()> {
    if ctx.debug_location {
        let loc = ctx.db.get_block_id_location(ctx.hash);
        println!(
            "restoring file (single) {:?}, index:{:?}",
            ctx.relative_file_path,
            loc.map(|loc| loc.file_index)
        );
    }

    let mut out_file = File::create(ctx.path.clone())?;
    let block = ctx.db.get_content_block(ctx.hash)?;
    if let Some(block) = block {
        out_file
            .write_all(block.as_ref())
            .wrap_err("write single-block file")?;

        {
            let mut hasher = ctx.hasher.borrow_mut();
            if let Some(h) = hasher.as_mut() {
                h.update(block.as_slice());
            }
        }
    } else if ctx.size > 0 {
        println!("Missing block {} for {:?}", ctx.hash, ctx.path,);
    }
    Ok(())
}

fn restore_file_multiblock<'a>(ctx: &RestoreFileContext<'a>) -> Result<()> {
    if ctx.debug_location {
        let loc = ctx
            .entry
            .block_lists
            .first()
            .map(|hash| ctx.db.get_block_id_location(hash))
            .flatten();
        println!(
            "restoring file (blocks) {:?}, index:{:?}",
            ctx.relative_file_path,
            loc.map(|loc| loc.file_index)
        );
    }
    let mut out_file = File::create(ctx.path.clone())?;
    // Each blockid points to a list of blockids
    for (blhi, blh) in ctx.entry.block_lists.iter().enumerate() {
        let blockhashoffset = blhi * ctx.db.offset_size();
        let binary_hashes = ctx
            .db
            .get_content_block(blh)
            .wrap_err_with(|| format!("get main content block: {}", blh))?;
        if let Some(binary_hashes) = binary_hashes {
            for (bi, bhash) in binary_hashes.chunks(ctx.db.hash_size()).enumerate() {
                //let bhash = base64::encode(bhash);
                let bhash = BlockIdHash::from_bytes(bhash)
                    .ok_or_else(|| eyre!("binary hash len is not 32 bytes"))?;
                let block = ctx.db.get_content_block(&bhash).wrap_err_with(|| {
                    format!("get one of content blocks (number {}): {}", bi, blh)
                })?;

                if let Some(block) = block {
                    let offset = (blockhashoffset + bi * ctx.db.block_size()) as u64;
                    out_file
                        .seek(SeekFrom::Start(offset))
                        .wrap_err("seek blockhashoffset + bi * db.block_size()")?;
                    out_file.write_all(&block).wrap_err("write block")?;
                    {
                        let mut hasher = ctx.hasher.borrow_mut();
                        if let Some(h) = hasher.as_mut() {
                            h.update(block.as_slice());
                        }
                    }
                } else {
                    println!("Failed to find block {} for {:?}", bhash, ctx.path,);
                }
            }
        } else {
            println!("Failed to find blocklist {} for {:?}", blh, ctx.path,);
        }
    }

    Ok(())
}

fn check_file_hash<'a>(ctx: &RestoreFileContext<'a>) -> Result<()> {
    let hasher = {
        let mut hasher = None;
        std::mem::swap(&mut hasher, &mut ctx.hasher.borrow_mut());

        hasher
    };
    if let Some(hasher) = hasher {
        let calculated_hash: &[u8] = &hasher.finalize()[..];
        let expected_hash = ctx.hash.hash.as_slice();
        if expected_hash == calculated_hash {
            println!(
                "hash is valid {} == {}",
                HexDisplayBytes(expected_hash),
                HexDisplayBytes(calculated_hash)
            );
        } else {
            Err(eyre!(
                "hash is invalid: expected != calculated, {} != {}",
                HexDisplayBytes(expected_hash),
                HexDisplayBytes(calculated_hash)
            ))?
        }
    }
    Ok(())
}
