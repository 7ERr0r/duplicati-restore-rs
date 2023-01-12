use crate::{
    blockhash::BlockIdHash,
    blockid::{FileEntry, FileType},
    database::DFileDatabase,
    hexdisplay::HexDisplayBytes,
};
use eyre::eyre;
use eyre::{Context, Result};
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

struct RestoreFileContext<'a> {
    db: &'a DFileDatabase,

    entry: &'a FileEntry,
    hash: &'a BlockIdHash,
    size: i64,

    debug_location: bool,
    strict_block_size: bool,
    hasher: RefCell<Option<sha2::Sha256>>,
    absolute_path: &'a Path,
    relative_file_path: &'a Path,
}

pub struct RestoreParams<'a> {
    pub db: &'a DFileDatabase,
    pub restore_path: &'a str,
    pub replace_backslash_to_slash: bool,
}

pub fn restore_file(entry: &FileEntry, params: &RestoreParams<'_>) -> Result<()> {
    let root_path = Path::new(params.restore_path);
    let dfile_path = &entry.path[0..];
    let mut dfile_path = dfile_path.replacen(":\\", "\\", 1);
    if params.replace_backslash_to_slash {
        dfile_path = dfile_path.replace('\\', "/");
    }
    let relative_file_path = Path::new(&dfile_path);

    let path = Path::join(root_path, relative_file_path);

    match &entry.file_type {
        FileType::Folder { .. } => {
            fs::create_dir_all(path)?;
        }
        FileType::File { hash, size, .. } => {
            let hasher = if *size > 0 { Some(Sha256::new()) } else { None };
            let context = RestoreFileContext {
                entry,
                db: params.db,
                debug_location: false,
                strict_block_size: true,
                hash,
                size: *size,
                hasher: RefCell::new(hasher),
                absolute_path: &path,
                relative_file_path,
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

fn restore_file_singleblock(ctx: &RestoreFileContext<'_>) -> Result<()> {
    if ctx.debug_location {
        let loc = ctx.db.get_block_id_location(ctx.hash);
        println!(
            "restoring file (single) {:?}, index:{:?}",
            ctx.relative_file_path,
            loc.map(|loc| loc.file_index)
        );
    }

    let mut out_file = File::create(ctx.absolute_path)?;
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
        Err(eyre!(
            "Missing block {} for {:?}",
            ctx.hash,
            ctx.absolute_path
        ))?;
    }
    Ok(())
}

fn restore_file_multiblock(ctx: &RestoreFileContext<'_>) -> Result<()> {
    if ctx.debug_location {
        let loc = ctx
            .entry
            .block_lists
            .first()
            .and_then(|hash| ctx.db.get_block_id_location(hash));
        println!(
            "restoring file (blocks) {:?}, index:{:?}",
            ctx.relative_file_path,
            loc.map(|loc| loc.file_index)
        );
    }
    let mut out_file = File::create(ctx.absolute_path)?;
    // Each blockid points to a list of blockids
    for (blhi, blh) in ctx.entry.block_lists.iter().enumerate() {
        let blockhashoffset = blhi * ctx.db.offset_size();
        let binary_hashes = ctx
            .db
            .get_content_block(blh)
            .wrap_err_with(|| format!("get main content block: {}", blh))?;
        if let Some(binary_hashes) = binary_hashes {
            let mut last_block_size = None;
            for (bi, bhash) in binary_hashes.chunks(ctx.db.hash_size()).enumerate() {
                //let bhash = base64::encode(bhash);
                let bhash = BlockIdHash::from_bytes(bhash)
                    .ok_or_else(|| eyre!("binary hash len is not 32 bytes"))?;
                let block = ctx.db.get_content_block(&bhash).wrap_err_with(|| {
                    format!("get one of content blocks (number {}): {}", bi, blh)
                })?;

                if let Some(block) = block {
                    let full_block = ctx.db.block_size();

                    let offset = (blockhashoffset + bi * full_block) as u64;
                    out_file
                        .seek(SeekFrom::Start(offset))
                        .wrap_err("seek blockhashoffset + bi * full_block")?;
                    out_file.write_all(&block).wrap_err("write block")?;
                    {
                        let mut hasher = ctx.hasher.borrow_mut();
                        if let Some(h) = hasher.as_mut() {
                            h.update(block.as_slice());
                        }
                    }
                    if ctx.strict_block_size {
                        if let Some(last) = last_block_size {
                            if last != full_block {
                                Err(eyre!(
                                    "last block size != full_block, {} != {}",
                                    last,
                                    full_block
                                ))?;
                            }
                        }
                        last_block_size = Some(block.len());
                    }
                } else {
                    Err(eyre!(
                        "Failed to find block {} for {:?}",
                        bhash,
                        ctx.absolute_path
                    ))?;
                }
            }
        } else {
            Err(eyre!(
                "Failed to find blocklist {} for {:?}",
                blh,
                ctx.absolute_path,
            ))?;
        }
    }

    Ok(())
}

fn check_file_hash(ctx: &RestoreFileContext<'_>) -> Result<()> {
    if ctx.size == 0 {
        return Ok(());
    }
    let hasher = {
        let mut hasher = None;
        std::mem::swap(&mut hasher, &mut ctx.hasher.borrow_mut());

        hasher
    };
    if let Some(hasher) = hasher {
        let calculated_hash: &[u8] = &hasher.finalize()[..];
        let expected_hash = ctx.hash.hash.as_slice();
        if expected_hash == calculated_hash {
            let debug_hash = false;
            if debug_hash {
                println!(
                    "hash is valid {} == {}",
                    HexDisplayBytes(expected_hash),
                    HexDisplayBytes(calculated_hash)
                );
            }
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
