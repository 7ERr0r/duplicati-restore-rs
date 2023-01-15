use crate::{
    blockhash::BlockIdHash, database::DFileDatabase, dfileentry::FileEntry, dfiletype::FileType,
    hexdisplay::HexDisplayBytes,
};
use eyre::eyre;
use eyre::{Context, Result};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::{
    cell::RefCell,
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

#[derive(Clone)]
pub struct RestoreContext {
    pub block_buffer: RefCell<Vec<u8>>,
    pub block_hashes_buffer: RefCell<Vec<u8>>,
}

impl RestoreContext {
    pub fn new() -> Self {
        Self {
            block_buffer: RefCell::new(Vec::with_capacity(8 * 1024)),
            block_hashes_buffer: RefCell::new(Vec::with_capacity(8 * 1024)),
        }
    }
}

struct RestoreFileContext<'a> {
    restore_context: &'a RestoreContext,
    db: &'a DFileDatabase,

    entry: &'a FileEntry,
    hash: &'a BlockIdHash,
    size: i64,

    debug_location: bool,
    strict_block_size: bool,
    hasher: RefCell<Option<sha2::Sha256>>,

    /// None if only verifying
    absolute_path: Option<&'a PathBuf>,
    /// None if only verifying
    relative_file_path: Option<&'a PathBuf>,

    out_file: RefCell<Option<File>>,
}

pub struct RestoreSummary {
    pub file_count: usize,
    pub folder_count: usize,
    pub total_bytes: u64,
    pub predicted_bytes: u64,
}

pub struct RestoreParams<'a> {
    pub db: Arc<DFileDatabase>,
    pub restore_path: Option<&'a str>,
    pub replace_backslash_to_slash: bool,
    pub summary: RestoreSummary,
}
/// Returns Some(absolute, relative)
pub fn calculate_path(entry: &FileEntry, params: &RestoreParams<'_>) -> Option<(PathBuf, PathBuf)> {
    if let Some(restore_path) = &params.restore_path {
        let root_path = Path::new(restore_path);
        let dfile_path = &entry.path[0..];
        let mut dfile_path = dfile_path.replacen(":\\", "\\", 1);
        if params.replace_backslash_to_slash {
            dfile_path = dfile_path.replace('\\', "/");
        }
        let relative_file_path = PathBuf::from(&dfile_path);

        let path = Path::join(root_path, &relative_file_path);
        Some((path, relative_file_path))
    } else {
        None
    }
}

pub fn restore_entry(
    entry: &FileEntry,
    params: &RestoreParams<'_>,
    restore_context: &RestoreContext,
) -> Result<()> {
    let paths = calculate_path(entry, params);
    let absolute_path = paths.as_ref().map(|v| &v.0);
    let relative_file_path = paths.as_ref().map(|v| &v.1);

    match &entry.file_type {
        FileType::Folder { .. } => {
            if let Some(path) = absolute_path {
                fs::create_dir_all(path)?;
            }
        }
        FileType::File { hash, size, .. } => {
            restore_file(
                params,
                restore_context,
                hash,
                *size,
                absolute_path,
                relative_file_path,
                entry,
            )?;
        }
        _ => (),
    }
    Ok(())
}
fn restore_file(
    params: &RestoreParams<'_>,
    restore_context: &RestoreContext,
    hash: &BlockIdHash,
    size: i64,
    absolute_path: Option<&PathBuf>,
    relative_file_path: Option<&PathBuf>,
    entry: &FileEntry,
) -> Result<()> {
    let hasher = if size > 0 { Some(Sha256::new()) } else { None };
    let out_file = if let Some(path) = &absolute_path {
        Some(File::create(path)?)
    } else {
        None
    };
    let context = RestoreFileContext {
        restore_context,
        entry,
        db: &params.db,
        debug_location: false,
        strict_block_size: true,
        hash,
        size,
        hasher: RefCell::new(hasher),
        absolute_path,
        relative_file_path,
        out_file: RefCell::new(out_file),
    };

    // Small files only have one block
    if entry.block_lists.is_empty() {
        restore_file_singleblock(&context)?;
    } else {
        restore_file_multiblock(&context)?;
    }

    check_file_hash(&context)?;
    Ok(())
}

fn restore_file_singleblock(ctx: &RestoreFileContext<'_>) -> Result<()> {
    debug_block_restore_maybe(ctx, true);

    if ctx.size <= 0 {
        return Ok(());
    }

    let buf = &mut ctx.restore_context.block_buffer.borrow_mut();
    buf.clear();
    let block = ctx.db.get_content_block(ctx.hash, buf)?;
    let _len = block.ok_or(|| eyre!("Missing block {} for {:?}", ctx.hash, ctx.absolute_path));

    if let Some(out_file) = ctx.out_file.borrow_mut().as_mut() {
        out_file
            .write_all(buf.as_slice())
            .wrap_err("write single-block file")?;
    }
    update_hasher_maybe(ctx, buf);

    Ok(())
}
fn debug_block_restore_maybe(ctx: &RestoreFileContext<'_>, is_multi: bool) {
    if !ctx.debug_location {
        return;
    }

    let multi_or_single = if is_multi { "multi" } else { "single" };
    let hash = if is_multi {
        Some(ctx.hash)
    } else {
        ctx.entry.block_lists.first()
    };
    let loc = hash.and_then(|hash| ctx.db.get_block_id_location(hash));
    println!(
        "restoring file ({}) {:?}, index:{:?}",
        multi_or_single,
        ctx.relative_file_path,
        loc.map(|loc| loc.file_index)
    );
}

fn restore_file_multiblock(ctx: &RestoreFileContext<'_>) -> Result<()> {
    debug_block_restore_maybe(ctx, true);

    // Each blockid points to a list of blockids
    for (main_hash_index, main_hash) in ctx.entry.block_lists.iter().enumerate() {
        restore_file_multiblock_main(ctx, main_hash_index, main_hash)?;
    }

    Ok(())
}

fn update_hasher_maybe(ctx: &RestoreFileContext<'_>, buf: &[u8]) {
    let mut hasher = ctx.hasher.borrow_mut();
    if let Some(h) = hasher.as_mut() {
        h.update(buf);
    }
}

fn restore_file_multiblock_block(
    ctx: &RestoreFileContext<'_>,
    block_index: usize,
    block_hash: &[u8],
    blockhashoffset: usize,
    last_block_size: &mut Option<usize>,
) -> Result<()> {
    //let bhash = base64::encode(bhash);
    let block_hash = BlockIdHash::from_bytes(block_hash)
        .ok_or_else(|| eyre!("binary hash len is not 32 bytes"))?;
    let buf = &mut ctx.restore_context.block_buffer.borrow_mut();
    buf.clear();
    let block = ctx
        .db
        .get_content_block(&block_hash, buf)
        .wrap_err_with(|| {
            format!(
                "get one of content blocks (number {}): {}",
                block_index, block_hash
            )
        })?;

    let _block_len = block.ok_or_else(|| {
        eyre!(
            "Failed to find block {} for {:?}",
            block_hash,
            ctx.absolute_path
        )
    })?;

    if let Some(out_file) = ctx.out_file.borrow_mut().as_mut() {
        let full_block = ctx.db.block_size();
        let offset = (blockhashoffset + block_index * full_block) as u64;
        out_file
            .seek(SeekFrom::Start(offset))
            .wrap_err("seek blockhashoffset + bi * full_block")?;
        out_file
            .write_all(buf.as_slice())
            .wrap_err("write (multi) block")?;
    }
    update_hasher_maybe(ctx, buf);
    check_strict_block(ctx, buf, last_block_size)?;

    Ok(())
}

fn check_strict_block(
    ctx: &RestoreFileContext<'_>,
    buf: &[u8],
    last_block_size: &mut Option<usize>,
) -> Result<()> {
    if !ctx.strict_block_size {
        return Ok(());
    }
    if let Some(last) = last_block_size {
        let full_block = ctx.db.block_size();
        if *last != full_block {
            Err(eyre!(
                "last block size != full_block, {} != {}",
                last,
                full_block
            ))?;
        }
    }
    *last_block_size = Some(buf.len());

    Ok(())
}

fn restore_file_multiblock_main(
    ctx: &RestoreFileContext<'_>,
    main_hash_index: usize,
    main_hash: &BlockIdHash,
) -> Result<()> {
    let blockhashoffset = main_hash_index * ctx.db.offset_size();

    let hashes_buf: &mut Vec<u8> = &mut ctx.restore_context.block_hashes_buffer.borrow_mut();
    let binary_hashes_len = {
        hashes_buf.clear();
        ctx.db
            .get_content_block(main_hash, hashes_buf)
            .wrap_err_with(|| format!("get main content block: {}", main_hash))?
    };

    let _len = binary_hashes_len.ok_or_else(|| {
        eyre!(
            "Failed to find blocklist {} for {:?}",
            main_hash,
            ctx.absolute_path,
        )
    })?;

    let mut last_block_size = None;
    for (bi, bhash) in hashes_buf.chunks(ctx.db.hash_size()).enumerate() {
        restore_file_multiblock_block(ctx, bi, bhash, blockhashoffset, &mut last_block_size)?;
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
    if hasher.is_none() {
        return Ok(());
    }
    let hasher = hasher.unwrap();

    let calculated_hash: &[u8] = &hasher.finalize()[..];
    let expected_hash = ctx.hash.hash.as_slice();
    if expected_hash != calculated_hash {
        return Err(eyre!(
            "hash is invalid: expected != calculated, {} != {}",
            HexDisplayBytes(expected_hash),
            HexDisplayBytes(calculated_hash)
        ));
    }
    let debug_hash = false;
    if debug_hash {
        println!(
            "hash is valid {} == {}",
            HexDisplayBytes(expected_hash),
            HexDisplayBytes(calculated_hash)
        );
    }

    Ok(())
}
