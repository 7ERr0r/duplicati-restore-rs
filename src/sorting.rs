use crate::{
    database::DFileDatabase, dfileentry::FileEntry, dfiletype::FileType, ziparchive::BlockLocation,
};
use std::cmp::Ordering;

/// Not necessary, but useful to speed up file reads from HDD
/// from like 200 Mbit/s to 700 Mbit/s
pub fn sort_files_sequentially(file_entries: &mut [FileEntry], dblock_db: &DFileDatabase) {
    file_entries.sort_by(|a, b| compare_fileentry(a, b, dblock_db));
}

/// Optional. Used for sorting.
pub fn get_first_bytes_location(entry: &FileEntry, db: &DFileDatabase) -> Option<BlockLocation> {
    match &entry.file_type {
        FileType::File { hash, .. } => {
            if entry.block_lists.is_empty() {
                db.get_block_id_location(hash)
            } else {
                let first = entry.block_lists.first();

                first.and_then(|bid| db.get_block_id_location(bid))
            }
        }
        _ => None,
    }
}

/// Optional. Used for sorting.
pub fn compare_fileentry(entry_a: &FileEntry, entry_b: &FileEntry, db: &DFileDatabase) -> Ordering {
    let a = get_first_bytes_location(entry_a, db);
    let b = get_first_bytes_location(entry_b, db);

    a.cmp(&b).then_with(|| entry_a.cmp(entry_b))
}
