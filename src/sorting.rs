use std::cmp::Ordering;

use crate::{
    blockid::{FileEntry, FileType},
    database::DFileDatabase,
    ziparchive::BlockLocation,
};

/// Optional. Used for sorting.
pub fn get_first_bytes_location(entry: &FileEntry, db: &DFileDatabase) -> Option<BlockLocation> {
    match &entry.file_type {
        FileType::File { hash, .. } => {
            if entry.block_lists.is_empty() {
                db.get_block_id_location(hash)
            } else {
                let first = entry.block_lists.first();

                first.map(|bid| db.get_block_id_location(bid)).flatten()
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
