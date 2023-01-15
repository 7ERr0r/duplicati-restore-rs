use crate::blockhash::BlockIdHash;

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
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
        matches!(self, FileType::File { .. })
    }

    #[allow(unused)]
    pub fn is_nonzero_file(&self) -> bool {
        match self {
            FileType::File { size, .. } => *size > 0,
            _ => false,
        }
    }

    pub fn is_folder(&self) -> bool {
        matches!(self, FileType::Folder { .. })
    }
}
