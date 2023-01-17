use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct RestoreFlags {
    /// the location of the backup
    #[arg(short, long)]
    pub backup_dir: String,

    /// a location to restore to
    #[arg(short, long, value_name = "FILE")]
    pub restore_dir: Option<String>,

    /// 1 thread will save and read files sequentially
    #[arg(short, long, default_value_t = 4)]
    pub threads_rayon: usize,

    /// displays progress bar in CLI
    #[arg(short, long)]
    pub progress_bar: bool,

    /// true if use additional hashmap to speed up hashed name lookup. Increases memory usage.
    #[arg(long)]
    pub hash_to_path: bool,

    /// true to restore windows backup on linux
    #[arg(long)]
    pub replace_backslash_to_slash: Option<bool>,

    /// true to verify without writing files to disk
    #[arg(long)]
    pub verify_only: bool,
}
