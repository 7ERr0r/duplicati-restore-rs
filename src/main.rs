mod blockid;
mod database;
mod stripbom;

use blockid::*;
use clap::Parser;
use database::*;
use eyre::eyre;
use eyre::{Context, Result};
use num_cpus;
use pbr::ProgressBar;
use rayon::prelude::*;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use zip;

use crate::stripbom::StripBom;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// the location of the backup
    #[arg(short, long)]
    backup_dir: String,

    /// a location to restore to
    #[arg(short, long, value_name = "FILE")]
    restore_dir: String,

    #[arg(short, long)]
    cpu_count: Option<usize>,

    #[arg(short, long)]
    progress_bar: Option<bool>,

    /// true if use additional hashmap to speed up hashed name lookup. Increases memory usage.
    #[arg(short, long)]
    hash_to_path: bool,
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    std::thread::spawn(|| {
        let _profiler = dhat::Profiler::new_heap();

        std::thread::sleep(std::time::Duration::from_secs(4 * 60));
    });

    std::thread::sleep(std::time::Duration::from_millis(100));
    let result = run();
    match result {
        Err(err) => {
            println!("err: {:?}", err);
        }
        Ok(_) => {}
    }
}

fn filename_ends_with<P: AsRef<Path>>(path: P, suffix: &str) -> bool {
    path.as_ref()
        .file_name()
        .map(|name| name.to_str())
        .flatten()
        .map(|name| name.ends_with(suffix))
        .unwrap_or(false)
}

fn path_is_dlist_zip<P: AsRef<Path>>(path: P) -> bool {
    filename_ends_with(path, "dlist.zip")
}
fn path_is_dblock_zip<P: AsRef<Path>>(path: P) -> bool {
    filename_ends_with(path, "dblock.zip")
}

/// Open dlist file and parse json inside
fn parse_dlist_file<P: AsRef<Path>>(dlist_path: P) -> Result<Vec<FileEntry>> {
    let dlist_reader = File::open(dlist_path.as_ref())
        .wrap_err_with(|| format!("open {:?}", dlist_path.as_ref()))?;
    let mut dlist_zip = zip::ZipArchive::new(dlist_reader)?;
    let filelist_name = "filelist.json";
    let dlist_file = dlist_zip.by_name(filelist_name)?;
    // let mut dlist_contents = Vec::new();
    // dlist_file.read_to_end(&mut dlist_contents)?;
    let bufrdr = BufReader::with_capacity(32 * 1024, dlist_file);
    let list = parse_dlist_read(bufrdr).wrap_err_with(|| {
        format!(
            "parse_dlist {:?} / {:?}",
            dlist_path.as_ref(),
            filelist_name
        )
    })?;

    Ok(list)
}

/// Open Manifest from zip
fn read_manifest<P: AsRef<Path>>(dlist_path: P) -> Result<Vec<u8>> {
    let manifest_file = File::open(dlist_path.as_ref())?;
    let mut manifest_zip = zip::ZipArchive::new(manifest_file)?;
    let mut manifest_file = manifest_zip.by_name("manifest")?;
    let mut manifest_contents = String::new();
    manifest_file
        .read_to_string(&mut manifest_contents)
        .wrap_err_with(|| format!("read manifest from {:?}", dlist_path.as_ref()))?;
    let manifest_contents = manifest_contents.strip_bom();
    let manifest_contents = manifest_contents.trim();
    Ok(manifest_contents.into())
}

fn run() -> Result<()> {
    let args = CliArgs::parse();
    let backup_dir = args.backup_dir.trim();
    let restore_dir = args.restore_dir.trim();

    // let db_location = Path::join(Path::new(restore_dir), Path::new("index.db"));
    // let db_location = db_location.to_str().unwrap();

    let cpu_count: usize = args.cpu_count.unwrap_or_else(|| num_cpus::get());

    // Set CPU count
    rayon::ThreadPoolBuilder::new()
        .num_threads(cpu_count.min(4))
        .build_global()
        .unwrap();

    // Find newest dlist
    let mut dlist_file_paths: Vec<PathBuf> = fs::read_dir(&backup_dir)?
        .filter_map(Result::ok)
        .filter(|f| path_is_dlist_zip(f.path()))
        //.map(|f| (f.metadata().map(|m| m.modified()), f))
        .map(|f| f.path().to_path_buf())
        .collect();

    dlist_file_paths.sort();

    let newest_dlist = dlist_file_paths
        .last()
        .ok_or_else(|| eyre!("last modified dlist file not found"))?;

    println!(
        "Newest: {:?} appears to be newest dlist, using it.",
        newest_dlist
    );
    println!("Parsing dlist");
    let mut file_entries = parse_dlist_file(newest_dlist)?;

    println!("Parsing manifest");
    let manifest_contents = read_manifest(newest_dlist)?;

    let file_count = file_entries.iter().filter(|f| f.is_file()).count();
    println!("{} files to be restored", file_count);
    let folder_count = file_entries.iter().filter(|f| f.is_folder()).count();
    println!("{} folders to be restored", folder_count);
    println!();

    // Get list of dblocks
    let zip_file_names: Vec<PathBuf> = fs::read_dir(backup_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|f| path_is_dblock_zip(f.path()))
        .map(|f| f.path().to_path_buf())
        .collect();

    println!("Found {} dblocks", zip_file_names.len());

    // Open dblock db connection and build db
    println!();
    println!("Indexing dblocks");
    let dblock_db = DB::new(&manifest_contents, args.hash_to_path)?;
    dblock_db.create_block_id_to_filenames(&zip_file_names)?;

    let show_progress = args.progress_bar.unwrap_or_default();
    println!("Restoring directory structure");
    let mut pb = if show_progress {
        Some(ProgressBar::new(folder_count as u64))
    } else {
        None
    };

    for d in file_entries.iter().filter(|f| f.is_folder()) {
        d.restore_file(&dblock_db, &restore_dir)
            .wrap_err("restoring dir")?;
        if let Some(pb) = &mut pb {
            pb.inc();
        }
    }
    println!();

    println!("Sorting file_entries");

    sort_files_sequentially(&mut file_entries, &dblock_db);

    println!("Restoring files");
    let pb = if show_progress {
        Some(Arc::new(Mutex::new(ProgressBar::new(file_count as u64))))
    } else {
        None
    };
    file_entries
        //.par_iter()
        .iter()
        .filter(|f| f.is_file())
        .par_bridge()
        //.iter()
        .try_for_each(|f| -> Result<()> {
            f.restore_file(&dblock_db, &restore_dir)
                .wrap_err("restoring file entry")?;
            if let Some(pb) = &pb {
                pb.lock().unwrap().inc();
            }
            Ok(())
        })?;

    Ok(())
}

/// Not necessary, but useful to speed up file reads from HDD
/// from like 200 Mbit/s to 700 Mbit/s
fn sort_files_sequentially(file_entries: &mut Vec<FileEntry>, dblock_db: &DB) {
    file_entries.sort_by(|a, b| a.compare(b, dblock_db));
}

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;
