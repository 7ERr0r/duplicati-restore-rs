mod blockid;
mod database;
mod stripbom;

use blockid::*;
use chrono::Duration;
use clap::Parser;
use database::*;
use eyre::eyre;
use eyre::{Context, Result};
use num_cpus;
use pbr::ProgressBar;
use rayon::prelude::*;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::Path;
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
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    std::thread::spawn(|| {
        let _profiler = dhat::Profiler::new_heap();

        std::thread::sleep(std::time::Duration::from_secs(60));
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

fn run() -> Result<()> {
    let args = CliArgs::parse();
    let backup_dir = args.backup_dir.trim();
    let restore_dir = args.restore_dir.trim();

    let db_location = Path::join(Path::new(restore_dir), Path::new("index.db"));
    let db_location = db_location.to_str().unwrap();

    let cpu_count: usize = args.cpu_count.unwrap_or_else(|| num_cpus::get());
    println!();

    // Set CPU count
    rayon::ThreadPoolBuilder::new()
        .num_threads(cpu_count)
        .build_global()
        .unwrap();

    // Find newest dlist
    let mut dlist_file_names: Vec<String> = fs::read_dir(&backup_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|f| f.path().to_str().unwrap().ends_with("dlist.zip"))
        .map(|f| f.path().to_str().unwrap().to_string())
        .collect();

    dlist_file_names.sort();

    let dlist = dlist_file_names
        .last()
        .ok_or_else(|| eyre!("last modified dlist file not found"))?;

    println!("{} appears to be newest dlist, using it.", dlist);
    println!("Parsing dlist");

    // Open dlist file
    let file_entries = {
        let dlist_reader = File::open(dlist.clone()).wrap_err_with(|| format!("open {}", dlist))?;
        let mut dlist_zip = zip::ZipArchive::new(dlist_reader)?;
        let mut dlist_file = dlist_zip.by_name("filelist.json")?;
        let mut dlist_contents = Vec::new();
        dlist_file.read_to_end(&mut dlist_contents)?;

        parse_dlist(&dlist_contents).wrap_err_with(|| format!("parse_dlist {}", dlist))?
    };

    println!("Parsing manifest");
    // Open Manifest
    let manifest_contents = {
        let manifest_file = File::open(dlist.clone())?;
        let mut manifest_zip = zip::ZipArchive::new(manifest_file)?;
        let mut manifest_file = manifest_zip.by_name("manifest")?;
        let mut manifest_contents = String::new();
        manifest_file
            .read_to_string(&mut manifest_contents)
            .wrap_err("read manifest")?;
        let manifest_contents = manifest_contents.strip_bom();
        let manifest_contents = manifest_contents.trim();
        manifest_contents.to_owned()
    };

    let file_count = file_entries.iter().filter(|f| f.is_file()).count();
    println!("{} files to be restored", file_count);
    let folder_count = file_entries.iter().filter(|f| f.is_folder()).count();
    println!("{} folders to be restored", folder_count);
    println!();

    // Get list of dblocks
    let zip_file_names: Vec<String> = fs::read_dir(backup_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|f| f.path().to_str().unwrap().to_string())
        .filter(|f| f.ends_with("dblock.zip"))
        .collect();

    println!("Found {} dblocks", zip_file_names.len());

    // Open dblock db connection and build db
    println!();
    println!("Indexing dblocks");
    let dblock_db =
        DB::new(db_location, &manifest_contents)?.create_block_id_to_filenames(&zip_file_names)?;

    println!("Restoring directory structure");
    let mut pb = ProgressBar::new(folder_count as u64);
    for d in file_entries.iter().filter(|f| f.is_folder()) {
        d.restore_file(&dblock_db, &restore_dir);
        pb.inc();
    }
    println!();

    println!("Restoring files");
    let pb = Arc::new(Mutex::new(ProgressBar::new(file_count as u64)));
    file_entries
        .par_iter()
        .filter(|f| f.is_file())
        .for_each(|f| {
            f.restore_file(&dblock_db, &restore_dir);
            pb.lock().unwrap().inc();
        });

    Ok(())
}

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;
