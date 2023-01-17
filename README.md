# Rust Duplicati Restore

[Fast](https://programming-language-benchmarks.vercel.app/rust-vs-csharp) [Duplicati](https://github.com/duplicati/duplicati) [disaster](https://duplicati.readthedocs.io/en/stable/08-disaster-recovery/) [recovery](https://github.com/duplicati/duplicati/tree/master/Duplicati/CommandLine/RecoveryTool).
Processes files across many threads, to maximze restore speed.

## Run

```
Usage: cargo run -- --backup-dir <BACKUP_DIR> --restore-dir <FILE>
```

[More flags here](https://github.com/7ERr0r/duplicati-restore-rs/blob/master/src/flags.rs#L5)

Or download the latest [binary from releases](https://github.com/7ERr0r/duplicati-restore-rs/releases)

## Limitations

* Does not yet support [encrypted backups](https://github.com/duplicati/duplicati/issues/2927) - `.aes` files
* Does not support [remote repositories](https://crates.io/crates/remotefs) yet, I reccomend using rclone to pull down a local copy


## Built With

* [Rust](https://www.rust-lang.org/)
* [`rayon` crate](https://github.com/rayon-rs/rayon)
* [Modified `zip` crate](https://github.com/7ERr0r/zip-duplicati)
* And many more, see [Cargo.toml](Cargo.toml) for full list

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Ben Fisher - His python script included in the Duplicati reposistory inspired
  this project, and this project was roughly based on it.

* Nathan McCarty - Created [Rust-Duplicati-Restore](https://github.com/nmccarty/Rust-Duplicati-Restore) itself

* 7ERr0r - Optimized ZIP reader. Added sha2 verification.
