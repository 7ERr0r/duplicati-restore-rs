#[cfg(feature = "dhat-heap")]
pub fn start_dhat_profiler() {
    std::thread::spawn(|| {
        let _profiler = dhat::Profiler::new_heap();

        std::thread::sleep(std::time::Duration::from_secs(10 * 60));
        // save profile after 10 minutes
    });

    std::thread::sleep(std::time::Duration::from_millis(200));
}

/// Does nothing
#[cfg(not(feature = "dhat-heap"))]
pub fn start_dhat_profiler() {}
