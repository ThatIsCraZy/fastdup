use std::{fs::File, os::{fd::AsRawFd, unix::fs::FileExt}, time::Instant, hint::black_box};
unsafe extern "C" { fn pread(fd: i32, buf: *mut u8, count: usize, offset: i64) -> isize; }
fn read(file: &File, length: usize, offset: u64, spare: bool) -> Vec<u8> {
    let mut bytes = Vec::<u8>::with_capacity(length);
    if spare {
        let mut initialized = 0;
        while initialized < length {
            // Fixture oracle: kernel writes at most the submitted spare capacity;
            // no byte is read until all requested bytes were returned successfully.
            let n = unsafe { pread(file.as_raw_fd(), bytes.as_mut_ptr().add(initialized), length-initialized, (offset+initialized as u64) as i64) };
            assert!(n > 0 && n as usize <= length-initialized);
            initialized += n as usize;
        }
        unsafe { bytes.set_len(initialized); }
    } else {
        bytes.resize(length, 0);
        file.read_exact_at(&mut bytes, offset).unwrap();
    }
    bytes
}
fn main() {
    let file = File::open("/source/fastdup/.artifacts/benchmark-source/Rocky-10.2-x86_64-minimal.iso").unwrap();
    for length in [4096, 65536, 1048576] {
        for k in 0..16 { assert_eq!(read(&file,length,k*length as u64,false),read(&file,length,k*length as u64,true)); }
        let rounds = 2048;
        let mut safe = Vec::new(); let mut spare = Vec::new();
        let measure = |uninitialized| { let start=Instant::now(); for k in 0..rounds { black_box(read(&file,length,(k%16)*length as u64,uninitialized)); } start.elapsed().as_nanos() as f64 / rounds as f64 };
        for sample in 0..11 { if sample % 2 == 0 { safe.push(measure(false)); spare.push(measure(true)); } else { spare.push(measure(true)); safe.push(measure(false)); } }
        safe.sort_by(f64::total_cmp); spare.sort_by(f64::total_cmp);
        println!("read_buffer bytes={length} safe_ns={:.1} spare_ns={:.1} speedup={:.3} samples=11 rounds={rounds}",safe[5],spare[5],safe[5]/spare[5]);
    }
}
