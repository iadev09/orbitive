#[cfg(unix)]
#[path = "support/shm.rs"]
mod shm;

fn main() {
    #[cfg(unix)]
    shm::main();
    #[cfg(not(unix))]
    eprintln!("SHM quota benchmarks require Unix.");
}
