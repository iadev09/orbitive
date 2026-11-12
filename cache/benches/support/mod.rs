use std::io::{self, BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn process_cpu_ns() -> u64 {
    clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)
}

pub fn clock_ns(clock: libc::clockid_t) -> u64 {
    let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: time is writable and the clock requires no other arguments.
    let status = unsafe { libc::clock_gettime(clock, &mut time) };
    assert_eq!(status, 0, "clock_gettime: {}", io::Error::last_os_error());
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

pub struct Worker {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl Worker {
    pub fn start(
        mode: &str,
        fleet: &str,
        node: u16,
        options: &[&str],
    ) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--worker", mode, fleet, &node.to_string()])
            .args(options)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap());
        Self { child, input, output }
    }

    pub fn send(
        &mut self,
        command: &str,
    ) {
        writeln!(self.input.as_mut().unwrap(), "{command}").unwrap();
    }

    #[allow(dead_code)] // The read benchmark uses the line-based commands.
    pub fn signal(&mut self) {
        self.input.as_mut().unwrap().write_all(&[1]).unwrap();
    }

    pub fn response(&mut self) -> String {
        let mut response = String::new();
        assert_ne!(
            self.output.read_line(&mut response).unwrap(),
            0,
            "worker closed stdout"
        );
        response.trim_end().to_owned()
    }

    pub fn finish(&mut self) {
        drop(self.input.take());
        assert!(self.child.wait().unwrap().success());
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.wait();
    }
}

pub struct BenchFleet(pub String);

impl BenchFleet {
    pub fn new(run: usize) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let name = format!(
            "cg{:04x}{run:x}{:06x}",
            std::process::id() & 0xffff,
            nonce & 0xff_ffff
        );
        let orbit = Command::new("orbit")
            .args(["fleets", "--names"])
            .output()
            .expect("install orbitive-cli before running this benchmark");
        assert!(orbit.status.success(), "orbit fleets failed");
        assert!(
            !String::from_utf8(orbit.stdout)
                .unwrap()
                .lines()
                .any(|fleet| fleet == name),
            "benchmark fleet name already exists"
        );
        Self(name)
    }
}

impl Drop for BenchFleet {
    fn drop(&mut self) {
        let status = Command::new("orbit")
            .args(["clear", &self.0, "--all", "--yes"])
            .stdout(Stdio::null())
            .status();
        if !status.is_ok_and(|status| status.success()) {
            eprintln!("Failed to clear benchmark fleet {}", self.0);
            assert!(std::thread::panicking(), "benchmark SHM cleanup failed");
        }
    }
}
