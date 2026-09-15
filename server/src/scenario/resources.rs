use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;

use super::{runner::RunnerError, workload::unix_timestamp_ms};

const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Serialize)]
struct ResourceSample {
    schema_version: u32,
    sequence: u64,
    timestamp_unix_ms: u64,
    elapsed_us: u64,
    pid: u32,
    cpu_time_us: Option<u64>,
    resident_memory_bytes: Option<u64>,
    virtual_memory_bytes: Option<u64>,
    error: Option<String>,
}

#[derive(Debug)]
struct ProcessUsage {
    cpu_time_us: u64,
    resident_memory_bytes: u64,
    virtual_memory_bytes: Option<u64>,
}

pub(super) struct ResourceMonitor {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<Result<(), RunnerError>>,
}

impl ResourceMonitor {
    pub(super) fn stop(self) -> Result<(), RunnerError> {
        self.stop.store(true, Ordering::Release);
        self.handle
            .join()
            .map_err(|_| RunnerError::new("resource monitor panicked"))?
    }
}

pub(super) fn start_resource_monitor(
    pid: u32,
    run_directory: &Path,
    origin: Instant,
) -> Result<ResourceMonitor, RunnerError> {
    let file = File::create(run_directory.join("resource-samples.jsonl"))?;
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        let mut writer = BufWriter::new(file);
        let mut sequence = 0_u64;
        loop {
            if sequence > 0 && thread_stop.load(Ordering::Acquire) {
                return Ok(());
            }
            let (usage, error) = match process_usage(pid) {
                Ok(usage) => (Some(usage), None),
                Err(error) => (None, Some(error)),
            };
            let sample = ResourceSample {
                schema_version: 1,
                sequence,
                timestamp_unix_ms: unix_timestamp_ms(),
                elapsed_us: origin.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                pid,
                cpu_time_us: usage.as_ref().map(|usage| usage.cpu_time_us),
                resident_memory_bytes: usage.as_ref().map(|usage| usage.resident_memory_bytes),
                virtual_memory_bytes: usage.as_ref().and_then(|usage| usage.virtual_memory_bytes),
                error,
            };
            serde_json::to_writer(&mut writer, &sample)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            sequence = sequence.saturating_add(1);

            if thread_stop.load(Ordering::Acquire) {
                return Ok(());
            }
            thread::sleep(SAMPLE_INTERVAL);
        }
    });
    Ok(ResourceMonitor { stop, handle })
}

#[cfg(target_os = "windows")]
fn process_usage(pid: u32) -> Result<ProcessUsage, String> {
    windows::process_usage(pid)
}

#[cfg(target_os = "linux")]
fn process_usage(pid: u32) -> Result<ProcessUsage, String> {
    linux::process_usage(pid)
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
fn process_usage(_pid: u32) -> Result<ProcessUsage, String> {
    Err(format!(
        "process resource sampling is unsupported on {}",
        std::env::consts::OS
    ))
}

#[cfg(target_os = "windows")]
mod windows {
    use std::{ffi::c_void, mem::size_of};

    use super::ProcessUsage;

    type Handle = *mut c_void;

    const PROCESS_QUERY_INFORMATION: u32 = 0x0400;
    const PROCESS_VM_READ: u32 = 0x0010;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct FileTime {
        low: u32,
        high: u32,
    }

    #[repr(C)]
    #[allow(non_snake_case)]
    struct ProcessMemoryCounters {
        cb: u32,
        PageFaultCount: u32,
        PeakWorkingSetSize: usize,
        WorkingSetSize: usize,
        QuotaPeakPagedPoolUsage: usize,
        QuotaPagedPoolUsage: usize,
        QuotaPeakNonPagedPoolUsage: usize,
        QuotaNonPagedPoolUsage: usize,
        PagefileUsage: usize,
        PeakPagefileUsage: usize,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn OpenProcess(access: u32, inherit_handle: i32, process_id: u32) -> Handle;
        fn CloseHandle(object: Handle) -> i32;
        fn GetProcessTimes(
            process: Handle,
            creation: *mut FileTime,
            exit: *mut FileTime,
            kernel: *mut FileTime,
            user: *mut FileTime,
        ) -> i32;
    }

    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(
            process: Handle,
            counters: *mut ProcessMemoryCounters,
            size: u32,
        ) -> i32;
    }

    struct OwnedHandle(Handle);

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    pub(super) fn process_usage(pid: u32) -> Result<ProcessUsage, String> {
        let process = unsafe { OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, 0, pid) };
        if process.is_null() {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let process = OwnedHandle(process);
        let mut creation = FileTime::default();
        let mut exit = FileTime::default();
        let mut kernel = FileTime::default();
        let mut user = FileTime::default();
        let times_ok =
            unsafe { GetProcessTimes(process.0, &mut creation, &mut exit, &mut kernel, &mut user) };
        if times_ok == 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }

        let mut memory = ProcessMemoryCounters {
            cb: size_of::<ProcessMemoryCounters>() as u32,
            PageFaultCount: 0,
            PeakWorkingSetSize: 0,
            WorkingSetSize: 0,
            QuotaPeakPagedPoolUsage: 0,
            QuotaPagedPoolUsage: 0,
            QuotaPeakNonPagedPoolUsage: 0,
            QuotaNonPagedPoolUsage: 0,
            PagefileUsage: 0,
            PeakPagefileUsage: 0,
        };
        let memory_ok = unsafe { GetProcessMemoryInfo(process.0, &mut memory, memory.cb) };
        if memory_ok == 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }

        Ok(ProcessUsage {
            cpu_time_us: (file_time_units(kernel) + file_time_units(user)) / 10,
            resident_memory_bytes: memory.WorkingSetSize as u64,
            // Windows' PagefileUsage is the process private commit charge. It is
            // the closest stable counterpart to Linux virtual-memory usage.
            virtual_memory_bytes: Some(memory.PagefileUsage as u64),
        })
    }

    fn file_time_units(value: FileTime) -> u64 {
        (u64::from(value.high) << 32) | u64::from(value.low)
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{ffi::c_long, fs};

    use super::ProcessUsage;

    const SC_CLK_TCK: i32 = 2;
    const SC_PAGESIZE: i32 = 30;

    unsafe extern "C" {
        fn sysconf(name: i32) -> c_long;
    }

    pub(super) fn process_usage(pid: u32) -> Result<ProcessUsage, String> {
        let stat =
            fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|error| error.to_string())?;
        let after_name = stat
            .rsplit_once(')')
            .map(|(_, fields)| fields.trim())
            .ok_or_else(|| "malformed /proc process stat".to_string())?;
        let fields = after_name.split_whitespace().collect::<Vec<_>>();
        // These indices account for fields 1 (pid) and 2 (comm), which were
        // removed above. See proc_pid_stat(5).
        let user_ticks = parse_field(&fields, 11, "user CPU ticks")?;
        let system_ticks = parse_field(&fields, 12, "system CPU ticks")?;
        let virtual_memory_bytes = parse_field(&fields, 20, "virtual memory")?;
        let resident_pages = parse_field(&fields, 21, "resident pages")?;
        let clock_ticks_per_second = unsafe { sysconf(SC_CLK_TCK) };
        let page_size_bytes = unsafe { sysconf(SC_PAGESIZE) };
        if clock_ticks_per_second <= 0 || page_size_bytes <= 0 {
            return Err("sysconf returned invalid process-accounting units".into());
        }

        Ok(ProcessUsage {
            cpu_time_us: user_ticks
                .saturating_add(system_ticks)
                .saturating_mul(1_000_000)
                / clock_ticks_per_second as u64,
            resident_memory_bytes: resident_pages.saturating_mul(page_size_bytes as u64),
            virtual_memory_bytes: Some(virtual_memory_bytes),
        })
    }

    fn parse_field(fields: &[&str], index: usize, name: &str) -> Result<u64, String> {
        fields
            .get(index)
            .ok_or_else(|| format!("missing {name} in /proc process stat"))?
            .parse::<u64>()
            .map_err(|_| format!("invalid {name} in /proc process stat"))
    }
}
#[cfg(all(test, any(target_os = "windows", target_os = "linux")))]
#[path = "resources_tests.rs"]
mod tests;
