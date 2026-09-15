use std::{
    fs::File,
    io::{BufWriter, Write},
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::management::{HttpRequest, send_http};

use super::manifest::WorkloadManifest;
use super::runner::RunnerError;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RequestMeasurement {
    pub schema_version: u32,
    pub workload_id: String,
    pub request_index: usize,
    pub worker_id: usize,
    pub method: String,
    pub path: String,
    pub scheduled_offset_us: u64,
    pub started_offset_us: u64,
    pub started_unix_ms: u64,
    pub completed_unix_ms: u64,
    pub latency_us: u64,
    pub transport_success: bool,
    pub http_success: bool,
    pub status_code: Option<u16>,
    pub response_bytes: usize,
    pub response_truncated: bool,
    pub backend_id: Option<String>,
    pub fixture_request_id: Option<String>,
    pub error: Option<String>,
}

pub(crate) struct RequestLog {
    writer: Mutex<RequestLogWriter>,
}

struct RequestLogWriter {
    writer: BufWriter<File>,
    error: Option<String>,
}

impl RequestLog {
    pub(crate) fn create(run_directory: &Path) -> Result<Self, RunnerError> {
        let path = run_directory.join("requests.jsonl");
        let file = File::create(&path).map_err(|error| {
            RunnerError::new(format!("failed to create {}: {error}", path.display()))
        })?;
        Ok(Self {
            writer: Mutex::new(RequestLogWriter {
                writer: BufWriter::new(file),
                error: None,
            }),
        })
    }

    fn record(&self, measurement: &RequestMeasurement) {
        let mut state = self.writer.lock().unwrap();
        if state.error.is_some() {
            return;
        }
        let result = (|| -> Result<(), String> {
            let mut line = serde_json::to_vec(measurement).map_err(|error| error.to_string())?;
            line.push(b'\n');
            state
                .writer
                .write_all(&line)
                .map_err(|error| error.to_string())?;
            state.writer.flush().map_err(|error| error.to_string())
        })();
        if let Err(error) = result {
            state.error = Some(error.to_string());
        }
    }

    pub(crate) fn finish(&self) -> Result<(), RunnerError> {
        let mut state = self.writer.lock().unwrap();
        state.writer.flush()?;
        match &state.error {
            Some(error) => Err(RunnerError::new(format!(
                "failed to stream requests.jsonl: {error}"
            ))),
            None => Ok(()),
        }
    }
}

pub(crate) fn run_workload(
    workload: WorkloadManifest,
    target: SocketAddr,
    default_timeout: Duration,
    run_seed: u64,
    origin: Instant,
    request_log: Arc<RequestLog>,
) -> Vec<RequestMeasurement> {
    let next_request = Arc::new(AtomicUsize::new(0));
    let workload = Arc::new(workload);
    let mut workers = Vec::with_capacity(workload.concurrency);

    for worker_id in 0..workload.concurrency {
        let workload = Arc::clone(&workload);
        let next_request = Arc::clone(&next_request);
        let request_log = Arc::clone(&request_log);
        workers.push(thread::spawn(move || {
            let mut measurements = Vec::new();
            loop {
                let request_index = next_request.fetch_add(1, Ordering::Relaxed);
                if request_index >= workload.requests {
                    break;
                }
                let scheduled_offset = scheduled_offset(&workload, request_index, run_seed);
                sleep_until(origin + scheduled_offset);
                let measurement = execute_request(
                    &workload,
                    request_index,
                    worker_id,
                    target,
                    default_timeout,
                    scheduled_offset,
                    origin,
                );
                request_log.record(&measurement);
                measurements.push(measurement);
            }
            measurements
        }));
    }

    let mut measurements = Vec::with_capacity(workload.requests);
    for worker in workers {
        if let Ok(mut worker_measurements) = worker.join() {
            measurements.append(&mut worker_measurements);
        }
    }
    measurements.sort_by_key(|measurement| measurement.request_index);
    measurements
}

fn scheduled_offset(workload: &WorkloadManifest, request_index: usize, run_seed: u64) -> Duration {
    let paced_us = workload
        .requests_per_second
        .map(|rate| ((request_index as f64 / rate) * 1_000_000.0) as u64)
        .unwrap_or_default();
    let jitter_us = if workload.jitter_ms == 0 {
        0
    } else {
        splitmix64(
            run_seed
                ^ stable_hash(&workload.id)
                ^ (request_index as u64).wrapping_mul(0x9E3779B97F4A7C15),
        ) % (workload.jitter_ms.saturating_mul(1_000).saturating_add(1))
    };
    Duration::from_micros(
        workload
            .start_after_ms
            .saturating_mul(1_000)
            .saturating_add(paced_us)
            .saturating_add(jitter_us),
    )
}

fn sleep_until(deadline: Instant) {
    loop {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        thread::sleep(deadline.duration_since(now));
    }
}

fn execute_request(
    workload: &WorkloadManifest,
    request_index: usize,
    worker_id: usize,
    target: SocketAddr,
    default_timeout: Duration,
    scheduled_offset: Duration,
    origin: Instant,
) -> RequestMeasurement {
    let started_at = Instant::now();
    let started_unix_ms = unix_timestamp_ms();
    let default_host = workload_default_host(target);
    let host = workload.host.as_deref().unwrap_or(&default_host);
    let timeout = workload
        .timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(default_timeout);
    let result = send_http(
        target,
        &HttpRequest {
            method: &workload.method,
            path: &workload.path,
            host,
            headers: &workload.headers,
            body: workload.body.as_bytes(),
            timeout,
            max_response_bytes: workload.max_response_bytes,
        },
    );
    let latency = started_at.elapsed();
    let completed_unix_ms = unix_timestamp_ms();

    match result {
        Ok(response) => RequestMeasurement {
            schema_version: 1,
            workload_id: workload.id.clone(),
            request_index,
            worker_id,
            method: workload.method.clone(),
            path: workload.path.clone(),
            scheduled_offset_us: duration_micros(scheduled_offset),
            started_offset_us: duration_micros(started_at.duration_since(origin)),
            started_unix_ms,
            completed_unix_ms,
            latency_us: duration_micros(latency),
            transport_success: true,
            http_success: (200..400).contains(&response.status_code),
            status_code: Some(response.status_code),
            response_bytes: response.bytes_received,
            response_truncated: response.truncated,
            backend_id: response.header("x-fixture-backend").map(str::to_string),
            fixture_request_id: response.header("x-fixture-request-id").map(str::to_string),
            error: None,
        },
        Err(error) => RequestMeasurement {
            schema_version: 1,
            workload_id: workload.id.clone(),
            request_index,
            worker_id,
            method: workload.method.clone(),
            path: workload.path.clone(),
            scheduled_offset_us: duration_micros(scheduled_offset),
            started_offset_us: duration_micros(started_at.duration_since(origin)),
            started_unix_ms,
            completed_unix_ms,
            latency_us: duration_micros(latency),
            transport_success: false,
            http_success: false,
            status_code: None,
            response_bytes: 0,
            response_truncated: false,
            backend_id: None,
            fixture_request_id: None,
            error: Some(error.to_string()),
        },
    }
}

fn workload_default_host(target: SocketAddr) -> String {
    target.to_string()
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E3779B97F4A7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
    value ^ (value >> 31)
}

pub(crate) fn stable_hash(value: &str) -> u64 {
    value.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, fs};

    fn workload() -> WorkloadManifest {
        WorkloadManifest {
            id: "paced".into(),
            requests: 3,
            concurrency: 1,
            method: "GET".into(),
            path: "/".into(),
            host: None,
            headers: BTreeMap::new(),
            body: String::new(),
            start_after_ms: 20,
            requests_per_second: Some(10.0),
            jitter_ms: 10,
            timeout_ms: None,
            max_response_bytes: 1024,
        }
    }

    #[test]
    fn workload_schedule_is_seeded_and_repeatable() {
        let workload = workload();
        let first = scheduled_offset(&workload, 2, 99);
        let second = scheduled_offset(&workload, 2, 99);
        let another = scheduled_offset(&workload, 2, 100);
        assert_eq!(first, second);
        assert_ne!(first, another);
        assert!(first >= Duration::from_millis(220));
        assert!(first <= Duration::from_millis(230));
    }

    #[test]
    fn stable_hash_does_not_depend_on_process_state() {
        assert_eq!(stable_hash("scenario"), 0xde4217cc72f4d0cf);
    }

    #[test]
    fn request_log_is_visible_before_the_workload_finishes() {
        let directory = std::env::temp_dir().join(format!(
            "webserver-request-log-{}-{}",
            std::process::id(),
            unix_timestamp_ms()
        ));
        fs::create_dir_all(&directory).unwrap();
        let log = RequestLog::create(&directory).unwrap();
        log.record(&RequestMeasurement {
            schema_version: 1,
            workload_id: "paced".into(),
            request_index: 0,
            worker_id: 0,
            method: "GET".into(),
            path: "/".into(),
            scheduled_offset_us: 0,
            started_offset_us: 10,
            started_unix_ms: 100,
            completed_unix_ms: 101,
            latency_us: 50,
            transport_success: true,
            http_success: true,
            status_code: Some(200),
            response_bytes: 12,
            response_truncated: false,
            backend_id: Some("backend-1".into()),
            fixture_request_id: Some("1".into()),
            error: None,
        });

        let contents = fs::read_to_string(directory.join("requests.jsonl")).unwrap();
        let request: serde_json::Value = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(request["workload_id"], "paced");
        assert_eq!(request["latency_us"], 50);
        log.finish().unwrap();
        fs::remove_dir_all(directory).unwrap();
    }
}
