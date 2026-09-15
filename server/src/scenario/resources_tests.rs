use super::*;

#[test]
fn samples_the_current_process() {
    let usage = process_usage(std::process::id()).unwrap();
    assert!(usage.resident_memory_bytes > 0);
}
