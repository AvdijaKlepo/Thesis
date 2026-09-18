use super::*;

#[test]
fn names_are_stable() {
    assert_eq!(
        RuntimeMode::from_str_name("thread_pool"),
        Some(RuntimeMode::ThreadPool)
    );
    assert_eq!(
        RuntimeMode::from_str_name("async"),
        Some(RuntimeMode::Async)
    );
    assert_eq!(RuntimeMode::from_str_name("unknown"), None);
    assert_eq!(RuntimeMode::ThreadPool.as_str(), "thread_pool");
    assert_eq!(RuntimeMode::Async.as_str(), "async");
}
