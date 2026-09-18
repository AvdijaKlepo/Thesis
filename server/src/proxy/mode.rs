use serde::{Deserialize, Serialize};

/// Selects the transport used for newly accepted proxy connections.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    ThreadPool,
    Async,
}

impl RuntimeMode {
    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "thread_pool" => Some(Self::ThreadPool),
            "async" => Some(Self::Async),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ThreadPool => "thread_pool",
            Self::Async => "async",
        }
    }
}
#[cfg(test)]
#[path = "tests/mode_tests.rs"]
mod tests;
