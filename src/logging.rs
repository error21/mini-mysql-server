use std::net::SocketAddr;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Query execution result type
#[derive(Debug, Clone, Copy)]
pub enum QueryResult {
    Success,
    Rejected,
    RateLimited,
    #[allow(dead_code)]
    RedisError,
}

impl std::fmt::Display for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryResult::Success => write!(f, "success"),
            QueryResult::Rejected => write!(f, "rejected"),
            QueryResult::RateLimited => write!(f, "rate_limited"),
            QueryResult::RedisError => write!(f, "redis_error"),
        }
    }
}

/// Structured query log entry
pub struct QueryLog<'a> {
    pub query_type: &'a str,
    pub table: Option<&'a str>,
    pub start_time: Instant,
    pub rows_returned: usize,
    pub client_addr: SocketAddr,
    pub result: QueryResult,
}

impl<'a> QueryLog<'a> {
    pub fn new(query_type: &'a str, client_addr: SocketAddr) -> Self {
        QueryLog {
            query_type,
            table: None,
            start_time: Instant::now(),
            rows_returned: 0,
            client_addr,
            result: QueryResult::Success,
        }
    }

    pub fn with_table(mut self, table: &'a str) -> Self {
        self.table = Some(table);
        self
    }

    pub fn with_rows(mut self, rows: usize) -> Self {
        self.rows_returned = rows;
        self
    }

    pub fn with_result(mut self, result: QueryResult) -> Self {
        self.result = result;
        self
    }

    pub fn log(self) {
        let duration_ms = self.start_time.elapsed().as_millis() as u64;
        let table = self.table.unwrap_or("-");

        match self.result {
            QueryResult::Success => {
                info!(
                    query_type = self.query_type,
                    table = table,
                    duration_ms = duration_ms,
                    rows = self.rows_returned,
                    client = %self.client_addr.ip(),
                    result = %self.result,
                    "query_executed"
                );
            }
            QueryResult::Rejected => {
                warn!(
                    query_type = self.query_type,
                    table = table,
                    duration_ms = duration_ms,
                    client = %self.client_addr.ip(),
                    result = %self.result,
                    "query_rejected"
                );
            }
            QueryResult::RateLimited => {
                warn!(
                    query_type = self.query_type,
                    client = %self.client_addr.ip(),
                    result = %self.result,
                    "rate_limit_exceeded"
                );
            }
            QueryResult::RedisError => {
                error!(
                    query_type = self.query_type,
                    table = table,
                    client = %self.client_addr.ip(),
                    result = %self.result,
                    "redis_error"
                );
            }
        }
    }
}

/// Log raw SQL at DEBUG level only (to prevent sensitive data leakage)
pub fn log_query_debug(query: &str, client_addr: SocketAddr) {
    debug!(
        client = %client_addr.ip(),
        sql = query,
        "raw_query"
    );
}

/// Log SCAN operation warning
pub fn log_scan_warning(table: &str, limit: usize, client_addr: SocketAddr) {
    warn!(
        table = table,
        limit = limit,
        client = %client_addr.ip(),
        "scan_operation_triggered"
    );
}

/// Log Redis connection error
pub fn log_redis_error(operation: &str, err: &dyn std::error::Error) {
    error!(
        operation = operation,
        error = %err,
        "redis_connection_error"
    );
}
