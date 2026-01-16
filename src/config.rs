use clap::Parser;

/// Mini MySQL Server - opensrv-mysql with Redis backend
#[derive(Parser, Debug, Clone)]
#[command(version, about)]
pub struct Config {
    /// Port to listen on
    #[arg(short, long, default_value = "3306")]
    pub port: u16,

    /// Redis URL
    #[arg(short, long, default_value = "redis://127.0.0.1:6379")]
    pub redis_url: String,

    /// Maximum rows returned for SCAN operations (0 = disabled)
    #[arg(long, default_value = "100")]
    pub scan_limit: usize,

    /// Rate limit: max requests per IP per window
    #[arg(long, default_value = "100")]
    pub rate_limit: u32,

    /// Rate limit window in seconds
    #[arg(long, default_value = "60")]
    pub rate_window: u64,

    /// Allow SCAN operations (table scans without WHERE clause)
    #[arg(long, default_value = "true")]
    pub allow_scan: bool,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

impl Config {
    pub fn parse_args() -> Self {
        Config::parse()
    }
}
