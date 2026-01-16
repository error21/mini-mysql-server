mod auth;
mod backend;
mod config;
mod logging;
mod query;
mod rate_limit;

use backend::{Backend, VERSION};
use config::Config;
use opensrv_mysql::AsyncMysqlIntermediary;
use redis::aio::ConnectionManager;
use std::process;
use tokio::io::split;
use tokio::net::TcpListener;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// Verify Redis connection at startup
async fn verify_redis_connection(conn: &mut ConnectionManager) -> bool {
    match redis::cmd("PING").query_async::<String>(conn).await {
        Ok(response) => {
            if response == "PONG" {
                true
            } else {
                error!("Redis PING returned unexpected response: {}", response);
                false
            }
        }
        Err(e) => {
            error!("Redis PING failed: {}", e);
            false
        }
    }
}

#[tokio::main]
async fn main() {
    let config = Config::parse_args();

    // Setup logging
    let log_level = match config.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(false)
        .finish();

    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("Failed to set up logging: {}", e);
        process::exit(1);
    }

    // Connect to Redis
    info!("Connecting to Redis: {}", config.redis_url);
    let client = match redis::Client::open(config.redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create Redis client: {}", e);
            process::exit(1);
        }
    };

    let mut conn = match ConnectionManager::new(client).await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to connect to Redis: {}", e);
            process::exit(1);
        }
    };

    // Verify Redis connection with PING
    if !verify_redis_connection(&mut conn).await {
        error!("Redis connection verification failed, exiting");
        process::exit(1);
    }
    info!("Redis connected and verified");

    // Log configuration
    info!(
        scan_limit = config.scan_limit,
        rate_limit = config.rate_limit,
        rate_window = config.rate_window,
        allow_scan = config.allow_scan,
        "Configuration loaded"
    );

    let addr = format!("0.0.0.0:{}", config.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind to {}: {}", addr, e);
            process::exit(1);
        }
    };

    info!("Mini MySQL Server listening on {}", addr);
    info!("Version: {} (MySQL 8 compatible)", VERSION);

    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                info!(client = %client_addr, "connection_accepted");

                let redis_conn = conn.clone();
                let config_clone = config.clone();

                tokio::spawn(async move {
                    let (r, w) = split(stream);
                    let backend = Backend::new(redis_conn, config_clone, client_addr);
                    if let Err(e) = AsyncMysqlIntermediary::run_on(backend, r, w).await {
                        error!(client = %client_addr, error = %e, "connection_error");
                    }
                    info!(client = %client_addr, "connection_closed");
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}
