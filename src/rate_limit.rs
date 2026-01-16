use redis::aio::ConnectionManager;
use std::net::IpAddr;
use tracing::warn;

use crate::logging;

/// Check if IP is within rate limit
/// Returns true if allowed, false if rate limited
pub async fn check_rate_limit(
    conn: &mut ConnectionManager,
    ip: &IpAddr,
    limit: u32,
    window_secs: u64,
) -> bool {
    if limit == 0 {
        // Rate limiting disabled
        return true;
    }

    let key = format!("ratelimit:{}", ip);

    // Use INCR + EXPIRE atomically via Lua script
    let script = redis::Script::new(
        r#"
        local current = redis.call('INCR', KEYS[1])
        if current == 1 then
            redis.call('EXPIRE', KEYS[1], ARGV[1])
        end
        return current
        "#,
    );

    match script
        .key(&key)
        .arg(window_secs)
        .invoke_async::<i64>(conn)
        .await
    {
        Ok(count) => {
            if count > limit as i64 {
                warn!(
                    ip = %ip,
                    count = count,
                    limit = limit,
                    "rate_limit_exceeded"
                );
                false
            } else {
                true
            }
        }
        Err(e) => {
            // On Redis error, allow the request (fail open for rate limiting)
            // but log the error
            logging::log_redis_error("rate_limit_check", &e);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would require a real Redis connection
    // Unit tests focus on the logic that doesn't require Redis

    #[test]
    fn test_key_format() {
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let key = format!("ratelimit:{}", ip);
        assert_eq!(key, "ratelimit:192.168.1.1");
    }

    #[test]
    fn test_ipv6_key_format() {
        let ip: IpAddr = "::1".parse().unwrap();
        let key = format!("ratelimit:{}", ip);
        assert_eq!(key, "ratelimit:::1");
    }
}
