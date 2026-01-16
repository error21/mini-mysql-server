use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};

use crate::logging;

/// Auth token data stored in Redis as JSON
/// Key: auth:{token}
/// TTL: 30-120 seconds (configurable)
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthToken {
    pub user_id: String,
    pub facility: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>, // JSON escape hatch for future fields
}

/// Result of qr_verify() function
#[derive(Debug)]
pub struct QrVerifyResult {
    pub verified: bool,
    pub user_id: Option<String>,
    pub facility: Option<String>,
    pub verified_at: Option<String>,
    pub data: Option<String>,
}

impl QrVerifyResult {
    /// Create empty result (token not found or invalid)
    pub fn not_found() -> Self {
        QrVerifyResult {
            verified: false,
            user_id: None,
            facility: None,
            verified_at: None,
            data: None,
        }
    }

    /// Create success result from auth token
    pub fn from_token(token: AuthToken) -> Self {
        let now: DateTime<Utc> = Utc::now();
        QrVerifyResult {
            verified: true,
            user_id: Some(token.user_id),
            facility: Some(token.facility),
            verified_at: Some(now.format("%Y-%m-%d %H:%M:%S").to_string()),
            data: token.data.map(|v| v.to_string()),
        }
    }
}

/// Verify QR token using atomic GETDEL
/// Token is consumed on first successful verification
pub async fn verify_token(conn: &mut ConnectionManager, token: &str) -> QrVerifyResult {
    let key = format!("auth:{}", token);

    // Use GETDEL for atomic read-and-delete
    let result: Result<Option<String>, redis::RedisError> =
        redis::cmd("GETDEL").arg(&key).query_async(conn).await;

    match result {
        Ok(Some(json)) => {
            // Parse JSON payload
            match serde_json::from_str::<AuthToken>(&json) {
                Ok(auth_token) => QrVerifyResult::from_token(auth_token),
                Err(e) => {
                    logging::log_redis_error("auth_token_parse", &e);
                    QrVerifyResult::not_found()
                }
            }
        }
        Ok(None) => {
            // Token not found or already consumed
            QrVerifyResult::not_found()
        }
        Err(e) => {
            logging::log_redis_error("getdel_auth_token", &e);
            QrVerifyResult::not_found()
        }
    }
}

/// Column definitions for qr_verify() result
pub fn qr_verify_columns() -> Vec<(&'static str, &'static str)> {
    vec![
        ("verified", "tinyint"),
        ("user_id", "varchar(255)"),
        ("facility", "varchar(255)"),
        ("verified_at", "datetime"),
        ("data", "text"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_token_deserialize() {
        let json = r#"{"user_id":"u001","facility":"fac-tokyo"}"#;
        let token: AuthToken = serde_json::from_str(json).unwrap();
        assert_eq!(token.user_id, "u001");
        assert_eq!(token.facility, "fac-tokyo");
        assert!(token.data.is_none());
    }

    #[test]
    fn test_auth_token_with_data() {
        let json = r#"{"user_id":"u001","facility":"fac-tokyo","data":{"extra":"value"}}"#;
        let token: AuthToken = serde_json::from_str(json).unwrap();
        assert_eq!(token.user_id, "u001");
        assert!(token.data.is_some());
    }

    #[test]
    fn test_qr_verify_result_not_found() {
        let result = QrVerifyResult::not_found();
        assert!(!result.verified);
        assert!(result.user_id.is_none());
    }

    #[test]
    fn test_qr_verify_result_from_token() {
        let token = AuthToken {
            user_id: "u001".to_string(),
            facility: "fac-tokyo".to_string(),
            data: None,
        };
        let result = QrVerifyResult::from_token(token);
        assert!(result.verified);
        assert_eq!(result.user_id.as_deref(), Some("u001"));
        assert_eq!(result.facility.as_deref(), Some("fac-tokyo"));
        assert!(result.verified_at.is_some());
    }
}
