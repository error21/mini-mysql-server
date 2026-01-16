use async_trait::async_trait;
use opensrv_mysql::*;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::auth::{self, qr_verify_columns, QrVerifyResult};
use crate::config::Config;
use crate::logging::{self, QueryLog, QueryResult};
use crate::query::{self, QueryType};
use crate::rate_limit;

pub const VERSION: &str = "8.0.36-mini-mysql-redis";

/// Table schema definition
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub pk_field: String,
    pub fields: Vec<(String, ColumnType)>,
}

/// Users table value (stored in Redis as JSON)
#[derive(Debug, Serialize, Deserialize)]
pub struct UserRecord {
    pub name: String,
    pub email: String,
    pub age: Option<i32>,
    pub created_at: String,
}

impl TableSchema {
    pub fn users() -> Self {
        TableSchema {
            name: "users".to_string(),
            pk_field: "id".to_string(),
            fields: vec![
                ("name".to_string(), ColumnType::MYSQL_TYPE_VARCHAR),
                ("email".to_string(), ColumnType::MYSQL_TYPE_VARCHAR),
                ("age".to_string(), ColumnType::MYSQL_TYPE_LONG),
                ("created_at".to_string(), ColumnType::MYSQL_TYPE_DATETIME),
            ],
        }
    }

    pub fn columns(&self) -> Vec<Column> {
        let mut cols = vec![Column {
            table: self.name.clone(),
            column: self.pk_field.clone(),
            coltype: ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: ColumnFlags::PRI_KEY_FLAG | ColumnFlags::NOT_NULL_FLAG,
        }];
        for (name, coltype) in &self.fields {
            cols.push(Column {
                table: self.name.clone(),
                column: name.clone(),
                coltype: *coltype,
                colflags: ColumnFlags::empty(),
            });
        }
        cols
    }
}

pub struct Backend {
    redis: Arc<Mutex<ConnectionManager>>,
    schemas: HashMap<String, TableSchema>,
    config: Config,
    client_addr: SocketAddr,
}

impl Backend {
    pub fn new(redis: ConnectionManager, config: Config, client_addr: SocketAddr) -> Self {
        let mut schemas = HashMap::new();
        schemas.insert("users".to_string(), TableSchema::users());
        Backend {
            redis: Arc::new(Mutex::new(redis)),
            schemas,
            config,
            client_addr,
        }
    }

    /// Get all keys for a table using SCAN with limit
    async fn get_all_keys(&self, table: &str, limit: usize) -> io::Result<Vec<String>> {
        // Log SCAN warning
        logging::log_scan_warning(table, limit, self.client_addr);

        let mut conn = self.redis.lock().await;
        let pattern = format!("{}.*", table);
        let mut cursor = 0u64;
        let mut keys = Vec::new();

        loop {
            let result: Result<(u64, Vec<String>), redis::RedisError> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await;

            match result {
                Ok((new_cursor, batch)) => {
                    keys.extend(batch);
                    cursor = new_cursor;

                    // Apply limit
                    if limit > 0 && keys.len() >= limit {
                        keys.truncate(limit);
                        break;
                    }

                    if cursor == 0 {
                        break;
                    }
                }
                Err(e) => {
                    logging::log_redis_error("scan", &e);
                    return Ok(Vec::new()); // Return empty on Redis error
                }
            }
        }
        Ok(keys)
    }

    /// Get single record by key
    async fn get_record(&self, key: &str) -> Option<String> {
        let mut conn = self.redis.lock().await;
        match conn.get::<_, Option<String>>(key).await {
            Ok(value) => value,
            Err(e) => {
                logging::log_redis_error("get", &e);
                None
            }
        }
    }

    /// Get table names for query validation
    fn table_names(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| s.as_str()).collect()
    }

    /// Write qr_verify result columns
    async fn write_qr_verify_result<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        result: QrVerifyResult,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        let cols: Vec<Column> = qr_verify_columns()
            .iter()
            .map(|(name, _)| Column {
                table: "".to_string(),
                column: name.to_string(),
                coltype: ColumnType::MYSQL_TYPE_VARCHAR,
                colflags: ColumnFlags::empty(),
            })
            .collect();

        let mut rw = results.start(&cols).await?;

        if result.verified {
            rw.write_col(1i32)?; // verified
            rw.write_col(result.user_id.as_deref().unwrap_or(""))?;
            rw.write_col(result.facility.as_deref().unwrap_or(""))?;
            rw.write_col(result.verified_at.as_deref().unwrap_or(""))?;
            rw.write_col(result.data.as_deref().unwrap_or(""))?;
            rw.end_row().await?;
        }
        // If not verified, return 0 rows

        rw.finish().await
    }
}

#[async_trait]
impl<W: tokio::io::AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;

    fn version(&self) -> String {
        VERSION.to_string()
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        _info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _id: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        // Log raw query at DEBUG level only
        logging::log_query_debug(query, self.client_addr);

        // Rate limit check
        {
            let mut conn = self.redis.lock().await;
            if !rate_limit::check_rate_limit(
                &mut conn,
                &self.client_addr.ip(),
                self.config.rate_limit,
                self.config.rate_window,
            )
            .await
            {
                QueryLog::new("rate_limited", self.client_addr)
                    .with_result(QueryResult::RateLimited)
                    .log();
                return results.completed(OkResponse::default()).await;
            }
        }

        // Parse and validate query
        let query_type = query::parse_query(query, &self.table_names());

        match query_type {
            QueryType::SelectVersion => {
                let cols = [Column {
                    table: "".to_string(),
                    column: "@@version".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VARCHAR,
                    colflags: ColumnFlags::empty(),
                }];
                let mut rw = results.start(&cols).await?;
                rw.write_col(VERSION)?;
                rw.end_row().await?;

                QueryLog::new("version", self.client_addr)
                    .with_rows(1)
                    .log();

                rw.finish().await
            }

            QueryType::SetOrUse => {
                QueryLog::new("set_use", self.client_addr).log();
                results.completed(OkResponse::default()).await
            }

            QueryType::ShowTables => {
                let cols = [Column {
                    table: "".to_string(),
                    column: "Tables_in_db".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VARCHAR,
                    colflags: ColumnFlags::empty(),
                }];
                let mut rw = results.start(&cols).await?;
                let count = self.schemas.len();
                for table_name in self.schemas.keys() {
                    rw.write_col(table_name)?;
                    rw.end_row().await?;
                }

                QueryLog::new("show_tables", self.client_addr)
                    .with_rows(count)
                    .log();

                rw.finish().await
            }

            QueryType::DescribeTable(table_name) => {
                let schema = self.schemas.get(&table_name).unwrap().clone();
                let cols = [
                    Column {
                        table: "".to_string(),
                        column: "Field".to_string(),
                        coltype: ColumnType::MYSQL_TYPE_VARCHAR,
                        colflags: ColumnFlags::empty(),
                    },
                    Column {
                        table: "".to_string(),
                        column: "Type".to_string(),
                        coltype: ColumnType::MYSQL_TYPE_VARCHAR,
                        colflags: ColumnFlags::empty(),
                    },
                    Column {
                        table: "".to_string(),
                        column: "Key".to_string(),
                        coltype: ColumnType::MYSQL_TYPE_VARCHAR,
                        colflags: ColumnFlags::empty(),
                    },
                ];
                let mut rw = results.start(&cols).await?;

                // PK field
                rw.write_col(&schema.pk_field)?;
                rw.write_col("varchar(255)")?;
                rw.write_col("PRI")?;
                rw.end_row().await?;

                // Other fields
                for (name, coltype) in &schema.fields {
                    rw.write_col(name)?;
                    let type_str = match coltype {
                        ColumnType::MYSQL_TYPE_VARCHAR => "varchar(255)",
                        ColumnType::MYSQL_TYPE_LONG => "int",
                        ColumnType::MYSQL_TYPE_DATETIME => "datetime",
                        _ => "unknown",
                    };
                    rw.write_col(type_str)?;
                    rw.write_col("")?;
                    rw.end_row().await?;
                }

                QueryLog::new("describe", self.client_addr)
                    .with_table(&table_name)
                    .with_rows(schema.fields.len() + 1)
                    .log();

                rw.finish().await
            }

            QueryType::SelectByPk { table, pk_value } => {
                let schema = self.schemas.get(&table).unwrap().clone();
                let cols = schema.columns();
                let key = format!("{}.{}", table, pk_value);

                let value = self.get_record(&key).await;
                let mut rw = results.start(&cols).await?;
                let mut row_count = 0;

                if let Some(json) = value {
                    if let Ok(record) = serde_json::from_str::<UserRecord>(&json) {
                        rw.write_col(&pk_value)?;
                        rw.write_col(&record.name)?;
                        rw.write_col(&record.email)?;
                        rw.write_col(record.age.map(|a| a.to_string()).unwrap_or_default())?;
                        rw.write_col(&record.created_at)?;
                        rw.end_row().await?;
                        row_count = 1;
                    }
                }

                QueryLog::new("pk_lookup", self.client_addr)
                    .with_table(&table)
                    .with_rows(row_count)
                    .log();

                rw.finish().await
            }

            QueryType::SelectScan { table } => {
                // Check if SCAN is allowed
                if !self.config.allow_scan {
                    QueryLog::new("scan", self.client_addr)
                        .with_table(&table)
                        .with_result(QueryResult::Rejected)
                        .log();
                    return results.completed(OkResponse::default()).await;
                }

                let schema = self.schemas.get(&table).unwrap().clone();
                let cols = schema.columns();
                let keys = self.get_all_keys(&table, self.config.scan_limit).await?;
                let mut rw = results.start(&cols).await?;
                let mut row_count = 0;

                for key in keys {
                    let pk_value = key.strip_prefix(&format!("{}.", table)).unwrap_or(&key);
                    if let Some(json) = self.get_record(&key).await {
                        if let Ok(record) = serde_json::from_str::<UserRecord>(&json) {
                            rw.write_col(pk_value)?;
                            rw.write_col(&record.name)?;
                            rw.write_col(&record.email)?;
                            rw.write_col(record.age.map(|a| a.to_string()).unwrap_or_default())?;
                            rw.write_col(&record.created_at)?;
                            rw.end_row().await?;
                            row_count += 1;
                        }
                    }
                }

                QueryLog::new("scan", self.client_addr)
                    .with_table(&table)
                    .with_rows(row_count)
                    .log();

                rw.finish().await
            }

            QueryType::QrVerify { token } => {
                let result = {
                    let mut conn = self.redis.lock().await;
                    auth::verify_token(&mut conn, &token).await
                };

                let verified = result.verified;
                QueryLog::new("qr_verify", self.client_addr)
                    .with_rows(if verified { 1 } else { 0 })
                    .log();

                self.write_qr_verify_result(result, results).await
            }

            QueryType::Rejected { reason } => {
                QueryLog::new("rejected", self.client_addr)
                    .with_result(QueryResult::Rejected)
                    .log();
                tracing::warn!(reason = reason, "query_rejected");
                results.completed(OkResponse::default()).await
            }
        }
    }
}
