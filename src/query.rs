/// SQL query types that are allowed
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// SELECT @@version
    SelectVersion,
    /// SHOW TABLES
    ShowTables,
    /// DESC/DESCRIBE {table}
    DescribeTable(String),
    /// SELECT * FROM {table} WHERE {pk} = '{value}'
    SelectByPk { table: String, pk_value: String },
    /// SELECT * FROM {table} (full scan)
    SelectScan { table: String },
    /// SELECT qr_verify('{token}')
    QrVerify { token: String },
    /// SET/USE commands (ignored)
    SetOrUse,
    /// Rejected query
    Rejected { reason: &'static str },
}

/// Parse and validate SQL query against whitelist
pub fn parse_query(query: &str, known_tables: &[&str]) -> QueryType {
    let query_trimmed = query.trim();
    let query_lower = query_trimmed.to_lowercase();

    // SET/USE commands - always allowed (no-op)
    if query_lower.starts_with("set ") || query_lower.starts_with("use ") {
        return QueryType::SetOrUse;
    }

    // SELECT @@version
    if query_lower.contains("@@version") {
        return QueryType::SelectVersion;
    }

    // SHOW TABLES
    if query_lower == "show tables" {
        return QueryType::ShowTables;
    }

    // DESC/DESCRIBE {table}
    for table in known_tables {
        if query_lower == format!("describe {}", table) || query_lower == format!("desc {}", table)
        {
            return QueryType::DescribeTable(table.to_string());
        }
    }

    // SELECT qr_verify('{token}')
    if let Some(token) = parse_qr_verify(&query_lower, query_trimmed) {
        return QueryType::QrVerify { token };
    }

    // Check for rejected patterns BEFORE allowing SELECT
    if let Some(reason) = check_rejected_patterns(&query_lower) {
        return QueryType::Rejected { reason };
    }

    // SELECT * FROM {table} WHERE {pk} = '{value}' or SELECT * FROM {table}
    if query_lower.starts_with("select") {
        for table in known_tables {
            if query_lower.contains(table) {
                // Check for WHERE clause
                if let Some(pk_value) = parse_where_pk(query_trimmed, "id") {
                    return QueryType::SelectByPk {
                        table: table.to_string(),
                        pk_value,
                    };
                }
                // No WHERE clause = scan
                if !query_lower.contains("where") {
                    return QueryType::SelectScan {
                        table: table.to_string(),
                    };
                }
            }
        }
    }

    // Default: reject unknown queries
    QueryType::Rejected {
        reason: "unknown_query",
    }
}

/// Check for patterns that should be rejected
fn check_rejected_patterns(query_lower: &str) -> Option<&'static str> {
    // Reject INSERT/UPDATE/DELETE
    if query_lower.starts_with("insert") {
        return Some("insert_not_allowed");
    }
    if query_lower.starts_with("update") {
        return Some("update_not_allowed");
    }
    if query_lower.starts_with("delete") {
        return Some("delete_not_allowed");
    }
    if query_lower.starts_with("drop") {
        return Some("drop_not_allowed");
    }
    if query_lower.starts_with("truncate") {
        return Some("truncate_not_allowed");
    }
    if query_lower.starts_with("alter") {
        return Some("alter_not_allowed");
    }
    if query_lower.starts_with("create") {
        return Some("create_not_allowed");
    }

    // Reject complex WHERE clauses
    if query_lower.contains("where") {
        // Multiple conditions
        if query_lower.contains(" and ") {
            return Some("and_not_allowed");
        }
        if query_lower.contains(" or ") {
            return Some("or_not_allowed");
        }
        // IN clause
        if query_lower.contains(" in ") || query_lower.contains(" in(") {
            return Some("in_not_allowed");
        }
        // LIKE clause
        if query_lower.contains(" like ") {
            return Some("like_not_allowed");
        }
        // Comparison operators (beyond =)
        if query_lower.contains(" > ") || query_lower.contains(" < ") {
            return Some("comparison_not_allowed");
        }
        if query_lower.contains(" >= ") || query_lower.contains(" <= ") {
            return Some("comparison_not_allowed");
        }
        if query_lower.contains(" <> ") || query_lower.contains(" != ") {
            return Some("comparison_not_allowed");
        }
        if query_lower.contains(" between ") {
            return Some("between_not_allowed");
        }
    }

    // Reject JOIN
    if query_lower.contains(" join ") {
        return Some("join_not_allowed");
    }

    // Reject ORDER BY / GROUP BY / LIMIT / OFFSET
    if query_lower.contains(" order by ") {
        return Some("order_by_not_allowed");
    }
    if query_lower.contains(" group by ") {
        return Some("group_by_not_allowed");
    }
    if query_lower.contains(" limit ") {
        return Some("limit_not_allowed");
    }
    if query_lower.contains(" offset ") {
        return Some("offset_not_allowed");
    }

    // Reject subqueries
    if query_lower.contains("(select") {
        return Some("subquery_not_allowed");
    }

    // Reject UNION
    if query_lower.contains(" union ") {
        return Some("union_not_allowed");
    }

    None
}

/// Parse SELECT qr_verify('token') pattern
fn parse_qr_verify(query_lower: &str, query_original: &str) -> Option<String> {
    // Match: SELECT qr_verify('...')
    if !query_lower.contains("qr_verify") {
        return None;
    }

    // Find the opening paren
    let paren_start = query_original.find("qr_verify(")? + 10;
    let after_paren = &query_original[paren_start..];

    // Extract quoted token
    if after_paren.starts_with('\'') {
        let end = after_paren[1..].find('\'')?;
        return Some(after_paren[1..1 + end].to_string());
    } else if after_paren.starts_with('"') {
        let end = after_paren[1..].find('"')?;
        return Some(after_paren[1..1 + end].to_string());
    }

    None
}

/// Parse simple WHERE clause: WHERE {pk_field} = 'value'
pub fn parse_where_pk(query: &str, pk_field: &str) -> Option<String> {
    let query_lower = query.to_lowercase();
    let pk_lower = pk_field.to_lowercase();

    // Find WHERE clause
    let where_pos = query_lower.find("where")?;
    let after_where = &query[where_pos + 5..];

    // Find pk_field = 'value' pattern
    let after_lower = after_where.to_lowercase();
    let pk_pos = after_lower.find(&pk_lower)?;
    let after_pk = &after_where[pk_pos + pk_field.len()..].trim_start();

    // Skip '='
    let after_eq = after_pk.strip_prefix('=')?.trim_start();

    // Extract quoted value
    if after_eq.starts_with('\'') {
        let end = after_eq[1..].find('\'')?;
        Some(after_eq[1..1 + end].to_string())
    } else if after_eq.starts_with('"') {
        let end = after_eq[1..].find('"')?;
        Some(after_eq[1..1 + end].to_string())
    } else {
        // Unquoted value (until space or end)
        let value: String = after_eq
            .chars()
            .take_while(|c| !c.is_whitespace())
            .collect();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_version() {
        let tables = ["users"];
        assert_eq!(
            parse_query("SELECT @@version", &tables),
            QueryType::SelectVersion
        );
    }

    #[test]
    fn test_show_tables() {
        let tables = ["users"];
        assert_eq!(parse_query("SHOW TABLES", &tables), QueryType::ShowTables);
    }

    #[test]
    fn test_describe_table() {
        let tables = ["users"];
        assert_eq!(
            parse_query("DESC users", &tables),
            QueryType::DescribeTable("users".to_string())
        );
    }

    #[test]
    fn test_select_by_pk() {
        let tables = ["users"];
        assert_eq!(
            parse_query("SELECT * FROM users WHERE id = 'u001'", &tables),
            QueryType::SelectByPk {
                table: "users".to_string(),
                pk_value: "u001".to_string()
            }
        );
    }

    #[test]
    fn test_select_scan() {
        let tables = ["users"];
        assert_eq!(
            parse_query("SELECT * FROM users", &tables),
            QueryType::SelectScan {
                table: "users".to_string()
            }
        );
    }

    #[test]
    fn test_qr_verify() {
        let tables = ["users"];
        assert_eq!(
            parse_query("SELECT qr_verify('abc123')", &tables),
            QueryType::QrVerify {
                token: "abc123".to_string()
            }
        );
    }

    #[test]
    fn test_reject_and() {
        let tables = ["users"];
        let result = parse_query("SELECT * FROM users WHERE id = 'u001' AND name = 'Alice'", &tables);
        assert!(matches!(result, QueryType::Rejected { reason: "and_not_allowed" }));
    }

    #[test]
    fn test_reject_join() {
        let tables = ["users", "orders"];
        let result = parse_query("SELECT * FROM users JOIN orders ON users.id = orders.user_id", &tables);
        assert!(matches!(result, QueryType::Rejected { reason: "join_not_allowed" }));
    }

    #[test]
    fn test_reject_update() {
        let tables = ["users"];
        let result = parse_query("UPDATE users SET name = 'X' WHERE id = 'u001'", &tables);
        assert!(matches!(result, QueryType::Rejected { reason: "update_not_allowed" }));
    }
}
