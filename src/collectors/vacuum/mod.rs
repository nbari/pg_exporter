use super::Collector;
use anyhow::Result;
use sqlx::PgPool;

#[derive(Clone)]
pub struct VacuumCollector;

impl Default for VacuumCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VacuumCollector {
    pub fn new() -> Self {
        Self
    }
}

impl Collector for VacuumCollector {
    fn name(&self) -> &'static str {
        "vacuum"
    }

    async fn collect(&self, pool: &PgPool) -> Result<String> {
        // Example vacuum metrics query
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT schemaname||'.'||relname, n_tup_ins + n_tup_upd + n_tup_del as total_changes
             FROM pg_stat_user_tables",
        )
        .fetch_all(pool)
        .await?;

        let mut output = String::new();
        for (table, changes) in rows {
            output.push_str(&format!(
                "pg_table_changes{{table=\"{}\"}} {}\n",
                table, changes
            ));
        }

        Ok(output)
    }

    fn enabled_by_default(&self) -> bool {
        false // Vacuum collector disabled by default
    }
}
