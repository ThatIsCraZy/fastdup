use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension as _, params};

use crate::{
    JobState, JobStatus, RepositoryBinding, RepositorySettings, ShareSettings, TelemetrySnapshot,
    unix_seconds,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("control database failed: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("control record is malformed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("control database lock is poisoned")]
    Poisoned,
    #[error("configuration revision changed")]
    RevisionConflict,
    #[error("an incomplete provisioning journal requires operator recovery")]
    ProvisioningIncomplete,
}

#[derive(Clone, Debug)]
pub struct ControlStore {
    connection: Arc<Mutex<Connection>>,
}

impl ControlStore {
    /// Opens and migrates the authoritative management configuration database.
    /// Repository bytes and Pool identity are deliberately absent from it.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                StoreError::Sql(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })?;
        }
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                must_change INTEGER NOT NULL,
                failed_attempts INTEGER NOT NULL DEFAULT 0,
                locked_until INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token_hash TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                csrf_token TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                last_seen INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
            CREATE TABLE IF NOT EXISTS repository_settings (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL,
                body TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS repository_binding (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                body TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS shares (
                id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL,
                body TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                state TEXT NOT NULL,
                progress_basis_points INTEGER NOT NULL,
                message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs(updated_at DESC);
            CREATE TABLE IF NOT EXISTS idempotency (
                key TEXT PRIMARY KEY,
                job_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                outcome TEXT NOT NULL,
                detail TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp DESC);
            CREATE TABLE IF NOT EXISTS provisioning_journal (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                metadata_target TEXT NOT NULL,
                data_target TEXT NOT NULL,
                step TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            PRAGMA optimize;
            ",
        )?;
        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        store.ensure_default_settings()?;
        Ok(store)
    }

    fn locked(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::Poisoned)
    }

    fn ensure_default_settings(&self) -> Result<(), StoreError> {
        let settings = RepositorySettings::default();
        self.locked()?.execute(
            "INSERT OR IGNORE INTO repository_settings(singleton, revision, body) VALUES(1, ?1, ?2)",
            params![settings.revision, serde_json::to_string(&settings)?],
        )?;
        Ok(())
    }

    pub fn settings(&self) -> Result<RepositorySettings, StoreError> {
        let body: String = self.locked()?.query_row(
            "SELECT body FROM repository_settings WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        Ok(serde_json::from_str(&body)?)
    }

    pub fn repository_binding(&self) -> Result<Option<RepositoryBinding>, StoreError> {
        let body = self
            .locked()?
            .query_row(
                "SELECT body FROM repository_binding WHERE singleton = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        body.map(|body| serde_json::from_str(&body).map_err(Into::into))
            .transpose()
    }

    pub fn set_repository_binding(&self, binding: &RepositoryBinding) -> Result<(), StoreError> {
        self.locked()?.execute(
            "INSERT INTO repository_binding(singleton, body) VALUES(1, ?1) ON CONFLICT(singleton) DO UPDATE SET body = excluded.body",
            [serde_json::to_string(binding)?],
        )?;
        Ok(())
    }

    pub fn update_settings(
        &self,
        expected_revision: u64,
        mut settings: RepositorySettings,
    ) -> Result<RepositorySettings, StoreError> {
        let connection = self.locked()?;
        let next = expected_revision.saturating_add(1);
        settings.revision = next;
        let changed = connection.execute(
            "UPDATE repository_settings SET revision = ?1, body = ?2 WHERE singleton = 1 AND revision = ?3",
            params![next, serde_json::to_string(&settings)?, expected_revision],
        )?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict);
        }
        Ok(settings)
    }

    pub fn shares(&self) -> Result<Vec<ShareSettings>, StoreError> {
        let connection = self.locked()?;
        let mut statement = connection.prepare("SELECT body FROM shares ORDER BY id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut shares = Vec::new();
        for row in rows {
            shares.push(serde_json::from_str(&row?)?);
        }
        Ok(shares)
    }

    pub fn upsert_share(
        &self,
        expected_revision: Option<u64>,
        mut share: ShareSettings,
    ) -> Result<ShareSettings, StoreError> {
        let connection = self.locked()?;
        match expected_revision {
            None => {
                share.revision = 1;
                connection.execute(
                    "INSERT INTO shares(id, revision, body) VALUES(?1, 1, ?2)",
                    params![share.id, serde_json::to_string(&share)?],
                )?;
            }
            Some(expected) => {
                share.revision = expected.saturating_add(1);
                let changed = connection.execute(
                    "UPDATE shares SET revision = ?1, body = ?2 WHERE id = ?3 AND revision = ?4",
                    params![
                        share.revision,
                        serde_json::to_string(&share)?,
                        share.id,
                        expected
                    ],
                )?;
                if changed != 1 {
                    return Err(StoreError::RevisionConflict);
                }
            }
        }
        Ok(share)
    }

    pub fn delete_share(&self, id: &str, expected_revision: u64) -> Result<(), StoreError> {
        let changed = self.locked()?.execute(
            "DELETE FROM shares WHERE id = ?1 AND revision = ?2",
            params![id, expected_revision],
        )?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict);
        }
        Ok(())
    }

    pub fn job_for_idempotency(&self, key: &str) -> Result<Option<JobStatus>, StoreError> {
        let connection = self.locked()?;
        let id = connection
            .query_row(
                "SELECT job_id FROM idempotency WHERE key = ?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        id.map_or(Ok(None), |id| {
            Self::job_with_connection(&connection, &id).map(Some)
        })
    }

    pub fn insert_job(&self, key: &str, job: &JobStatus) -> Result<(), StoreError> {
        let connection = self.locked()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO jobs(id, kind, state, progress_basis_points, message, created_at, updated_at) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![job.id, job.kind, job_state_name(job.state), job.progress_basis_points, job.message, job.created_at, job.updated_at],
        )?;
        transaction.execute(
            "INSERT INTO idempotency(key, job_id) VALUES(?1, ?2)",
            params![key, job.id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn update_job(&self, job: &JobStatus) -> Result<(), StoreError> {
        self.locked()?.execute(
            "UPDATE jobs SET state = ?1, progress_basis_points = ?2, message = ?3, updated_at = ?4 WHERE id = ?5",
            params![job_state_name(job.state), job.progress_basis_points, job.message, job.updated_at, job.id],
        )?;
        Ok(())
    }

    pub fn recent_jobs(&self, limit: usize) -> Result<Vec<JobStatus>, StoreError> {
        let connection = self.locked()?;
        let mut statement = connection.prepare(
            "SELECT id, kind, state, progress_basis_points, message, created_at, updated_at FROM jobs ORDER BY updated_at DESC LIMIT ?1",
        )?;
        let rows = statement.query_map([i64::try_from(limit).unwrap_or(i64::MAX)], row_to_job)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn job_with_connection(connection: &Connection, id: &str) -> Result<JobStatus, StoreError> {
        Ok(connection.query_row(
            "SELECT id, kind, state, progress_basis_points, message, created_at, updated_at FROM jobs WHERE id = ?1",
            [id],
            row_to_job,
        )?)
    }

    pub fn audit(
        &self,
        actor: &str,
        action: &str,
        outcome: &str,
        detail: &str,
    ) -> Result<(), StoreError> {
        self.locked()?.execute(
            "INSERT INTO audit_log(timestamp, actor, action, outcome, detail) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![unix_seconds(), actor, action, outcome, detail],
        )?;
        Ok(())
    }

    pub fn begin_provisioning(
        &self,
        metadata_target: &str,
        data_target: &str,
    ) -> Result<(), StoreError> {
        let connection = self.locked()?;
        if connection
            .query_row(
                "SELECT 1 FROM provisioning_journal WHERE singleton = 1",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(StoreError::ProvisioningIncomplete);
        }
        connection.execute(
            "INSERT INTO provisioning_journal(singleton, metadata_target, data_target, step, updated_at) VALUES(1, ?1, ?2, 'selected', ?3)",
            params![metadata_target, data_target, unix_seconds()],
        )?;
        Ok(())
    }

    pub fn advance_provisioning(&self, step: &str) -> Result<(), StoreError> {
        let changed = self.locked()?.execute(
            "UPDATE provisioning_journal SET step = ?1, updated_at = ?2 WHERE singleton = 1",
            params![step, unix_seconds()],
        )?;
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::ProvisioningIncomplete)
        }
    }

    pub fn finish_provisioning(&self) -> Result<(), StoreError> {
        self.locked()?
            .execute("DELETE FROM provisioning_journal WHERE singleton = 1", [])?;
        Ok(())
    }

    pub(crate) fn connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.connection)
    }
}

fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
    }
}

fn parse_job_state(value: &str) -> Result<JobState, rusqlite::Error> {
    match value {
        "queued" => Ok(JobState::Queued),
        "running" => Ok(JobState::Running),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn row_to_job(row: &rusqlite::Row<'_>) -> Result<JobStatus, rusqlite::Error> {
    let state: String = row.get(2)?;
    Ok(JobStatus {
        id: row.get(0)?,
        kind: row.get(1)?,
        state: parse_job_state(&state)?,
        progress_basis_points: row.get(3)?,
        message: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

#[derive(Clone, Debug)]
pub struct TelemetryStore {
    connection: Arc<Mutex<Connection>>,
}

impl TelemetryStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                StoreError::Sql(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })?;
        }
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS samples_raw (
                observed_at INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                body TEXT NOT NULL,
                PRIMARY KEY(observed_at, sequence)
            );
            CREATE INDEX IF NOT EXISTS idx_samples_raw_time ON samples_raw(observed_at);
            CREATE TABLE IF NOT EXISTS samples_10s (
                observed_at INTEGER PRIMARY KEY,
                body TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS samples_60s (
                observed_at INTEGER PRIMARY KEY,
                body TEXT NOT NULL
            );
            PRAGMA optimize;
            ",
        )?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn locked(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::Poisoned)
    }

    pub fn insert(&self, observed_at: i64, snapshot: &TelemetrySnapshot) -> Result<(), StoreError> {
        self.locked()?.execute(
            "INSERT OR REPLACE INTO samples_raw(observed_at, sequence, body) VALUES(?1, ?2, ?3)",
            params![
                observed_at,
                snapshot.sequence,
                serde_json::to_string(snapshot)?
            ],
        )?;
        Ok(())
    }

    pub fn query(
        &self,
        from: i64,
        to: i64,
        limit: usize,
    ) -> Result<Vec<TelemetrySnapshot>, StoreError> {
        let connection = self.locked()?;
        let span = to.saturating_sub(from);
        let table = if span > 604_800 {
            "samples_60s"
        } else if span > 86_400 {
            "samples_10s"
        } else {
            "samples_raw"
        };
        let sql = format!(
            "SELECT body FROM {table} WHERE observed_at >= ?1 AND observed_at <= ?2 ORDER BY observed_at LIMIT ?3"
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
            params![from, to, i64::try_from(limit).unwrap_or(i64::MAX)],
            |row| row.get::<_, String>(0),
        )?;
        let mut result = Vec::new();
        for row in rows {
            result.push(serde_json::from_str(&row?)?);
        }
        Ok(result)
    }

    pub fn retain_and_roll_up(&self, now: i64) -> Result<(), StoreError> {
        let connection = self.locked()?;
        roll_up(&connection, "samples_raw", "samples_10s", 10, now - 604_800)?;
        roll_up(
            &connection,
            "samples_10s",
            "samples_60s",
            60,
            now - 7_776_000,
        )?;
        connection.execute(
            "DELETE FROM samples_raw WHERE observed_at < ?1",
            [now - 86_400],
        )?;
        connection.execute(
            "DELETE FROM samples_10s WHERE observed_at < ?1",
            [now - 604_800],
        )?;
        connection.execute(
            "DELETE FROM samples_60s WHERE observed_at < ?1",
            [now - 7_776_000],
        )?;
        Ok(())
    }
}

fn roll_up(
    connection: &Connection,
    source: &str,
    target: &str,
    bucket_seconds: i64,
    from: i64,
) -> Result<(), StoreError> {
    let sql = format!(
        "SELECT observed_at, body FROM {source} WHERE observed_at >= ?1 ORDER BY observed_at"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([from], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut buckets = std::collections::BTreeMap::<i64, Vec<TelemetrySnapshot>>::new();
    for row in rows {
        let (observed_at, body) = row?;
        buckets
            .entry(observed_at - observed_at.rem_euclid(bucket_seconds))
            .or_default()
            .push(serde_json::from_str(&body)?);
    }
    let insert = format!("INSERT OR REPLACE INTO {target}(observed_at, body) VALUES(?1, ?2)");
    for (observed_at, samples) in buckets {
        let snapshot = average_snapshots(&samples);
        connection.execute(
            &insert,
            params![observed_at, serde_json::to_string(&snapshot)?],
        )?;
    }
    Ok(())
}

fn average_snapshots(samples: &[TelemetrySnapshot]) -> TelemetrySnapshot {
    let mut result = samples.last().cloned().unwrap_or_default();
    if samples.is_empty() {
        return result;
    }
    let count = samples.len() as f64;
    result.frontend_read_mbps = samples
        .iter()
        .map(|sample| sample.frontend_read_mbps)
        .sum::<f64>()
        / count;
    result.frontend_write_mbps = samples
        .iter()
        .map(|sample| sample.frontend_write_mbps)
        .sum::<f64>()
        / count;
    result.dedup_rate = samples.iter().map(|sample| sample.dedup_rate).sum::<f64>() / count;
    result.reduction_ratio = samples
        .iter()
        .map(|sample| sample.reduction_ratio)
        .sum::<f64>()
        / count;
    result.cpu_percent = samples.iter().map(|sample| sample.cpu_percent).sum::<f64>() / count;
    result.ram_percent = samples.iter().map(|sample| sample.ram_percent).sum::<f64>() / count;
    result.series.clear();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_update_is_revision_guarded() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = ControlStore::open(&directory.path().join("control.db")).expect("open store");
        let current = store.settings().expect("default settings");
        let updated = store
            .update_settings(
                current.revision,
                RepositorySettings {
                    auto_mount: false,
                    ..current.clone()
                },
            )
            .expect("guarded update");
        assert!(!updated.auto_mount);
        assert!(matches!(
            store.update_settings(current.revision, current),
            Err(StoreError::RevisionConflict)
        ));
    }

    #[test]
    fn telemetry_rollups_survive_raw_retention_and_range_queries_select_them() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = TelemetryStore::open(&directory.path().join("telemetry.db"))
            .expect("open telemetry store");
        let now = 10_000_000_i64;
        for index in 0..12_u64 {
            let snapshot = TelemetrySnapshot {
                sequence: index,
                frontend_read_mbps: index as f64,
                ..TelemetrySnapshot::default()
            };
            store
                .insert(
                    now - 90_000 + i64::try_from(index).expect("small fixture"),
                    &snapshot,
                )
                .expect("insert sample");
        }

        store.retain_and_roll_up(now).expect("roll up and retain");
        let history = store
            .query(now - 100_000, now, 100)
            .expect("query 10-second history");
        assert_eq!(history.len(), 2);
        assert!(history.iter().all(|sample| sample.series.is_empty()));
    }

    #[test]
    fn incomplete_provisioning_is_durable_and_fail_closed() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("control.db");
        let store = ControlStore::open(&path).expect("open store");
        store
            .begin_provisioning("metadata", "data")
            .expect("begin journal");
        store
            .advance_provisioning("metadata_formatted")
            .expect("advance journal");
        drop(store);

        let reopened = ControlStore::open(&path).expect("reopen store");
        assert!(matches!(
            reopened.begin_provisioning("metadata", "data"),
            Err(StoreError::ProvisioningIncomplete)
        ));
    }
}
