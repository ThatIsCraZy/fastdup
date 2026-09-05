use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension as _, params};

use crate::{
    AuditEvent, JobState, JobStatus, RepositoryBinding, RepositorySettings, ShareSettings,
    TelemetrySnapshot, unix_seconds,
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
    #[error("invalid UI language or unknown user")]
    InvalidUserPreference,
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
            CREATE TABLE IF NOT EXISTS user_preferences (
                username TEXT PRIMARY KEY REFERENCES users(username),
                ui_language TEXT NOT NULL CHECK(ui_language IN ('de', 'en'))
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
        make_database_group_writable(path)?;
        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        store.ensure_default_settings()?;
        Ok(store)
    }

    pub fn user_ui_language(&self, username: &str) -> Result<String, StoreError> {
        let language: Option<String> = self
            .locked()?
            .query_row(
                "SELECT ui_language FROM user_preferences WHERE username = ?1",
                [username],
                |row| row.get(0),
            )
            .optional()?;
        match language.as_deref() {
            None | Some("de") => Ok("de".into()),
            Some("en") => Ok("en".into()),
            Some(_) => Err(StoreError::InvalidUserPreference),
        }
    }

    pub fn set_user_ui_language(&self, username: &str, language: &str) -> Result<(), StoreError> {
        if !matches!(language, "de" | "en") {
            return Err(StoreError::InvalidUserPreference);
        }
        let changed = self.locked()?.execute(
            "INSERT INTO user_preferences(username, ui_language)
             SELECT username, ?2 FROM users WHERE username = ?1
             ON CONFLICT(username) DO UPDATE SET ui_language = excluded.ui_language",
            params![username, language],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidUserPreference);
        }
        Ok(())
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

    pub fn recent_audit(&self, limit: usize) -> Result<Vec<AuditEvent>, StoreError> {
        let connection = self.locked()?;
        let mut statement = connection.prepare(
            "SELECT id, timestamp, actor, action, outcome, detail FROM audit_log ORDER BY timestamp DESC, id DESC LIMIT ?1",
        )?;
        statement
            .query_map([i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
                Ok(AuditEvent {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    actor: row.get(2)?,
                    action: row.get(3)?,
                    outcome: row.get(4)?,
                    detail: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
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
    path: std::path::PathBuf,
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
        connection.busy_timeout(std::time::Duration::from_millis(500))?;
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
        make_database_group_writable(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn locked(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::Poisoned)
    }

    pub fn insert(&self, observed_at: i64, snapshot: &TelemetrySnapshot) -> Result<(), StoreError> {
        // The live chart is a sliding window, not part of one historical measurement.
        let mut measurement = snapshot.clone();
        measurement.series.clear();
        self.locked()?.execute(
            "INSERT OR REPLACE INTO samples_raw(observed_at, sequence, body) VALUES(?1, ?2, ?3)",
            params![
                observed_at,
                snapshot.sequence,
                serde_json::to_string(&measurement)?
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
        if limit == 0 || from > to {
            return Ok(Vec::new());
        }
        let limit = limit.min(1_500);
        let connection = self.locked()?;
        let span = to.saturating_sub(from);
        let bucket_seconds = (span / i64::try_from(limit).unwrap_or(1_500)).saturating_add(1);
        let table = if span > 604_800 {
            "samples_60s"
        } else if span > 86_400 {
            "samples_10s"
        } else {
            "samples_raw"
        };
        let sql = format!(
            // SQLite takes bare columns from the row supplying MIN(observed_at).
            // Return a representative measurement across the entire selected range.
            "SELECT json_set(body, '$.series', json('[]')), MIN(observed_at) FROM {table}
             WHERE observed_at >= ?1 AND observed_at <= ?2
             GROUP BY (observed_at - ?1) / ?3 ORDER BY MIN(observed_at) LIMIT ?4"
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
            params![
                from,
                to,
                bucket_seconds,
                i64::try_from(limit).unwrap_or(1_500)
            ],
            |row| row.get::<_, String>(0),
        )?;
        let mut result = Vec::new();
        for row in rows {
            result.push(serde_json::from_str(&row?)?);
        }
        Ok(result)
    }

    pub fn retain_and_roll_up(&self, now: i64) -> Result<(), StoreError> {
        // Maintenance uses its own bounded SQLite cache and never holds the sampler mutex.
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(std::time::Duration::from_millis(500))?;
        connection.pragma_update(None, "cache_size", -2048)?;
        let writer = Connection::open(&self.path)?;
        writer.busy_timeout(std::time::Duration::from_millis(500))?;
        writer.pragma_update(None, "cache_size", -2048)?;
        writer.pragma_update(None, "synchronous", "NORMAL")?;
        roll_up(
            &connection,
            &writer,
            "samples_raw",
            "samples_10s",
            10,
            now - 604_800,
        )?;
        roll_up(
            &connection,
            &writer,
            "samples_10s",
            "samples_60s",
            60,
            now - 7_776_000,
        )?;
        writer.execute(
            "DELETE FROM samples_raw WHERE observed_at < ?1",
            [now - 86_400],
        )?;
        writer.execute(
            "DELETE FROM samples_10s WHERE observed_at < ?1",
            [now - 604_800],
        )?;
        writer.execute(
            "DELETE FROM samples_60s WHERE observed_at < ?1",
            [now - 7_776_000],
        )?;
        Ok(())
    }
}

fn make_database_group_writable(path: &Path) -> Result<(), StoreError> {
    let mut paths = vec![path.to_path_buf()];
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        paths.push(sidecar.into());
    }
    for candidate in paths {
        let metadata = match std::fs::metadata(&candidate) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_store_error(error)),
        };
        let mut permissions = metadata.permissions();
        if permissions.mode() & 0o777 != 0o660 {
            permissions.set_mode(0o660);
            std::fs::set_permissions(candidate, permissions).map_err(io_store_error)?;
        }
    }
    Ok(())
}

fn io_store_error(error: std::io::Error) -> StoreError {
    StoreError::Sql(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

/// One bucket stays in memory regardless of retention length or sample count.
struct TelemetryAverage {
    latest: TelemetrySnapshot,
    sums: [f64; 6],
    count: u64,
}

impl TelemetryAverage {
    fn new() -> Self {
        Self {
            latest: TelemetrySnapshot::default(),
            sums: [0.0; 6],
            count: 0,
        }
    }

    fn push(&mut self, mut sample: TelemetrySnapshot) {
        for (sum, value) in self.sums.iter_mut().zip([
            sample.frontend_read_mbps,
            sample.frontend_write_mbps,
            sample.dedup_rate,
            sample.reduction_ratio,
            sample.cpu_percent,
            sample.ram_percent,
        ]) {
            *sum += value;
        }
        self.count += 1;
        sample.series.clear();
        self.latest = sample;
    }

    fn finish(mut self) -> TelemetrySnapshot {
        let count = self.count.max(1) as f64;
        self.latest.frontend_read_mbps = self.sums[0] / count;
        self.latest.frontend_write_mbps = self.sums[1] / count;
        self.latest.dedup_rate = self.sums[2] / count;
        self.latest.reduction_ratio = self.sums[3] / count;
        self.latest.cpu_percent = self.sums[4] / count;
        self.latest.ram_percent = self.sums[5] / count;
        self.latest
    }
}

fn roll_up(
    connection: &Connection,
    writer: &Connection,
    source: &str,
    target: &str,
    bucket_seconds: i64,
    from: i64,
) -> Result<(), StoreError> {
    // Strip legacy chart windows before decoding; old databases need no migration.
    let sql = format!(
        "SELECT observed_at, json_set(body, '$.series', json('[]')) FROM {source}
         WHERE observed_at >= ?1 ORDER BY observed_at"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([from], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let insert = format!("INSERT OR REPLACE INTO {target}(observed_at, body) VALUES(?1, ?2)");
    let mut pending: Option<(i64, TelemetryAverage)> = None;
    for row in rows {
        let (observed_at, body) = row?;
        let bucket = observed_at - observed_at.rem_euclid(bucket_seconds);
        if pending.as_ref().is_some_and(|(time, _)| *time != bucket) {
            let (time, average) = pending.take().expect("pending bucket exists");
            writer.execute(
                &insert,
                params![time, serde_json::to_string(&average.finish())?],
            )?;
        }
        pending
            .get_or_insert_with(|| (bucket, TelemetryAverage::new()))
            .1
            .push(serde_json::from_str(&body)?);
    }
    if let Some((time, average)) = pending {
        writer.execute(
            &insert,
            params![time, serde_json::to_string(&average.finish())?],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_language_is_validated_persisted_and_isolated_per_user() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        {
            let store = ControlStore::open(&path).unwrap();
            store.locked().unwrap().execute(
                "INSERT INTO users(username,password_hash,must_change) VALUES('alice','test',0),('bob','test',0)", [],
            ).unwrap();
            assert_eq!(store.user_ui_language("alice").unwrap(), "de");
            store.set_user_ui_language("alice", "en").unwrap();
            assert_eq!(store.user_ui_language("bob").unwrap(), "de");
            assert!(store.set_user_ui_language("bob", "fr").is_err());
            assert!(store.set_user_ui_language("unknown", "en").is_err());
            assert!(
                store
                    .locked()
                    .unwrap()
                    .execute("INSERT INTO user_preferences VALUES('bob','fr')", [])
                    .is_err()
            );
        }
        let store = ControlStore::open(&path).unwrap();
        assert_eq!(store.user_ui_language("alice").unwrap(), "en");
        assert_eq!(store.user_ui_language("bob").unwrap(), "de");
    }

    #[test]
    fn historical_samples_do_not_repeat_live_chart_series() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = TelemetryStore::open(&directory.path().join("telemetry.db")).unwrap();
        let snapshot = TelemetrySnapshot {
            sequence: 1,
            series: vec![
                crate::SeriesPoint {
                    time: "sample".into(),
                    read: 4.0,
                    write: 2.0
                };
                900
            ],
            ..TelemetrySnapshot::default()
        };
        store.insert(100, &snapshot).unwrap();
        let body: String = store
            .locked()
            .unwrap()
            .query_row("SELECT body FROM samples_raw", [], |row| row.get(0))
            .unwrap();
        assert!(
            body.len() < 4096,
            "history must store one measurement, not 900 repeated chart points"
        );
        assert_eq!(snapshot.series.len(), 900, "live chart remains intact");
        assert!(store.query(0, 200, 10).unwrap()[0].series.is_empty());
    }

    #[test]
    fn history_bounds_legacy_series_and_covers_the_requested_range() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = TelemetryStore::open(&directory.path().join("telemetry.db")).unwrap();
        let snapshot = TelemetrySnapshot {
            series: vec![
                crate::SeriesPoint {
                    time: "legacy".into(),
                    read: 1.0,
                    write: 2.0
                };
                900
            ],
            ..TelemetrySnapshot::default()
        };
        let body = serde_json::to_string(&snapshot).unwrap();
        store
            .locked()
            .unwrap()
            .execute(
                "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<99)
             INSERT INTO samples_raw SELECT x,x,json_set(?1, '$.sequence', x) FROM n",
                [&body],
            )
            .unwrap();
        let samples = store.query(0, 99, 10).unwrap();
        assert_eq!(samples.len(), 10);
        assert_eq!(
            samples.last().unwrap().sequence,
            90,
            "cover the end, not just the first ten seconds"
        );
        assert!(samples.iter().all(|sample| sample.series.is_empty()));
        assert!(store.query(0, 99, 0).unwrap().is_empty());
    }

    #[test]
    fn rollup_can_publish_while_the_sampler_advances_its_read_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("telemetry.db");
        let store = TelemetryStore::open(&path).unwrap();
        store.insert(100, &TelemetrySnapshot::default()).unwrap();
        let reader = Connection::open(&path).unwrap();
        let writer = Connection::open(&path).unwrap();
        let mut statement = reader.prepare("SELECT body FROM samples_raw").unwrap();
        let mut rows = statement.query([]).unwrap();
        assert!(rows.next().unwrap().is_some());
        // A concurrent sampler commits after maintenance acquired its read snapshot.
        store.insert(101, &TelemetrySnapshot::default()).unwrap();
        roll_up(&reader, &writer, "samples_raw", "samples_10s", 10, 0).unwrap();
        assert_eq!(
            writer
                .query_row("SELECT count(*) FROM samples_10s", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn streaming_rollup_preserves_averages_and_latest_metadata() {
        let mut average = TelemetryAverage::new();
        for value in [2.0, 4.0, 9.0] {
            average.push(TelemetrySnapshot {
                observed_at: value.to_string(),
                frontend_read_mbps: value,
                frontend_write_mbps: value * 2.0,
                dedup_rate: value * 3.0,
                reduction_ratio: value * 4.0,
                cpu_percent: value * 5.0,
                ram_percent: value * 6.0,
                ..TelemetrySnapshot::default()
            });
        }
        let result = average.finish();
        assert_eq!(result.observed_at, "9");
        for (actual, expected) in [
            (result.frontend_read_mbps, 5.0),
            (result.frontend_write_mbps, 10.0),
            (result.dedup_rate, 15.0),
            (result.reduction_ratio, 20.0),
            (result.cpu_percent, 25.0),
            (result.ram_percent, 30.0),
        ] {
            assert!((actual - expected).abs() < f64::EPSILON);
        }
    }

    #[test]
    #[ignore = "manual RSS probe; run directly under /usr/bin/time"]
    fn telemetry_rollup_memory_probe() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = TelemetryStore::open(&directory.path().join("telemetry.db")).unwrap();
        let snapshot = TelemetrySnapshot {
            series: vec![
                crate::SeriesPoint {
                    time: "2026-09-05T12:00:00Z".into(),
                    read: 100.0,
                    write: 50.0
                };
                900
            ],
            ..TelemetrySnapshot::default()
        };
        let body = serde_json::to_string(&snapshot).unwrap();
        store
            .locked()
            .unwrap()
            .execute(
                "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<3599)
             INSERT INTO samples_raw SELECT 100000+x,x,?1 FROM n",
                [&body],
            )
            .unwrap();
        let start = std::time::Instant::now();
        store.retain_and_roll_up(103_600).unwrap();
        eprintln!("rollup elapsed: {:?}", start.elapsed());
        assert_eq!(
            store
                .locked()
                .unwrap()
                .query_row("SELECT count(*) FROM samples_10s", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            360
        );
    }

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
    fn audit_export_preserves_operator_feedback_fields() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = ControlStore::open(&directory.path().join("control.db")).expect("open store");
        store
            .audit("admin", "mount", "accepted", "job-42")
            .expect("write audit event");

        let events = store.recent_audit(10).expect("read audit events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].actor, "admin");
        assert_eq!(events[0].action, "mount");
        assert_eq!(events[0].outcome, "accepted");
        assert_eq!(events[0].detail, "job-42");
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
