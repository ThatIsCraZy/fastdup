use argon2::password_hash::{PasswordHash, PasswordHasher as _, PasswordVerifier as _, SaltString};
use argon2::{Argon2, Params, Version};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore as _;
use rusqlite::{OptionalExtension as _, params};
use sha2::{Digest as _, Sha256};

use crate::store::{ControlStore, StoreError};
use crate::unix_seconds;

const INITIAL_USERNAME: &str = "admin";
const INITIAL_PASSWORD: &str = "fastdup01.";
const SESSION_SECONDS: i64 = 8 * 60 * 60;
const MAX_FAILURES: u32 = 5;
const LOCK_SECONDS: i64 = 5 * 60;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoginResult {
    pub session_token: String,
    pub csrf_token: String,
    pub username: String,
    pub must_change_password: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedSession {
    pub username: String,
    pub csrf_token: String,
    pub must_change_password: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("authentication failed")]
    InvalidCredentials,
    #[error("account is temporarily locked")]
    Locked,
    #[error("session is invalid or expired")]
    InvalidSession,
    #[error("password must contain between 12 and 128 characters")]
    WeakPassword,
    #[error("authentication database failed: {0}")]
    Store(#[from] StoreError),
    #[error("authentication database failed: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("password hash failed")]
    PasswordHash,
}

#[derive(Clone, Debug)]
pub struct SessionManager {
    store: ControlStore,
}

impl SessionManager {
    pub fn new(store: ControlStore) -> Result<Self, AuthError> {
        let manager = Self { store };
        manager.ensure_initial_admin()?;
        Ok(manager)
    }

    fn ensure_initial_admin(&self) -> Result<(), AuthError> {
        let connection = self.store.connection();
        let connection = connection.lock().map_err(|_| StoreError::Poisoned)?;
        let present = connection
            .query_row(
                "SELECT 1 FROM users WHERE username = ?1",
                [INITIAL_USERNAME],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !present {
            let password_hash = hash_password(INITIAL_PASSWORD)?;
            connection.execute(
                "INSERT INTO users(username, password_hash, must_change, failed_attempts, locked_until) VALUES(?1, ?2, 1, 0, 0)",
                params![INITIAL_USERNAME, password_hash],
            )?;
        }
        Ok(())
    }

    pub fn login(&self, username: &str, password: &str) -> Result<LoginResult, AuthError> {
        let now = unix_seconds();
        let connection = self.store.connection();
        let connection = connection.lock().map_err(|_| StoreError::Poisoned)?;
        let user = connection
            .query_row(
                "SELECT password_hash, must_change, failed_attempts, locked_until FROM users WHERE username = ?1",
                [username],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, bool>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((password_hash, must_change, failed_attempts, locked_until)) = user else {
            return Err(AuthError::InvalidCredentials);
        };
        if locked_until > now {
            return Err(AuthError::Locked);
        }
        let parsed = PasswordHash::new(&password_hash).map_err(|_| AuthError::PasswordHash)?;
        if Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_err()
        {
            let failures = failed_attempts.saturating_add(1);
            let next_lock = if failures >= MAX_FAILURES {
                now + LOCK_SECONDS
            } else {
                0
            };
            connection.execute(
                "UPDATE users SET failed_attempts = ?1, locked_until = ?2 WHERE username = ?3",
                params![failures, next_lock, username],
            )?;
            return Err(AuthError::InvalidCredentials);
        }
        connection.execute(
            "UPDATE users SET failed_attempts = 0, locked_until = 0 WHERE username = ?1",
            [username],
        )?;
        let token = random_token(32);
        let csrf = random_token(24);
        connection.execute(
            "INSERT INTO sessions(token_hash, username, csrf_token, expires_at, last_seen) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![token_hash(&token), username, csrf, now + SESSION_SECONDS, now],
        )?;
        Ok(LoginResult {
            session_token: token,
            csrf_token: csrf,
            username: username.to_owned(),
            must_change_password: must_change,
        })
    }

    pub fn authenticate(&self, token: &str) -> Result<AuthenticatedSession, AuthError> {
        let now = unix_seconds();
        let connection = self.store.connection();
        let connection = connection.lock().map_err(|_| StoreError::Poisoned)?;
        connection.execute("DELETE FROM sessions WHERE expires_at <= ?1", [now])?;
        let session = connection
            .query_row(
                "SELECT sessions.username, sessions.csrf_token, users.must_change FROM sessions JOIN users ON users.username = sessions.username WHERE sessions.token_hash = ?1 AND sessions.expires_at > ?2",
                params![token_hash(token), now],
                |row| Ok(AuthenticatedSession { username: row.get(0)?, csrf_token: row.get(1)?, must_change_password: row.get(2)? }),
            )
            .optional()?;
        let Some(session) = session else {
            return Err(AuthError::InvalidSession);
        };
        connection.execute(
            "UPDATE sessions SET last_seen = ?1 WHERE token_hash = ?2",
            params![now, token_hash(token)],
        )?;
        Ok(session)
    }

    pub fn change_password(
        &self,
        session_token: &str,
        current_password: &str,
        new_password: &str,
    ) -> Result<LoginResult, AuthError> {
        if !(12..=128).contains(&new_password.chars().count()) {
            return Err(AuthError::WeakPassword);
        }
        let session = self.authenticate(session_token)?;
        let connection = self.store.connection();
        let connection = connection.lock().map_err(|_| StoreError::Poisoned)?;
        let password_hash: String = connection.query_row(
            "SELECT password_hash FROM users WHERE username = ?1",
            [&session.username],
            |row| row.get(0),
        )?;
        let parsed = PasswordHash::new(&password_hash).map_err(|_| AuthError::PasswordHash)?;
        Argon2::default()
            .verify_password(current_password.as_bytes(), &parsed)
            .map_err(|_| AuthError::InvalidCredentials)?;
        let replacement = hash_password(new_password)?;
        connection.execute(
            "UPDATE users SET password_hash = ?1, must_change = 0 WHERE username = ?2",
            params![replacement, session.username],
        )?;
        connection.execute(
            "DELETE FROM sessions WHERE username = ?1",
            [&session.username],
        )?;
        drop(connection);
        self.login(&session.username, new_password)
    }

    pub fn logout(&self, token: &str) -> Result<(), AuthError> {
        self.store
            .connection()
            .lock()
            .map_err(|_| StoreError::Poisoned)?
            .execute(
                "DELETE FROM sessions WHERE token_hash = ?1",
                [token_hash(token)],
            )?;
        Ok(())
    }
}

fn hash_password(password: &str) -> Result<String, AuthError> {
    let mut salt_bytes = [0_u8; 16];
    rand::rng().fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes).map_err(|_| AuthError::PasswordHash)?;
    let params = Params::new(19_456, 2, 1, None).map_err(|_| AuthError::PasswordHash)?;
    Argon2::new(argon2::Algorithm::Argon2id, Version::V0x13, params)
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| AuthError::PasswordHash)
}

fn random_token(bytes: usize) -> String {
    let mut random = vec![0_u8; bytes];
    rand::rng().fill_bytes(&mut random);
    URL_SAFE_NO_PAD.encode(random)
}

fn token_hash(token: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_login_requires_a_password_change_and_rotates_session() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = ControlStore::open(&directory.path().join("control.db")).expect("open store");
        let sessions = SessionManager::new(store).expect("session manager");
        let login = sessions
            .login("admin", "fastdup01.")
            .expect("initial login");
        assert!(login.must_change_password);
        let changed = sessions
            .change_password(&login.session_token, "fastdup01.", "a-longer-secret-2026")
            .expect("change password");
        assert!(!changed.must_change_password);
        assert!(matches!(
            sessions.authenticate(&login.session_token),
            Err(AuthError::InvalidSession)
        ));
    }
}
