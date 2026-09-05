use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, COOKIE, SET_COOKIE};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use fastdup_control::{
    AgentControl, ApplianceControl, Command, ControlEvent, ControlProblem, ControlStore,
    SessionManager, ShareSettings, TelemetryStore, TlsIdentity,
};
use futures_util::StreamExt as _;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::BroadcastStream;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

#[derive(RustEmbed)]
#[folder = "../../web/fastdup-ui/dist"]
struct UiAssets;

#[derive(Clone)]
struct AppState {
    control: Arc<dyn ApplianceControl>,
    sessions: SessionManager,
    store: ControlStore,
    telemetry: TelemetryStore,
    fingerprint: Arc<RwLock<String>>,
    tls: Option<axum_server::tls_rustls::RustlsConfig>,
    secure_cookie: bool,
    tls_directory: PathBuf,
    history_slots: Arc<tokio::sync::Semaphore>,
    hostnames: Vec<String>,
    tls_update: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    problem: ControlProblem,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.problem)).into_response()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionResponse {
    ui_language: String,
    username: String,
    csrf_token: String,
    must_change_password: bool,
    certificate_fingerprint: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PasswordRequest {
    current_password: String,
    new_password: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PrincipalsResponse {
    users: Vec<String>,
    groups: Vec<String>,
}

#[derive(Deserialize)]
struct HistoryQuery {
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<usize>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let state_directory = PathBuf::from(
        std::env::var_os("FASTDUP_CONTROL_STATE_DIR")
            .unwrap_or_else(|| "/var/lib/fastdup/control".into()),
    );
    let store = ControlStore::open(&state_directory.join("control.db"))?;
    let telemetry = TelemetryStore::open(&state_directory.join("telemetry.db"))?;
    let sessions = SessionManager::new(store.clone())?;
    let tls_directory = state_directory.join("tls");
    let hostnames = vec![hostname()];
    let identity = TlsIdentity::load_or_generate(&tls_directory, &hostnames)?;
    let insecure = std::env::var_os("FASTDUP_CONTROL_INSECURE_HTTP").is_some();
    let tls = if insecure {
        None
    } else {
        Some(
            axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &identity.certificate_path,
                &identity.private_key_path,
            )
            .await?,
        )
    };
    let control: Arc<dyn ApplianceControl> =
        AgentControl::new(std::env::var_os("FASTDUP_AGENT_SOCKET").map_or_else(
            || PathBuf::from(fastdup_control::CONTROL_SOCKET_PATH),
            PathBuf::from,
        ));
    let state = AppState {
        control,
        sessions,
        store,
        telemetry,
        fingerprint: Arc::new(RwLock::new(identity.fingerprint)),
        tls: tls.clone(),
        secure_cookie: !insecure,
        tls_directory,
        history_slots: Arc::new(tokio::sync::Semaphore::new(2)),
        hostnames,
        tls_update: Arc::new(tokio::sync::Mutex::new(())),
    };
    let application = routes(state)
        .layer(CompressionLayer::new())
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http());
    let address = SocketAddr::from(([0, 0, 0, 0], 8080));
    if insecure {
        let listener = tokio::net::TcpListener::bind(address).await?;
        axum::serve(listener, application).await?;
    } else {
        axum_server::bind_rustls(address, tls.expect("TLS config exists in secure mode"))
            .serve(application.into_make_service())
            .await?;
    }
    Ok(())
}

fn routes(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/session", get(session))
        .route("/api/v1/session/login", post(login))
        .route("/api/v1/session/password", put(change_password))
        .route("/api/v1/session/language", put(change_language))
        .route("/api/v1/session/logout", post(logout))
        .route("/api/v1/tls/regenerate", post(regenerate_tls))
        .route("/api/v1/tls/import", post(import_tls))
        .route("/api/v1/users", get(list_users).post(create_user))
        .route("/api/v1/appliance", get(appliance_telemetry))
        .route("/api/v1/snapshot", get(appliance_snapshot))
        .route("/api/v1/repository/commands", post(submit_command))
        .route("/api/v1/shares", get(shares).post(upsert_share))
        .route("/api/v1/shares/{id}", delete(delete_share))
        .route("/api/v1/samba/principals", get(samba_principals))
        .route("/api/v1/telemetry/history", get(history))
        .route("/api/v1/audit", get(audit_log))
        .route("/api/v1/events", get(events))
        .fallback(static_asset)
        .with_state(state)
}

async fn session(State(state): State<AppState>, headers: HeaderMap) -> Result<Response, ApiError> {
    let token = session_token(&headers).ok_or_else(unauthorized)?;
    let session = state.sessions.authenticate(&token).map_err(auth_error)?;
    Ok(Json(SessionResponse {
        ui_language: state
            .store
            .user_ui_language(&session.username)
            .map_err(store_error)?,
        username: session.username,
        csrf_token: session.csrf_token,
        must_change_password: session.must_change_password,
        certificate_fingerprint: fingerprint(&state),
    })
    .into_response())
}

async fn login(
    State(state): State<AppState>,
    Json(request): Json<LoginRequest>,
) -> Result<Response, ApiError> {
    let result = match state.sessions.login(&request.username, &request.password) {
        Ok(result) => result,
        Err(error) => {
            state
                .store
                .audit(&request.username, "login", "failure", &error.to_string())
                .map_err(store_error)?;
            return Err(auth_error(error));
        }
    };
    state
        .store
        .audit(&request.username, "login", "success", "web session created")
        .map_err(store_error)?;
    let body = SessionResponse {
        ui_language: state
            .store
            .user_ui_language(&result.username)
            .map_err(store_error)?,
        username: result.username,
        csrf_token: result.csrf_token,
        must_change_password: result.must_change_password,
        certificate_fingerprint: fingerprint(&state),
    };
    let mut response = Json(body).into_response();
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(&session_cookie(&result.session_token, state.secure_cookie))
            .map_err(internal_error)?,
    );
    Ok(response)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LanguageRequest {
    language: String,
}

async fn change_language(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<LanguageRequest>,
) -> Result<Response, ApiError> {
    let token = session_token(&headers).ok_or_else(unauthorized)?;
    let session = state.sessions.authenticate(&token).map_err(auth_error)?;
    require_csrf(&headers, &session.csrf_token)?;
    if !matches!(request.language.as_str(), "de" | "en") {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            problem: ControlProblem::new("invalid_language", "Supported UI languages: de, en"),
        });
    }
    state
        .store
        .set_user_ui_language(&session.username, &request.language)
        .map_err(store_error)?;
    Ok(Json(serde_json::json!({ "uiLanguage": request.language })).into_response())
}

async fn change_password(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PasswordRequest>,
) -> Result<Response, ApiError> {
    let token = session_token(&headers).ok_or_else(unauthorized)?;
    let session = state.sessions.authenticate(&token).map_err(auth_error)?;
    require_csrf(&headers, &session.csrf_token)?;
    let result = state
        .sessions
        .change_password(&token, &request.current_password, &request.new_password)
        .map_err(auth_error)?;
    state
        .store
        .audit(
            &session.username,
            "password_change",
            "success",
            "all previous sessions invalidated",
        )
        .map_err(store_error)?;
    let body = SessionResponse {
        ui_language: state
            .store
            .user_ui_language(&result.username)
            .map_err(store_error)?,
        username: result.username,
        csrf_token: result.csrf_token,
        must_change_password: result.must_change_password,
        certificate_fingerprint: fingerprint(&state),
    };
    let mut response = Json(body).into_response();
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(&session_cookie(&result.session_token, state.secure_cookie))
            .map_err(internal_error)?,
    );
    Ok(response)
}

async fn logout(State(state): State<AppState>, headers: HeaderMap) -> Result<Response, ApiError> {
    if let Some(token) = session_token(&headers) {
        let session = state.sessions.authenticate(&token).ok();
        if let Some(session) = session.as_ref() {
            require_csrf(&headers, &session.csrf_token)?;
        }
        state.sessions.logout(&token).map_err(auth_error)?;
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(&expired_session_cookie(state.secure_cookie))
            .map_err(internal_error)?,
    );
    Ok(response)
}

async fn regenerate_tls(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let session = authenticate(&state, &headers, false)?;
    require_csrf(&headers, &session.csrf_token)?;
    let _guard = state.tls_update.lock().await;
    let (certificate, key) =
        TlsIdentity::self_signed_pem(&state.hostnames).map_err(internal_error)?;
    activate_tls(
        &state,
        &session.username,
        "tls_regenerate",
        certificate,
        key,
    )
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportTlsRequest {
    pfx: String,
    password: String,
}

async fn import_tls(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ImportTlsRequest>,
) -> Result<Response, ApiError> {
    use base64::Engine as _;
    let session = authenticate(&state, &headers, false)?;
    require_csrf(&headers, &session.csrf_token)?;
    let _guard = state.tls_update.try_lock().map_err(|_| {
        control_error(ControlProblem::new(
            "tls_busy",
            "Another certificate update is in progress",
        ))
    })?;
    let archive = base64::engine::general_purpose::STANDARD
        .decode(&request.pfx)
        .map_err(|_| bad_request("tls_invalid", "Invalid PFX encoding"))?;
    let (certificate, key) = tokio::task::spawn_blocking(move || {
        fastdup_control::decode_pfx(&archive, &request.password)
    })
    .await
    .map_err(internal_error)?
    .map_err(|message| bad_request("tls_invalid", message))?;
    activate_tls(&state, &session.username, "tls_import", certificate, key).await
}

async fn activate_tls(
    state: &AppState,
    username: &str,
    action: &str,
    certificate: Vec<u8>,
    key: Vec<u8>,
) -> Result<Response, ApiError> {
    // Validate certificate/key compatibility before touching the active on-disk identity.
    let config = axum_server::tls_rustls::RustlsConfig::from_pem(certificate.clone(), key.clone())
        .await
        .map_err(|_| {
            bad_request(
                "tls_invalid",
                "Certificate and private key are incompatible with TLS",
            )
        })?;
    let directory = state.tls_directory.clone();
    let live_tls = state.tls.clone();
    let live_fingerprint = Arc::clone(&state.fingerprint);
    let identity = tokio::task::spawn_blocking(move || {
        let identity = TlsIdentity::publish_pem(&directory, &certificate, &key)?;
        // Complete activation even if the importing browser disconnects.
        if let Some(tls) = live_tls { tls.reload_from_config(config.get_inner()); }
        if let Ok(mut fingerprint) = live_fingerprint.write() { identity.fingerprint.clone_into(&mut fingerprint); }
        Ok::<_, fastdup_control::TlsIdentityError>(identity)
    }).await.map_err(internal_error)?.map_err(internal_error)?;
    state
        .store
        .audit(
            username,
            action,
            "success",
            "certificate reloaded without service restart",
        )
        .map_err(store_error)?;
    Ok(Json(serde_json::json!({"certificateFingerprint":identity.fingerprint})).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateUserRequest {
    username: String,
    password: String,
}

async fn list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.sessions.list_users().map_err(internal_error)?).into_response())
}

async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateUserRequest>,
) -> Result<Response, ApiError> {
    let session = authenticate(&state, &headers, false)?;
    require_csrf(&headers, &session.csrf_token)?;
    let manager = state.sessions.clone();
    let username = request.username.clone();
    tokio::task::spawn_blocking(move || manager.create_user(&request.username, &request.password))
        .await
        .map_err(internal_error)?
        .map_err(|error| match error {
            fastdup_control::AuthError::UserExists => control_error(ControlProblem::new(
                "user_exists",
                "Username already exists",
            )),
            fastdup_control::AuthError::InvalidUsername
            | fastdup_control::AuthError::WeakPassword => {
                bad_request("user_invalid", error.to_string())
            }
            _ => internal_error(error),
        })?;
    state
        .store
        .audit(&session.username, "user_create", "success", &username)
        .map_err(store_error)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({"username":username,"mustChangePassword":true})),
    )
        .into_response())
}

fn bad_request(code: &str, message: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        problem: ControlProblem::new(code, message),
    }
}

async fn appliance_telemetry(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(
        state
            .control
            .inspect()
            .await
            .map_err(control_error)?
            .telemetry,
    )
    .into_response())
}

async fn appliance_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authenticate(&state, &headers, false)?;
    let mut snapshot = state.control.inspect().await.map_err(control_error)?;
    snapshot.certificate_fingerprint = fingerprint(&state);
    Ok(Json(snapshot).into_response())
}

async fn submit_command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(command): Json<Command>,
) -> Result<Response, ApiError> {
    let session = authenticate(&state, &headers, false)?;
    require_csrf(&headers, &session.csrf_token)?;
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map_or_else(|| Uuid::new_v4().to_string(), str::to_owned);
    let job = state
        .control
        .submit(command, key)
        .await
        .map_err(control_error)?;
    state
        .store
        .audit(&session.username, &job.kind, "accepted", &job.id)
        .map_err(store_error)?;
    Ok((StatusCode::ACCEPTED, Json(job)).into_response())
}

async fn shares(State(state): State<AppState>, headers: HeaderMap) -> Result<Response, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.control.inspect().await.map_err(control_error)?.shares).into_response())
}

async fn samba_principals(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(PrincipalsResponse {
        users: local_principals("/etc/passwd", 2),
        groups: local_principals("/etc/group", 2),
    })
    .into_response())
}

async fn upsert_share(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(share): Json<ShareSettings>,
) -> Result<Response, ApiError> {
    let session = authenticate(&state, &headers, false)?;
    require_csrf(&headers, &session.csrf_token)?;
    let expected_revision = (share.revision > 0).then_some(share.revision);
    let command = Command::UpsertShare {
        expected_revision,
        share,
    };
    let job = state
        .control
        .submit(command, Uuid::new_v4().to_string())
        .await
        .map_err(control_error)?;
    Ok((StatusCode::ACCEPTED, Json(job)).into_response())
}

async fn delete_share(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let session = authenticate(&state, &headers, false)?;
    require_csrf(&headers, &session.csrf_token)?;
    let revision = headers
        .get("if-match")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim_matches('"').parse::<u64>().ok())
        .ok_or_else(|| ApiError {
            status: StatusCode::PRECONDITION_REQUIRED,
            problem: ControlProblem::new("revision_required", "If-Match revision is required"),
        })?;
    let job = state
        .control
        .submit(
            Command::DeleteShare {
                id,
                expected_revision: revision,
            },
            Uuid::new_v4().to_string(),
        )
        .await
        .map_err(control_error)?;
    Ok((StatusCode::ACCEPTED, Json(job)).into_response())
}

async fn history(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HistoryQuery>,
) -> Result<Response, ApiError> {
    authenticate(&state, &headers, false)?;
    let now = fastdup_control::unix_seconds();
    let from = query.from.unwrap_or(now - 900);
    let to = query.to.unwrap_or(now);
    let limit = query.limit.unwrap_or(1_500).min(1_500);
    let permit = Arc::clone(&state.history_slots)
        .try_acquire_owned()
        .map_err(|_| ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            problem: ControlProblem::new(
                "history_busy",
                "Eine Historienabfrage läuft bereits. Bitte erneut versuchen.",
            ),
        })?;
    let telemetry = state.telemetry;
    let samples = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        telemetry.query(from, to, limit)
    })
    .await
    .map_err(internal_error)?
    .map_err(store_error)?;
    Ok(Json(samples).into_response())
}

async fn audit_log(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authenticate(&state, &headers, false)?;
    Ok(Json(state.store.recent_audit(10_000).map_err(store_error)?).into_response())
}

async fn events(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    authenticate(&state, &headers, false)?;
    let stream = BroadcastStream::new(state.control.subscribe()).filter_map(|event| async move {
        let event = event.ok()?;
        let (kind, sequence, payload) = match event {
            ControlEvent::Snapshot { snapshot } => {
                let sequence = snapshot.sequence.to_string();
                ("snapshot", sequence, serde_json::to_string(&snapshot).ok()?)
            }
            ControlEvent::Job { job } => {
                let sequence = job.updated_at.to_string();
                ("job", sequence, serde_json::to_string(&job).ok()?)
            }
            ControlEvent::Alert { code, message } => {
                let payload = serde_json::json!({ "code": code, "message": message });
                (
                    "alert",
                    fastdup_control::unix_seconds().to_string(),
                    serde_json::to_string(&payload).ok()?,
                )
            }
            ControlEvent::Audit { action, outcome } => {
                let payload = serde_json::json!({ "action": action, "outcome": outcome });
                (
                    "audit",
                    fastdup_control::unix_seconds().to_string(),
                    serde_json::to_string(&payload).ok()?,
                )
            }
        };
        Some(Ok(Event::default().event(kind).id(sequence).data(payload)))
    });
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

async fn static_asset(request: Request<Body>) -> Response {
    let requested = request.uri().path().trim_start_matches('/');
    let asset = if requested.is_empty() {
        "index.html"
    } else {
        requested
    };
    let (served_name, contents) = if let Some(contents) = UiAssets::get(asset) {
        (asset, contents.data.into_owned())
    } else if let Some(index) = UiAssets::get("index.html") {
        ("index.html", index.data.into_owned())
    } else {
        ("index.html", Vec::new())
    };
    let mime = mime_guess::from_path(served_name).first_or_octet_stream();
    let mut response = Response::new(Body::from(contents));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_str(mime.as_ref())
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    response.headers_mut().insert(
        CACHE_CONTROL,
        if served_name == "index.html" {
            HeaderValue::from_static("no-cache")
        } else {
            HeaderValue::from_static("public, max-age=31536000, immutable")
        },
    );
    response
}

fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
    allow_password_change: bool,
) -> Result<fastdup_control::AuthenticatedSession, ApiError> {
    let token = session_token(headers).ok_or_else(unauthorized)?;
    let session = state.sessions.authenticate(&token).map_err(auth_error)?;
    if session.must_change_password && !allow_password_change {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            problem: ControlProblem::new(
                "password_change_required",
                "Das Initialpasswort muss zuerst geändert werden",
            ),
        });
    }
    Ok(session)
}

fn require_csrf(headers: &HeaderMap, expected: &str) -> Result<(), ApiError> {
    let valid = headers
        .get("x-csrf-token")
        .and_then(|value| value.to_str().ok())
        == Some(expected);
    if valid {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::FORBIDDEN,
            problem: ControlProblem::new("csrf_failed", "CSRF token is missing or invalid"),
        })
    }
}

fn session_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|cookie| cookie.trim().split_once('='))
        .find_map(|(name, value)| (name == "fastdup_session").then(|| value.to_owned()))
}

fn session_cookie(token: &str, secure: bool) -> String {
    let secure_attribute = if secure { "; Secure" } else { "" };
    format!(
        "fastdup_session={token}; Path=/; Max-Age=28800{secure_attribute}; HttpOnly; SameSite=Strict"
    )
}

fn expired_session_cookie(secure: bool) -> String {
    let secure_attribute = if secure { "; Secure" } else { "" };
    format!("fastdup_session=; Path=/; Max-Age=0{secure_attribute}; HttpOnly; SameSite=Strict")
}

fn auth_error(error: fastdup_control::AuthError) -> ApiError {
    let message = error.to_string();
    drop(error);
    ApiError {
        status: StatusCode::UNAUTHORIZED,
        problem: ControlProblem::new("authentication_failed", message),
    }
}

fn unauthorized() -> ApiError {
    ApiError {
        status: StatusCode::UNAUTHORIZED,
        problem: ControlProblem::new("authentication_required", "Anmeldung erforderlich"),
    }
}

fn control_error(problem: ControlProblem) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        problem,
    }
}

fn store_error(error: fastdup_control::StoreError) -> ApiError {
    let message = error.to_string();
    drop(error);
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        problem: ControlProblem::new("store_failed", message),
    }
}

fn internal_error(error: impl std::fmt::Display) -> ApiError {
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        problem: ControlProblem::new("internal", error.to_string()),
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname").map_or_else(
        |_| "fastdup-appliance".to_owned(),
        |value| value.trim().to_owned(),
    )
}

fn fingerprint(state: &AppState) -> String {
    state
        .fingerprint
        .read()
        .map_or_else(|_| "nicht verfügbar".to_owned(), |value| value.clone())
}

fn local_principals(path: &str, numeric_field: usize) -> Vec<String> {
    let mut names = std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let fields = line.split(':').collect::<Vec<_>>();
            let id = fields.get(numeric_field)?.parse::<u32>().ok()?;
            (id >= 1_000).then(|| fields.first().copied().unwrap_or_default().to_owned())
        })
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

#[cfg(test)]
mod tests {
    use super::{LanguageRequest, session_cookie};

    #[test]
    fn language_request_cannot_select_another_user() {
        assert!(
            serde_json::from_str::<LanguageRequest>(
                r#"{"language":"en","username":"someone-else"}"#
            )
            .is_err()
        );
        assert_eq!(
            serde_json::from_str::<LanguageRequest>(r#"{"language":"en"}"#)
                .unwrap()
                .language,
            "en"
        );
    }

    #[test]
    fn session_cookie_matches_transport_security() {
        let insecure = session_cookie("token", false);
        assert!(!insecure.contains("; Secure"));
        assert!(insecure.contains("; HttpOnly; SameSite=Strict"));

        let secure = session_cookie("token", true);
        assert!(secure.contains("; Secure; HttpOnly; SameSite=Strict"));
    }
}

#[cfg(test)]
mod settings_api_tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, AppState) {
        let directory = tempfile::tempdir().unwrap();
        let store = ControlStore::open(&directory.path().join("control.db")).unwrap();
        let sessions = SessionManager::new(store.clone()).unwrap();
        let state = AppState {
            control: fastdup_control::InMemoryControl::new(fastdup_control::ApplianceSnapshot {
                telemetry: fastdup_control::TelemetrySnapshot::default(),
                targets: vec![],
                repository: None,
                settings: fastdup_control::RepositorySettings::default(),
                shares: vec![],
                jobs: vec![],
                certificate_fingerprint: String::new(),
            }),
            sessions,
            store,
            telemetry: TelemetryStore::open(&directory.path().join("telemetry.db")).unwrap(),
            fingerprint: Arc::new(RwLock::new(String::new())),
            tls: None,
            secure_cookie: true,
            tls_directory: directory.path().join("tls"),
            history_slots: Arc::new(tokio::sync::Semaphore::new(2)),
            hostnames: vec!["localhost".into()],
            tls_update: Arc::new(tokio::sync::Mutex::new(())),
        };
        (directory, state)
    }

    #[tokio::test]
    async fn user_and_pfx_endpoints_require_login_password_change_and_csrf() {
        let (_directory, state) = fixture();
        assert_eq!(
            list_users(State(state.clone()), HeaderMap::new())
                .await
                .unwrap_err()
                .status,
            StatusCode::UNAUTHORIZED
        );
        let login = state.sessions.login("admin", "fastdup01.").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            format!("fastdup_session={}", login.session_token)
                .parse()
                .unwrap(),
        );
        assert!(
            list_users(State(state.clone()), headers.clone())
                .await
                .is_err()
        );
        let login = state
            .sessions
            .change_password(&login.session_token, "fastdup01.", "long-test-password")
            .unwrap();
        headers.insert(
            "cookie",
            format!("fastdup_session={}", login.session_token)
                .parse()
                .unwrap(),
        );
        assert!(
            create_user(
                State(state.clone()),
                headers.clone(),
                Json(CreateUserRequest {
                    username: "alice".into(),
                    password: "alice-start-password".into()
                })
            )
            .await
            .is_err()
        );
        assert!(
            import_tls(
                State(state.clone()),
                headers.clone(),
                Json(ImportTlsRequest {
                    pfx: String::new(),
                    password: String::new()
                })
            )
            .await
            .is_err()
        );
        assert_eq!(state.sessions.list_users().unwrap().len(), 1);
        headers.insert("x-csrf-token", login.csrf_token.parse().unwrap());
        let result = create_user(
            State(state.clone()),
            headers.clone(),
            Json(CreateUserRequest {
                username: "alice".into(),
                password: "alice-start-password".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.status(), StatusCode::CREATED);
        assert_eq!(state.sessions.list_users().unwrap().len(), 2);
        let invalid = import_tls(
            State(state.clone()),
            headers,
            Json(ImportTlsRequest {
                pfx: "invalid".into(),
                password: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
        assert!(!state.tls_directory.join("active").exists());
    }
}
