//! Async job-worker subsystem.
//!
//! Each request (thumbnail, hash, compress) is dispatched into a `tokio::task`
//! so the HTTP handler returns immediately with a **job id**.  The client can:
//!
//! * `GET /jobs`                — list recent jobs (default last 50, filter by status)
//! * `GET /jobs/active`         — list currently running jobs
//! * `GET /jobs/:id`            — inspect one job
//! * `POST /jobs/:id/cancel`    — cancel a running or queued job

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;
use uuid::Uuid;

// ── public types ────────────────────────────────────────────────

/// What kind of work was requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    Thumbnail,
    Hash,
    HashCompare,
    Compress,
}

impl fmt::Display for JobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                JobType::Thumbnail => "thumbnail",
                JobType::Hash => "hash",
                JobType::HashCompare => "hash_compare",
                JobType::Compress => "compress",
            }
        )
    }
}

/// Current lifecycle state of the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                JobStatus::Running => "running",
                JobStatus::Completed => "completed",
                JobStatus::Cancelled => "cancelled",
                JobStatus::Failed => "failed",
            }
        )
    }
}

/// Metadata attached to a job when it is created.
#[derive(Debug, Clone, Serialize)]
pub struct JobMeta {
    /// Original uploaded filename (if available).
    pub file_name: Option<String>,
    /// Size in bytes of the uploaded file.
    pub file_size: Option<u64>,
    /// Optional extra context (compress profile, encoder, etc.).
    pub extra: Option<serde_json::Value>,
}

/// A single tracked job — cloneable for JSON serialisation.
#[derive(Debug, Clone, Serialize)]
pub struct JobInfo {
    pub id: String,
    #[serde(rename = "type")]
    pub job_type: JobType,
    pub status: JobStatus,
    pub meta: JobMeta,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Elapsed wall-clock time in ms (best-effort proxy for CPU time).
    /// Precise kernel CPU time requires platform-specific syscalls.
    pub elapsed_ms: Option<u64>,
    /// Result message on error, empty otherwise.
    pub error: Option<String>,
    /// Arbitrary result payload set by the worker (e.g. hash string, compare distance).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

// ── internal state ──────────────────────────────────────────────

/// Per-job state protected by its own mutex.
struct JobStateInner {
    info: Mutex<JobInfo>,
    cancelled: AtomicBool,
}

/// Shared handle to a single job (cheap to clone — only an Arc).
#[derive(Clone)]
pub(crate) struct JobState {
    inner: Arc<JobStateInner>,
}

impl JobState {
    fn new(id: String, job_type: JobType, meta: JobMeta) -> Self {
        let info = JobInfo {
            id,
            job_type,
            status: JobStatus::Running,
            meta,
            created_at: chrono::Utc::now(),
            started_at: Some(chrono::Utc::now()),
            finished_at: None,
            elapsed_ms: None,
            error: None,
            result: None,
        };
        Self {
            inner: Arc::new(JobStateInner {
                info: Mutex::new(info),
                cancelled: AtomicBool::new(false),
            }),
        }
    }

    fn cancel(&self) -> bool {
        self.inner
            .cancelled
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    fn set_done(&self, status: JobStatus, elapsed_ms: u64, error: Option<String>) {
        self.set_done_with_result(status, elapsed_ms, error, None);
    }

    fn set_done_with_result(
        &self,
        status: JobStatus,
        elapsed_ms: u64,
        error: Option<String>,
        result: Option<serde_json::Value>,
    ) {
        let mut info = self.inner.info.lock().unwrap();
        info.status = status;
        info.elapsed_ms = Some(elapsed_ms);
        info.error = error;
        info.result = result;
        info.finished_at = Some(chrono::Utc::now());
    }

    fn snapshot(&self) -> JobInfo {
        self.inner.info.lock().unwrap().clone()
    }
}

/// Shared job registry — the only source of truth.
#[derive(Clone)]
pub struct JobRegistry(pub Arc<Mutex<Vec<JobState>>>);

const DEFAULT_LIST_LIMIT: usize = 50;

impl JobRegistry {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    pub fn insert(&self, state: JobState) {
        self.0.lock().unwrap().push(state);
    }

    /// Return a snapshot of all jobs matching the optional status filter,
    /// limited to `limit` most recent entries.
    pub fn list(&self, status: Option<JobStatusFilter>, limit: usize) -> Vec<JobInfo> {
        let guard = self.0.lock().unwrap();
        let filtered: Vec<_> = guard
            .iter()
            .filter(|j| status.map_or(true, |s| j.inner.info.lock().unwrap().status == s.into()))
            .map(|j| j.snapshot())
            .collect();
        let start = filtered.len().saturating_sub(limit);
        filtered.into_iter().skip(start).collect()
    }

    pub fn get(&self, id: &str) -> Option<JobState> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|j| j.inner.info.lock().unwrap().id == id)
            .cloned()
    }

    /// Prune old completed/failed/cancelled jobs to avoid unbounded memory growth.
    pub fn prune(&self, max_age: Duration) {
        let now = chrono::Utc::now();
        let mut guard = self.0.lock().unwrap();
        guard.retain(|j| {
            let info = j.inner.info.lock().unwrap();
            let still_running = matches!(info.status, JobStatus::Running);
            drop(info);
            let not_expired = j.inner.info.lock().unwrap().created_at
                + chrono::Duration::from_std(max_age).unwrap_or_default()
                > now;
            still_running || not_expired
        });
    }

    /// Spawn a periodic pruning task that runs every `interval` and removes
    /// jobs older than `max_age`. Returns a `tokio::task::JoinHandle`.
    pub fn spawn_pruner(
        &self,
        interval: Duration,
        max_age: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn({
            let this = self.clone();
            async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tick.tick().await;
                    this.prune(max_age);
                }
            }
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct JobListQuery {
    /// Filter by status string (running, completed, cancelled, failed).
    pub status: Option<String>,
    /// Maximum number of jobs to return. Default 50.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    DEFAULT_LIST_LIMIT
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatusFilter {
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl From<JobStatusFilter> for JobStatus {
    fn from(v: JobStatusFilter) -> JobStatus {
        match v {
            JobStatusFilter::Running => JobStatus::Running,
            JobStatusFilter::Completed => JobStatus::Completed,
            JobStatusFilter::Cancelled => JobStatus::Cancelled,
            JobStatusFilter::Failed => JobStatus::Failed,
        }
    }
}

impl JobStatusFilter {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "running" => Some(JobStatusFilter::Running),
            "completed" => Some(JobStatusFilter::Completed),
            "cancelled" => Some(JobStatusFilter::Cancelled),
            "failed" => Some(JobStatusFilter::Failed),
            _ => None,
        }
    }
}

// ── API handlers ────────────────────────────────────────────────

pub async fn handle_list_jobs(
    State(reg): State<JobRegistry>,
    Query(q): Query<JobListQuery>,
) -> impl IntoResponse {
    let status = q.status.as_deref().and_then(JobStatusFilter::parse);
    let jobs = reg.list(status, q.limit);

    let items: Vec<serde_json::Value> = jobs
        .into_iter()
        .map(|j| {
            serde_json::json!({
                "id": j.id,
                "type": j.job_type.to_string(),
                "status": j.status.to_string(),
                "file_name": j.meta.file_name,
                "file_size": j.meta.file_size,
                "extra": j.meta.extra,
                "created_at": j.created_at.to_rfc3339(),
                "started_at": j.started_at.map(|t| t.to_rfc3339()),
                "finished_at": j.finished_at.map(|t| t.to_rfc3339()),
                "elapsed_ms": j.elapsed_ms,
                "error": j.error,
                "result": j.result,
            })
        })
        .collect();

    Json(serde_json::json!({
        "ok": true,
        "total": items.len(),
        "limit": q.limit,
        "jobs": items,
    }))
}

pub async fn handle_get_job(
    State(reg): State<JobRegistry>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match reg.get(&id) {
        Some(state) => {
            let info = state.snapshot();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "ok": true,
                    "job": {
                        "id": info.id,
                        "type": info.job_type.to_string(),
                        "status": info.status.to_string(),
                        "file_name": info.meta.file_name,
                        "file_size": info.meta.file_size,
                        "extra": info.meta.extra,
                        "created_at": info.created_at.to_rfc3339(),
                        "started_at": info.started_at.map(|t| t.to_rfc3339()),
                        "finished_at": info.finished_at.map(|t| t.to_rfc3339()),
                        "elapsed_ms": info.elapsed_ms,
                        "error": info.error,
                        "result": info.result,
                    }
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "ok": false,
                "error": format!("job not found: {}", id),
            })),
        ),
    }
}

pub async fn handle_cancel_job(
    State(reg): State<JobRegistry>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match reg.get(&id) {
        Some(state) => {
            if state.is_cancelled() {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "ok": false,
                        "error": "job already cancelled",
                    })),
                );
            }
            let current_status = state.snapshot().status;
            if current_status != JobStatus::Running {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "ok": false,
                        "error": format!("cannot cancel job in status '{}'", current_status),
                    })),
                );
            }
            state.cancel();

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "ok": true,
                    "message": format!("job {} cancelling...", id),
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "ok": false,
                "error": format!("job not found: {}", id),
            })),
        ),
    }
}

/// `GET /jobs/active` — list currently running jobs.
pub async fn handle_list_active_jobs(
    State(reg): State<JobRegistry>,
    Query(q): Query<JobListQuery>,
) -> impl IntoResponse {
    let jobs = reg.list(Some(JobStatusFilter::Running), q.limit);

    let items: Vec<serde_json::Value> = jobs
        .into_iter()
        .map(|j| {
            serde_json::json!({
                "id": j.id,
                "type": j.job_type.to_string(),
                "status": j.status.to_string(),
                "file_name": j.meta.file_name,
                "file_size": j.meta.file_size,
                "extra": j.meta.extra,
                "created_at": j.created_at.to_rfc3339(),
                "started_at": j.started_at.map(|t| t.to_rfc3339()),
                "elapsed_ms": j.elapsed_ms,
                "error": j.error,
                "result": j.result,
            })
        })
        .collect();

    Json(serde_json::json!({
        "ok": true,
        "total": items.len(),
        "limit": q.limit,
        "jobs": items,
    }))
}

// ── job dispatcher ──────────────────────────────────────────────

/// Spawn a background job and register it. Returns the job ID immediately.
///
/// `work` is called on Tokio's blocking thread pool (`spawn_blocking`).
/// It receives an `Arc<AtomicBool>` that the worker should check periodically
/// to respect cancellation requests.
///
/// `on_complete` is called after work finishes (success or failure) and can
/// store an arbitrary result in the job metadata.
pub fn spawn_job<F, G>(
    registry: JobRegistry,
    job_type: JobType,
    meta: JobMeta,
    work: F,
    on_complete: G,
) -> String
where
    F: FnOnce(std::sync::Arc<AtomicBool>) + Send + 'static,
    G: FnOnce() -> Option<serde_json::Value> + Send + 'static,
{
    let id = Uuid::new_v4().to_string();
    let state = JobState::new(id.clone(), job_type, meta);
    registry.insert(state.clone());

    let cancelled = std::sync::Arc::new(AtomicBool::new(false));

    tokio::task::spawn_blocking(move || {
        let start = Instant::now();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Check cancellation before starting.
            if cancelled.load(Ordering::SeqCst) {
                return;
            }
            work(cancelled.clone());
        }));

        let elapsed = start.elapsed().as_millis() as u64;

        let done_status = if cancelled.load(Ordering::SeqCst) {
            JobStatus::Cancelled
        } else if result.is_err() {
            JobStatus::Failed
        } else {
            JobStatus::Completed
        };

        let error = if result.is_err() {
            Some("worker panicked".to_string())
        } else if done_status == JobStatus::Cancelled {
            Some("cancelled by user".to_string())
        } else {
            None
        };

        let result_data = match done_status {
            JobStatus::Completed => on_complete(),
            _ => None,
        };

        state.set_done_with_result(done_status, elapsed, error, result_data);
    });

    id
}
