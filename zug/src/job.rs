use crate::{Error, RedisConnection, RedisPool, Result};
use rand::RngExt;
use redis::cmd as redis_cmd;
use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize, Serialize, Serializer,
};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;

pub(crate) const JOB_STATUS_HASH_KEY: &str = "job-status";
const JOB_KEY_PREFIX: &str = "job";
const IN_PROGRESS_KEY_PREFIX: &str = "in-progress";
pub const DEFAULT_JOB_STATUS_TTL_SECONDS: usize = 48 * 60 * 60;

fn default_job_status_ttl_seconds() -> usize {
    DEFAULT_JOB_STATUS_TTL_SECONDS
}

pub(crate) fn queue_key(queue: &str) -> String {
    if queue.starts_with("queue:") {
        queue.to_string()
    } else {
        format!("queue:{queue}")
    }
}

pub(crate) fn job_key(job_id: &str) -> String {
    format!("{JOB_KEY_PREFIX}:{job_id}")
}

pub(crate) fn in_progress_key(job_id: &str) -> String {
    format!("{IN_PROGRESS_KEY_PREFIX}:{job_id}")
}

pub(crate) fn queue_wake_channel(queue: &str) -> String {
    let queue = queue.strip_prefix("queue:").unwrap_or(queue);
    format!("queue:{queue}:wake")
}

pub(crate) fn unix_timestamp_score(datetime: chrono::DateTime<chrono::Utc>) -> f64 {
    datetime.timestamp_millis() as f64 / 1000.0
}

pub(crate) fn current_timestamp_score() -> f64 {
    unix_timestamp_score(chrono::Utc::now())
}

const ENQUEUE_JOB_FUNCTION_BASENAME: &str = "zug_enqueue_job";
const FETCH_JOB_FUNCTION_BASENAME: &str = "zug_fetch_job";
const COMPLETE_JOB_FUNCTION_BASENAME: &str = "zug_complete_job";
const REDIS_FUNCTION_FINGERPRINT_LEN: usize = 16;

static REDIS_FUNCTION_FINGERPRINT: LazyLock<String> = LazyLock::new(|| {
    let mut hasher = Sha256::new();
    hasher.update(include_str!("job.rs").as_bytes());
    hasher.update(ZUG_REDIS_FUNCTION_LIBRARY_TEMPLATE.as_bytes());
    hasher.update(ENQUEUE_JOB_FUNCTION_BASENAME.as_bytes());
    hasher.update(FETCH_JOB_FUNCTION_BASENAME.as_bytes());
    hasher.update(COMPLETE_JOB_FUNCTION_BASENAME.as_bytes());
    hex::encode(hasher.finalize())[..REDIS_FUNCTION_FINGERPRINT_LEN].to_string()
});

static ENQUEUE_JOB_FUNCTION_NAME: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{ENQUEUE_JOB_FUNCTION_BASENAME}_{}",
        REDIS_FUNCTION_FINGERPRINT.as_str()
    )
});
static FETCH_JOB_FUNCTION_NAME: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{FETCH_JOB_FUNCTION_BASENAME}_{}",
        REDIS_FUNCTION_FINGERPRINT.as_str()
    )
});
static COMPLETE_JOB_FUNCTION_NAME: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{COMPLETE_JOB_FUNCTION_BASENAME}_{}",
        REDIS_FUNCTION_FINGERPRINT.as_str()
    )
});
static ZUG_REDIS_FUNCTION_LIBRARY_NAME: LazyLock<String> =
    LazyLock::new(|| format!("zug_{}", REDIS_FUNCTION_FINGERPRINT.as_str()));

static ZUG_REDIS_FUNCTION_LIBRARY: LazyLock<String> = LazyLock::new(|| {
    ZUG_REDIS_FUNCTION_LIBRARY_TEMPLATE
        .replace(
            "__ZUG_REDIS_FUNCTION_LIBRARY_NAME__",
            ZUG_REDIS_FUNCTION_LIBRARY_NAME.as_str(),
        )
        .replace("__ENQUEUE_JOB_FUNCTION_NAME__", enqueue_job_function_name())
        .replace("__FETCH_JOB_FUNCTION_NAME__", fetch_job_function_name())
        .replace(
            "__COMPLETE_JOB_FUNCTION_NAME__",
            complete_job_function_name(),
        )
});

pub(crate) fn enqueue_job_function_name() -> &'static str {
    ENQUEUE_JOB_FUNCTION_NAME.as_str()
}

pub(crate) fn fetch_job_function_name() -> &'static str {
    FETCH_JOB_FUNCTION_NAME.as_str()
}

pub(crate) fn complete_job_function_name() -> &'static str {
    COMPLETE_JOB_FUNCTION_NAME.as_str()
}

pub(crate) fn zug_redis_function_library() -> &'static str {
    ZUG_REDIS_FUNCTION_LIBRARY.as_str()
}

const ZUG_REDIS_FUNCTION_LIBRARY_TEMPLATE: &str = r#"#!lua name=__ZUG_REDIS_FUNCTION_LIBRARY_NAME__
redis.register_function('__ENQUEUE_JOB_FUNCTION_NAME__', function(keys, args)
local queues_key = keys[1]
local job_key = keys[2]
local queue_key = keys[3]
local status_key = keys[4]
local unique_key = keys[5]
local remove_queue_key = keys[6]
local in_progress_key = keys[7]

local queue_name = args[1]
local job_id = args[2]
local job_payload = args[3]
local score = args[4]
local status = args[5]
local status_ttl = args[6]
local unique_ttl = args[7]
local check_existing = args[8]
local lease_token = args[9]

if check_existing == '1' then
    if redis.call('EXISTS', job_key) == 1 or redis.call('HEXISTS', status_key, job_id) == 1 then
        return 0
    end
end

if unique_key ~= '' then
    if redis.call('EXISTS', unique_key) == 1 then
        return 0
    end
    redis.call('SET', unique_key, '', 'EX', unique_ttl)
end

redis.call('SADD', queues_key, queue_name)
redis.call('SET', job_key, job_payload)
redis.call('ZADD', queue_key, score, job_id)

if remove_queue_key ~= '' then
    redis.call('ZREM', remove_queue_key, job_id)
end

if in_progress_key ~= '' then
    if lease_token == '' or redis.call('GET', in_progress_key) ~= lease_token then
        return 0
    end
    redis.call('DEL', in_progress_key)
end

redis.call('HSET', status_key, job_id, status)
redis.call('HEXPIRE', status_key, status_ttl, 'FIELDS', 1, job_id)

return 1
end)

redis.register_function('__FETCH_JOB_FUNCTION_NAME__', function(keys, args)
local queue_key = keys[1]
local status_key = keys[2]

local job_key_prefix = args[1]
local in_progress_key_prefix = args[2]
local now = args[3]
local candidate_limit = tonumber(args[4])
local lease_timeout = args[5]
local status = args[6]
local default_status_ttl = args[7]
local lease_token = args[8]

local job_ids = redis.call('ZRANGEBYSCORE', queue_key, '-inf', now, 'LIMIT', 0, candidate_limit)

for _, job_id in ipairs(job_ids) do
    local in_progress_key = in_progress_key_prefix .. job_id
    local claimed = redis.call('SET', in_progress_key, lease_token, 'NX', 'EX', lease_timeout)

    if claimed then
        local job_key = job_key_prefix .. job_id
        local job_payload = redis.call('GET', job_key)

        if job_payload then
            local status_ttl = default_status_ttl
            local decoded, job = pcall(cjson.decode, job_payload)
            if decoded and job['status_ttl_seconds'] then
                status_ttl = job['status_ttl_seconds']
            end

            redis.call('HSET', status_key, job_id, status)
            redis.call('HEXPIRE', status_key, status_ttl, 'FIELDS', 1, job_id)

            return job_payload
        end

        redis.call('ZREM', queue_key, job_id)
        redis.call('DEL', in_progress_key)
    end
end

return false
end)

redis.register_function('__COMPLETE_JOB_FUNCTION_NAME__', function(keys, args)
local source_queue_key = keys[1]
local destination_queue_key = keys[2]
local job_key = keys[3]
local in_progress_key = keys[4]
local status_key = keys[5]

local job_id = args[1]
local status = args[2]
local status_ttl = args[3]
local lease_token = args[4]

if lease_token == '' or redis.call('GET', in_progress_key) ~= lease_token then
    return 0
end

redis.call('ZREM', source_queue_key, job_id)
if destination_queue_key ~= source_queue_key then
    redis.call('ZREM', destination_queue_key, job_id)
end
redis.call('DEL', job_key)
redis.call('DEL', in_progress_key)
redis.call('HSET', status_key, job_id, status)
redis.call('HEXPIRE', status_key, status_ttl, 'FIELDS', 1, job_id)

return 1
end)

"#;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Deferred,
    #[default]
    Queued,
    InProgress,
    Complete,
    NotFound,
}

impl JobStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deferred => "deferred",
            Self::Queued => "queued",
            Self::InProgress => "in_progress",
            Self::Complete => "complete",
            Self::NotFound => "not_found",
        }
    }

    pub async fn get(redis: &RedisPool, job_id: impl AsRef<str>) -> Result<Self> {
        job_status(redis, job_id).await
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for JobStatus {
    type Err = Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "deferred" => Ok(Self::Deferred),
            "queued" => Ok(Self::Queued),
            "in_progress" => Ok(Self::InProgress),
            "complete" => Ok(Self::Complete),
            "not_found" => Ok(Self::NotFound),
            value => Err(Error::Message(format!("unknown job status: {value}"))),
        }
    }
}

pub async fn job_status(redis: &RedisPool, job_id: impl AsRef<str>) -> Result<JobStatus> {
    let job_id = job_id.as_ref().to_string();
    let mut redis = redis.get().await?;

    if matches!(
        redis
            .hget(JOB_STATUS_HASH_KEY.to_string(), job_id.clone())
            .await?
            .as_deref(),
        Some("complete")
    ) {
        return Ok(JobStatus::Complete);
    }

    if redis.exists(in_progress_key(&job_id)).await? {
        return Ok(JobStatus::InProgress);
    }

    let now = current_timestamp_score();
    for queue in redis.smembers("queues".to_string()).await? {
        if let Some(score) = redis.zscore(queue_key(&queue), job_id.clone()).await? {
            return if score > now {
                Ok(JobStatus::Deferred)
            } else {
                Ok(JobStatus::Queued)
            };
        }
    }

    Ok(JobStatus::NotFound)
}

pub async fn job_status_ttl(redis: &RedisPool, job_id: impl AsRef<str>) -> Result<Option<i64>> {
    let mut redis = redis.get().await?;

    Ok(redis
        .hfield_ttl(JOB_STATUS_HASH_KEY.to_string(), job_id.as_ref().to_string())
        .await?)
}

pub(crate) fn duration_as_seconds(duration: std::time::Duration) -> usize {
    usize::try_from(duration.as_secs()).unwrap_or(usize::MAX)
}

fn status_ttl_seconds_after(delay: std::time::Duration, status_ttl_seconds: usize) -> usize {
    let delay_seconds =
        usize::try_from(delay.as_secs()).unwrap_or(usize::MAX.saturating_sub(status_ttl_seconds));

    status_ttl_seconds.saturating_add(delay_seconds)
}

fn status_ttl_seconds_until(
    datetime: chrono::DateTime<chrono::Utc>,
    status_ttl_seconds: usize,
) -> usize {
    let delay = datetime
        .signed_duration_since(chrono::Utc::now())
        .to_std()
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));

    status_ttl_seconds_after(delay, status_ttl_seconds)
}

#[must_use]
pub fn opts() -> EnqueueOpts {
    EnqueueOpts {
        queue: "default".into(),
        retry: RetryOpts::Yes,
        unique_for: None,
        retry_queue: None,
        defer_by: None,
        status_ttl_seconds: DEFAULT_JOB_STATUS_TTL_SECONDS,
    }
}

pub struct EnqueueOpts {
    pub(crate) queue: String,
    pub(crate) retry: RetryOpts,
    pub(crate) unique_for: Option<std::time::Duration>,
    pub(crate) retry_queue: Option<String>,
    pub(crate) defer_by: Option<std::time::Duration>,
    pub(crate) status_ttl_seconds: usize,
}

impl EnqueueOpts {
    #[must_use]
    pub fn queue_name(&self) -> &str {
        &self.queue
    }

    #[must_use]
    pub fn retry_queue_name(&self) -> Option<&str> {
        self.retry_queue.as_deref()
    }

    #[must_use]
    pub fn queue<S: Into<String>>(self, queue: S) -> Self {
        Self {
            queue: queue.into(),
            ..self
        }
    }

    #[must_use]
    pub fn retry<RO>(self, retry: RO) -> Self
    where
        RO: Into<RetryOpts>,
    {
        Self {
            retry: retry.into(),
            ..self
        }
    }

    #[must_use]
    pub fn unique_for(self, unique_for: std::time::Duration) -> Self {
        Self {
            unique_for: Some(unique_for),
            ..self
        }
    }

    #[must_use]
    pub fn defer_by(self, defer_by: std::time::Duration) -> Self {
        Self {
            defer_by: Some(defer_by),
            ..self
        }
    }

    #[must_use]
    pub fn retry_queue(self, retry_queue: String) -> Self {
        Self {
            retry_queue: Some(retry_queue),
            ..self
        }
    }

    #[must_use]
    pub fn job_status_ttl(self, status_ttl: std::time::Duration) -> Self {
        Self {
            status_ttl_seconds: duration_as_seconds(status_ttl),
            ..self
        }
    }

    pub fn create_job(&self, class: String, args: impl serde::Serialize) -> Result<Job> {
        let args = serde_json::to_value(args)?;

        let args = if args.is_array() {
            args
        } else {
            JsonValue::Array(vec![args])
        };

        Ok(Job {
            queue: self.queue.clone(),
            class,
            job_id: new_job_id(),
            status_ttl_seconds: self.status_ttl_seconds,
            created_at: chrono::Utc::now().timestamp() as f64,
            enqueued_at: None,
            retry: self.retry.clone(),
            args,
            error_message: None,
            error_class: None,
            failed_at: None,
            retry_count: None,
            retried_at: None,
            retry_queue: self.retry_queue.clone(),
            unique_for: self.unique_for,
            lease_token: None,
        })
    }

    pub async fn perform_async(
        self,
        redis: &RedisPool,
        class: String,
        args: impl serde::Serialize,
    ) -> Result<Option<String>> {
        let job = self.create_job(class, args)?;
        let job_id = job.job_id.clone();
        let mut work = UnitOfWork::from_job(job);
        let enqueued = if let Some(duration) = self.defer_by {
            work.schedule(redis, duration).await?
        } else {
            work.enqueue(redis).await?
        };

        Ok(enqueued.then_some(job_id))
    }
}

/// Helper function for enqueueing a worker by name.
pub async fn perform_async(
    redis: &RedisPool,
    class: String,
    queue: String,
    args: impl serde::Serialize,
) -> Result<Option<String>> {
    opts().queue(queue).perform_async(redis, class, args).await
}

pub(crate) fn new_job_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

fn new_lease_token() -> String {
    uuid::Uuid::now_v7().to_string()
}

#[derive(Clone, Debug, PartialEq)]
pub enum RetryOpts {
    Yes,
    Never,
    Max(usize),
}

impl Serialize for RetryOpts {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            RetryOpts::Yes => serializer.serialize_bool(true),
            RetryOpts::Never => serializer.serialize_bool(false),
            RetryOpts::Max(value) => serializer.serialize_u64(value as u64),
        }
    }
}

impl<'de> Deserialize<'de> for RetryOpts {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RetryOptsVisitor;

        impl Visitor<'_> for RetryOptsVisitor {
            type Value = RetryOpts;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a boolean, null, or a positive integer")
            }

            fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value {
                    Ok(RetryOpts::Yes)
                } else {
                    Ok(RetryOpts::Never)
                }
            }

            fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(RetryOpts::Never)
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(RetryOpts::Max(value as usize))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                usize::try_from(value)
                    .map(RetryOpts::Max)
                    .map_err(|_| E::custom("retry count must be a non-negative integer"))
            }
        }

        deserializer.deserialize_any(RetryOptsVisitor)
    }
}

impl From<bool> for RetryOpts {
    fn from(value: bool) -> Self {
        match value {
            true => RetryOpts::Yes,
            false => RetryOpts::Never,
        }
    }
}

impl From<usize> for RetryOpts {
    fn from(value: usize) -> Self {
        RetryOpts::Max(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job {
    pub queue: String,
    pub args: JsonValue,
    pub retry: RetryOpts,
    pub class: String,
    pub job_id: String,
    #[serde(default = "default_job_status_ttl_seconds")]
    pub status_ttl_seconds: usize,
    pub created_at: f64,
    pub enqueued_at: Option<f64>,
    pub failed_at: Option<f64>,
    pub error_message: Option<String>,
    pub error_class: Option<String>,
    pub retry_count: Option<usize>,
    pub retried_at: Option<f64>,
    pub retry_queue: Option<String>,

    #[serde(skip)]
    pub unique_for: Option<std::time::Duration>,

    #[serde(skip)]
    pub lease_token: Option<String>,
}

#[derive(Debug)]
pub struct UnitOfWork {
    pub queue: String,
    pub job: Job,
}

fn unique_key_for_job(job: &Job) -> Result<String> {
    let args_as_json_string = serde_json::to_string(&job.args)?;
    let args_hash = hex::encode(Sha256::digest(&args_as_json_string));

    Ok(format!(
        "zug:unique:{}:{}:{}",
        &job.queue, &job.class, &args_hash
    ))
}

fn namespaced_optional_key(redis: &RedisConnection, key: Option<String>) -> String {
    key.map(|key| redis.namespaced_key(key)).unwrap_or_default()
}

fn namespaced_key_prefix(redis: &RedisConnection, prefix: &str) -> String {
    redis.namespaced_key(prefix.to_string())
}

struct AtomicEnqueue<'a> {
    job: &'a Job,
    score: f64,
    status: JobStatus,
    status_ttl_seconds: usize,
    check_existing_job: bool,
    remove_from_queue: Option<String>,
    clear_in_progress: bool,
    lease_token: Option<String>,
}

async fn enqueue_job_atomic_direct(
    redis: &mut RedisConnection,
    enqueue: AtomicEnqueue<'_>,
) -> Result<bool> {
    let unique = if let Some(duration) = enqueue.job.unique_for {
        Some((
            unique_key_for_job(enqueue.job)?,
            duration_as_seconds(duration).max(1),
        ))
    } else {
        None
    };
    let unique_key = unique.as_ref().map(|(key, _)| key.clone());
    let unique_ttl_seconds = unique.map(|(_, ttl)| ttl).unwrap_or(0);
    let in_progress_key = if enqueue.clear_in_progress {
        Some(in_progress_key(&enqueue.job.job_id))
    } else {
        None
    };
    if enqueue.clear_in_progress && enqueue.lease_token.is_none() {
        return Err(Error::Message(format!(
            "missing lease token for job {}",
            enqueue.job.job_id
        )));
    }

    let mut command = redis_cmd("FCALL");
    command
        .arg(enqueue_job_function_name())
        .arg(7)
        .arg(redis.namespaced_key("queues".to_string()))
        .arg(redis.namespaced_key(job_key(&enqueue.job.job_id)))
        .arg(redis.namespaced_key(queue_key(&enqueue.job.queue)))
        .arg(redis.namespaced_key(JOB_STATUS_HASH_KEY.to_string()))
        .arg(namespaced_optional_key(redis, unique_key))
        .arg(namespaced_optional_key(redis, enqueue.remove_from_queue))
        .arg(namespaced_optional_key(redis, in_progress_key))
        .arg(enqueue.job.queue.clone())
        .arg(enqueue.job.job_id.clone())
        .arg(serde_json::to_string(enqueue.job)?)
        .arg(enqueue.score)
        .arg(enqueue.status.as_str())
        .arg(enqueue.status_ttl_seconds)
        .arg(unique_ttl_seconds)
        .arg(if enqueue.check_existing_job { "1" } else { "0" })
        .arg(enqueue.lease_token.as_deref().unwrap_or(""));

    let enqueued: i64 = redis.query_prepared_command(&mut command).await?;

    Ok(enqueued == 1)
}

pub(crate) async fn fetch_job_atomic_direct(
    redis: &mut RedisConnection,
    queue: String,
    now: f64,
    candidate_limit: isize,
    lease_timeout_seconds: usize,
) -> Result<Option<Job>> {
    let lease_token = new_lease_token();
    let mut command = redis_cmd("FCALL");
    command
        .arg(fetch_job_function_name())
        .arg(2)
        .arg(redis.namespaced_key(queue))
        .arg(redis.namespaced_key(JOB_STATUS_HASH_KEY.to_string()))
        .arg(namespaced_key_prefix(redis, "job:"))
        .arg(namespaced_key_prefix(redis, "in-progress:"))
        .arg(now)
        .arg(candidate_limit)
        .arg(lease_timeout_seconds)
        .arg(JobStatus::InProgress.as_str())
        .arg(DEFAULT_JOB_STATUS_TTL_SECONDS)
        .arg(lease_token.clone());

    let job_raw: Option<String> = redis.query_prepared_command(&mut command).await?;

    Ok(job_raw
        .map(|job_raw| -> Result<Job> {
            let mut job = serde_json::from_str::<Job>(&job_raw)?;
            job.lease_token = Some(lease_token);
            Ok(job)
        })
        .transpose()?)
}

async fn complete_job_atomic_direct(
    redis: &mut RedisConnection,
    source_queue: String,
    destination_queue: String,
    job: &Job,
) -> Result<()> {
    let lease_token = job
        .lease_token
        .as_deref()
        .ok_or_else(|| Error::Message(format!("missing lease token for job {}", job.job_id)))?;
    let mut command = redis_cmd("FCALL");
    command
        .arg(complete_job_function_name())
        .arg(5)
        .arg(redis.namespaced_key(source_queue))
        .arg(redis.namespaced_key(destination_queue))
        .arg(redis.namespaced_key(job_key(&job.job_id)))
        .arg(redis.namespaced_key(in_progress_key(&job.job_id)))
        .arg(redis.namespaced_key(JOB_STATUS_HASH_KEY.to_string()))
        .arg(job.job_id.clone())
        .arg(JobStatus::Complete.as_str())
        .arg(job.status_ttl_seconds)
        .arg(lease_token);

    let completed: i64 = redis.query_prepared_command(&mut command).await?;
    if completed != 1 {
        return Err(Error::Message(format!(
            "job lease no longer owned: {}",
            job.job_id
        )));
    }

    Ok(())
}

impl UnitOfWork {
    #[must_use]
    pub fn from_job(job: Job) -> Self {
        Self {
            queue: queue_key(&job.queue),
            job,
        }
    }

    pub async fn enqueue(&self, redis: &RedisPool) -> Result<bool> {
        let mut redis = redis.get().await?;
        self.enqueue_direct(&mut redis).await
    }

    pub(crate) async fn enqueue_direct(&self, redis: &mut RedisConnection) -> Result<bool> {
        let mut job = self.job.clone();
        job.enqueued_at = Some(chrono::Utc::now().timestamp() as f64);

        let enqueued = enqueue_job_atomic_direct(
            redis,
            AtomicEnqueue {
                score: current_timestamp_score(),
                status: JobStatus::Queued,
                status_ttl_seconds: job.status_ttl_seconds,
                job: &job,
                check_existing_job: true,
                remove_from_queue: None,
                clear_in_progress: false,
                lease_token: None,
            },
        )
        .await?;
        if enqueued {
            publish_queue_wake_direct(redis, &job.queue, &job.job_id).await;
        }

        Ok(enqueued)
    }

    pub async fn complete(&mut self, redis: &RedisPool) -> Result<()> {
        let mut redis = redis.get().await?;
        self.complete_direct(&mut redis).await
    }

    async fn complete_direct(&mut self, redis: &mut RedisConnection) -> Result<()> {
        let destination_queue = queue_key(&self.job.queue);

        complete_job_atomic_direct(redis, self.queue.clone(), destination_queue, &self.job).await?;

        Ok(())
    }

    pub async fn requeue(&mut self, redis: &RedisPool) -> Result<()> {
        if let Some(retry_count) = self.job.retry_count {
            let mut redis = redis.get().await?;
            let retry_at = Self::retry_job_at(retry_count);

            self.job.enqueued_at = None;
            let destination_queue = queue_key(&self.job.queue);

            let enqueued = enqueue_job_atomic_direct(
                &mut redis,
                AtomicEnqueue {
                    score: unix_timestamp_score(retry_at),
                    status: JobStatus::Deferred,
                    status_ttl_seconds: status_ttl_seconds_until(
                        retry_at,
                        self.job.status_ttl_seconds,
                    ),
                    job: &self.job,
                    check_existing_job: false,
                    remove_from_queue: (destination_queue != self.queue)
                        .then_some(self.queue.clone()),
                    clear_in_progress: true,
                    lease_token: self.job.lease_token.clone(),
                },
            )
            .await?;
            if !enqueued {
                return Err(Error::Message(format!(
                    "job lease no longer owned: {}",
                    self.job.job_id
                )));
            }
            if enqueued {
                publish_queue_wake_direct(&mut redis, &self.job.queue, &self.job.job_id).await;
            }
        }

        Ok(())
    }

    fn retry_job_at(count: usize) -> chrono::DateTime<chrono::Utc> {
        let seconds_to_delay = count.pow(4) + 15 + (rand::rng().random_range(0..30) * (count + 1));

        chrono::Utc::now() + chrono::Duration::seconds(seconds_to_delay as i64)
    }

    pub async fn schedule(
        &mut self,
        redis: &RedisPool,
        duration: std::time::Duration,
    ) -> Result<bool> {
        let enqueue_at = chrono::Utc::now() + chrono::Duration::from_std(duration)?;

        let mut redis = redis.get().await?;

        self.job.enqueued_at = None;

        let enqueued = enqueue_job_atomic_direct(
            &mut redis,
            AtomicEnqueue {
                score: unix_timestamp_score(enqueue_at),
                status: JobStatus::Deferred,
                status_ttl_seconds: status_ttl_seconds_after(duration, self.job.status_ttl_seconds),
                job: &self.job,
                check_existing_job: true,
                remove_from_queue: None,
                clear_in_progress: false,
                lease_token: None,
            },
        )
        .await?;
        if enqueued {
            publish_queue_wake_direct(&mut redis, &self.job.queue, &self.job.job_id).await;
        }

        Ok(enqueued)
    }
}

async fn publish_queue_wake_direct(redis: &mut RedisConnection, queue: &str, job_id: &str) {
    if let Err(err) = redis.publish(queue_wake_channel(queue), job_id).await {
        tracing::debug!("failed to publish Zug wake notification: {err:?}");
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn job_status_uses_arq_status_strings() {
        assert_eq!(JobStatus::Deferred.as_str(), "deferred");
        assert_eq!(JobStatus::Queued.as_str(), "queued");
        assert_eq!(JobStatus::InProgress.as_str(), "in_progress");
        assert_eq!(JobStatus::Complete.as_str(), "complete");
        assert_eq!(JobStatus::NotFound.as_str(), "not_found");
        assert_eq!(JobStatus::default(), JobStatus::Queued);
        assert_eq!(JobStatus::InProgress.to_string(), "in_progress");
        assert_eq!("queued".parse::<JobStatus>().unwrap(), JobStatus::Queued);
        assert_eq!(
            serde_json::to_string(&JobStatus::InProgress).unwrap(),
            "\"in_progress\""
        );
        assert_eq!(
            serde_json::from_str::<JobStatus>("\"not_found\"").unwrap(),
            JobStatus::NotFound
        );
    }

    #[test]
    fn job_status_ttl_defaults_when_missing_from_payload() {
        let job: Job = serde_json::from_value(serde_json::json!({
            "queue": "default",
            "args": [],
            "retry": true,
            "class": "TestWorker",
            "job_id": "abc123",
            "created_at": 1337.0
        }))
        .unwrap();

        assert_eq!(job.job_id, "abc123");
        assert_eq!(job.status_ttl_seconds, DEFAULT_JOB_STATUS_TTL_SECONDS);
    }

    #[test]
    fn generated_job_id_is_uuid_v7() {
        let job = opts().create_job("TestWorker".to_string(), ()).unwrap();
        let parsed = uuid::Uuid::parse_str(&job.job_id).unwrap();

        assert_eq!(parsed.get_version_num(), 7);
    }
}
