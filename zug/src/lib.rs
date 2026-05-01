pub mod cli;
pub mod registry;
pub mod schedule;

mod job;
mod middleware;
mod processor;
mod redis_client;
mod stats;
mod worker;

pub use crate::redis_client::{
    with_custom_namespace, RedisConnection, RedisConnectionManager, RedisError, RedisPool,
};
pub(crate) use job::{
    current_timestamp_score, fetch_job_atomic_direct, queue_key, queue_wake_channel,
};
pub use job::{
    job_status, job_status_ttl, opts, perform_async, EnqueueOpts, Job, JobStatus, RetryOpts,
    UnitOfWork, DEFAULT_JOB_STATUS_TTL_SECONDS,
};
pub(crate) use middleware::{Chain, ChainOutcome};
pub use middleware::{ChainIter, ServerMiddleware};
pub use processor::{BalanceStrategy, Processor, ProcessorConfig, QueueConfig, WorkFetcher};
pub use stats::{Counter, StatsPublisher};
pub use worker::{Worker, WorkerContext, WorkerOpts, WorkerRef};
pub use zug_macros::Worker;

#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Message(String),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),

    #[error(transparent)]
    Cron(#[from] cron::error::Error),

    #[error(transparent)]
    BB8(#[from] bb8::RunError<redis::RedisError>),

    #[error(transparent)]
    ChronoRange(#[from] chrono::OutOfRangeError),

    #[error(transparent)]
    Redis(#[from] redis::RedisError),

    #[error(transparent)]
    Any(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, Error>;
