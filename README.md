Zug
======

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE.md)

Zug is a lightweight Redis-backed async job queue for Rust. It provides typed workers,
immediate jobs, delayed jobs, scheduled cron jobs, retry handling, middleware, queue balancing, and
optional worker process memory metrics.

The library is built on Tokio and stores job state in Redis, making it a small building block for
applications that need background work without a large runtime or service dependency.

Zug requires Redis 7.4 or newer and uses RESP3 connections. Job status retention uses Redis
hash-field expiration (`HEXPIRE`/`HTTL`), which was introduced in Redis 7.4. Worker wakeups use
RESP3 pub/sub as a best-effort latency optimization. Zug registers its Redis Function library when
opening Redis connections and uses `FCALL` for atomic queue state transitions. Function library and
function names include a content fingerprint, so rolling deploys with changed function argument
protocols can run old and new code side by side.

## Install

```toml
[dependencies]
zug = "0.1"
```

With default features, Zug requires Rust 1.88 because the `rss-stats` feature uses `sysinfo`.
If you do not need process RSS metrics, disable default features.

```toml
zug = { version = "0.1", default-features = false }
```

Redis-backed tests use `redis://127.0.0.1/` by default. Set `ZUG_REDIS_URL` when your Redis
7.4+ test server listens on another address or port.

```sh
ZUG_REDIS_URL=redis://127.0.0.1:6380/ cargo test --workspace --tests
```

## Quick Start

Define a worker by implementing `Worker<Args>`. Worker arguments use `serde`, so they can stay
strongly typed in your application code while being serialized into Redis.

```rust
use async_trait::async_trait;
use bb8::Pool;
use zug::{Processor, RedisConnectionManager, Result, Worker};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
struct EmailWorker;

#[derive(Deserialize, Debug, Serialize)]
struct EmailArgs {
    user_guid: String,
}

#[async_trait]
impl Worker<EmailArgs> for EmailWorker {
    fn opts() -> zug::WorkerOpts<EmailArgs, Self> {
        zug::WorkerOpts::new().queue("mailers")
    }

    async fn perform(&self, args: EmailArgs) -> Result<()> {
        info!(user_guid = %args.user_guid, "Sending email");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let manager = RedisConnectionManager::new("redis://127.0.0.1/")?;
    let redis = Pool::builder().build(manager).await?;

    EmailWorker::perform_async(
        &redis,
        EmailArgs {
            user_guid: "USR-123".to_string(),
        },
    )
    .await?;

    EmailWorker::opts()
        .defer_by(Duration::from_secs(60))
        .perform_async(
            &redis,
            EmailArgs {
                user_guid: "USR-456".to_string(),
            },
        )
    .await?;

    let mut processor = Processor::new(redis.clone());
    processor.register(EmailWorker);
    processor.run().await;

    Ok(())
}
```

`Processor::run` owns the worker loop and runs until cancelled. In an application that also serves
HTTP traffic, spawn it into its own Tokio task and keep the cancellation token if you need graceful
shutdown.

## Logging

Zug emits logs and worker lifecycle events through `tracing`. The library does not install a
global subscriber; applications should choose their own subscriber and output format. For console
output, initialize `tracing-subscriber` in your binary.

```rust
tracing_subscriber::fmt::init();
```

For structured JSON logs:

```rust
tracing_subscriber::fmt()
    .json()
    .init();
```

## Enqueue Jobs

Use a worker's typed helpers for the normal path.

```rust
let job_id = EmailWorker::perform_async(
    &redis,
    EmailArgs {
        user_guid: "USR-123".to_string(),
    },
)
.await?;
```

`perform_async` returns `Some(job_id)` when the job is written, or `None` when the job was skipped
because an existing job/unique lock already owns that enqueue slot.

Use `defer_by` for one-time delayed jobs. Delayed jobs are stored directly in their target queue
with a future score, so no separate delayed-job mover is needed.

```rust
EmailWorker::opts()
    .defer_by(std::time::Duration::from_secs(300))
    .perform_async(
        &redis,
        EmailArgs {
            user_guid: "USR-123".to_string(),
        },
    )
.await?;
```

See [zug/examples/delayed.rs](zug/examples/delayed.rs) for a complete delayed job example.

You can also enqueue by worker name when you want to construct jobs dynamically.

```rust
zug::perform_async(
    &redis,
    EmailWorker::class_name(),
    "mailers".to_string(),
    EmailArgs {
        user_guid: "USR-123".to_string(),
    },
)
.await?;
```

## Worker Options

Worker defaults live in `Worker::opts`. You can override the same options when enqueueing a job.

```rust
#[async_trait]
impl Worker<EmailArgs> for EmailWorker {
    fn opts() -> zug::WorkerOpts<EmailArgs, Self> {
        zug::WorkerOpts::new()
            .queue("mailers")
            .retry(5usize)
            .retry_queue("mailers_retry")
            .defer_by(std::time::Duration::from_secs(300))
            .unique_for(std::time::Duration::from_secs(30))
            .job_status_ttl(std::time::Duration::from_secs(24 * 60 * 60))
    }

    async fn perform(&self, args: EmailArgs) -> Result<()> {
        info!(user_guid = %args.user_guid, "Sending email");
        Ok(())
    }
}
```

Available options:

- `queue(name)` sets the worker's default enqueue target queue. `Processor::new(redis)` listens to
    this queue after the worker is registered.
- `retry(true)` keeps default retry behavior.
- `retry(false)` disables retries for that job.
- `retry(count)` sets a maximum retry count.
- `retry_queue(name)` sends failed retry attempts to another queue. Automatic processor queue
    discovery includes this queue too.
- `defer_by(duration)` stores the job in its target queue with a future score. `perform_async` is
    still the enqueue method; the defer setting only changes the score and initial status.
- `unique_for(duration)` prevents duplicate jobs with the same worker, queue, and args hash during the duration.
- `job_status_ttl(duration)` controls how long Redis keeps the job's final status after the job leaves a queue. The default is 48 hours.

Job creation writes the payload, queue membership, sorted-set score, uniqueness lock, and status
field in one Redis Function call. Duplicate unique jobs return `None` from `perform_async`.
See
[zug/examples/unique.rs](zug/examples/unique.rs) for a complete example.

## Worker State

For application dependencies such as database pools, HTTP clients, or configuration, attach typed
state to the processor and read it from `perform_with_context`. This is similar to arq's `ctx` and
axum's `State` pattern.

```rust
#[derive(Clone)]
struct AppState {
    db: sqlx::PgPool,
}

#[async_trait]
impl Worker<EmailArgs> for EmailWorker {
    async fn perform_with_context(
        &self,
        ctx: zug::WorkerContext,
        args: EmailArgs,
    ) -> Result<()> {
        let state = ctx.state::<AppState>()?;

        sqlx::query("insert into email_events (user_guid) values ($1)")
            .bind(&args.user_guid)
            .execute(&state.db)
            .await
            .map_err(|err| zug::Error::Message(err.to_string()))?;

        Ok(())
    }
}

let state = AppState { db };
let mut processor = Processor::new(redis.clone()).with_state(state);
processor.register(EmailWorker);
```

`WorkerContext::state::<T>()` returns an `Arc<T>`, so the same state can be shared across all worker
tasks. `WorkerContext::redis_pool()` gives access to Zug's Redis pool for enqueueing follow-up jobs
or reading job-related data.

For worker-specific dependencies, storing the dependency directly on the worker is also a good
option:

```rust
#[derive(Clone)]
struct EmailWorker {
    db: sqlx::PgPool,
}

let mut processor = Processor::new(redis.clone());
processor.register(EmailWorker { db });
```

## Job Status

Zug follows arq's status model by deriving active state from Redis structures rather than from
fields in the serialized `Job`. A completed job is represented by a `complete` field in the
`job-status` hash, which is Zug's equivalent of arq's result marker. Active jobs are checked by
the `in-progress:<job_id>` lease key and then by scanning known queue sorted sets. The sorted-set
score is the ready timestamp: scores in the future are `Deferred`, and scores at or before now are
`Queued`.

Status hash fields use the job's `job_status_ttl` value, defaulting to 48 hours, so completed job
state expires automatically. This requires Redis 7.4 or newer because Zug uses hash-field
expiration through `HEXPIRE`.

The public `JobStatus` enum follows the same shape as arq:

- `Deferred`: the job id is in a queue sorted set with a score later than now.
- `Queued`: the job id is in a queue sorted set with a score at or before now.
- `InProgress`: a processor has fetched the job and holds its lease key.
- `Complete`: the job reached a terminal state, including success, disabled retries, or exhausted retries.
- `NotFound`: no result marker, lease key, or queue entry exists for the job id.

```rust
use zug::{job_status, JobStatus, UnitOfWork, Worker};

let job = EmailWorker::opts()
    .queue("mailers")
    .into_opts()
    .create_job(EmailWorker::class_name(), EmailArgs {
        user_guid: "USR-123".to_string(),
    })?;
let job_id = job.job_id.clone();

UnitOfWork::from_job(job).enqueue(&redis).await?;

assert_eq!(job_status(&redis, &job_id).await?, JobStatus::Queued);
```

## Running Workers

Create a processor, register worker instances, and run it. By default, registering a worker adds
that worker's `Worker::opts().queue(...)` queue to the processor. If the worker does not customize
its queue, the processor listens to `default` for that worker.

```rust
let manager = zug::RedisConnectionManager::new("redis://127.0.0.1/")?;
let redis = bb8::Pool::builder().build(manager).await?;

let mut processor = zug::Processor::new(redis.clone());
processor.register(EmailWorker);
processor.run().await;
```

Use `Processor::with_queues(redis, queues)` when a process should consume an explicit queue set.
This is useful when you intentionally run separate worker processes for different queues, or when
jobs are produced outside of the registered worker defaults.

If you register workers with `#[derive(Worker)]`, you can also build the processor from registered
worker defaults:

```rust
let mut processor = zug::Processor::from_registered_workers(redis.clone())?;
processor.run().await;
```

This uses each registered worker's `Worker::opts().queue(...)` value, any `retry_queue(...)`, plus
any extra queues declared with `zug::register_queue!(...)`, as the queues this process consumes.

You can tune concurrency and queue behavior with `ProcessorConfig`.

```rust
use zug::{BalanceStrategy, ProcessorConfig, QueueConfig};

let config = ProcessorConfig::default()
    .concurrency(8)
    .balance_strategy(BalanceStrategy::RoundRobin)
    .poll_interval(std::time::Duration::from_millis(500))
    .lease_timeout(std::time::Duration::from_secs(30))
    .queue_config(
        "reports".to_string(),
        QueueConfig::default().concurrency(1),
    );

let processor = zug::Processor::with_queues(
    redis.clone(),
    vec!["mailers".to_string(), "reports".to_string()],
)
.with_config(config);
```

`BalanceStrategy::RoundRobin` rotates the shared worker queue order after each fetch. Use
`BalanceStrategy::None` when strict queue order is desired.

`concurrency(n)` sets how many jobs may run at the same time in that worker pool.
`queue_config(queue, QueueConfig::default().concurrency(n))` creates a queue-specific worker pool.
That queue is removed from the shared worker pool, so `concurrency(1)` means only one job from this
queue can execute in this process at a time.

### Config-Driven Runner

For an arq-style worker command, compile a small binary in your application and let Zug build the
processor from registered workers plus a TOML config file. The command has to live in the final
application binary because `inventory` collects registrations at link time. Zug re-exports the
worker registration macros, so application crates only need the normal `zug` dependency.

Register workers with `#[derive(Worker)]` after importing `zug::Worker`. The derive macro only
emits the inventory registration used by the config-driven runner; it does not implement the
`Worker<Args>` trait or the worker's `perform` method. Args are inferred by Rust from your
`impl Worker<Args> for WorkerType` block. Derived workers must implement `Default` because the
runner needs a way to construct them during inventory registration. Use `WorkerContext` state for
database pools, HTTP clients, configuration, and other runtime dependencies.

Default-constructed worker:

```rust
use zug::Worker;

#[derive(Worker, Clone, Default)]
#[zug(name = "email.reminder")]
struct EmailWorker;

#[async_trait]
impl Worker<EmailArgs> for EmailWorker {
    fn opts() -> zug::WorkerOpts<EmailArgs, Self> {
        zug::WorkerOpts::new().queue("mailers")
    }

    async fn perform(&self, args: EmailArgs) -> zug::Result<()> {
        send_email(args).await
    }
}
```

Workers that need runtime state should read typed state from `WorkerContext`.

```rust
use zug::Worker;

#[derive(Clone)]
struct AppState {
    db: sqlx::PgPool,
}

#[derive(Worker, Clone, Default)]
#[zug(name = "report.nightly")]
struct ReportWorker;

#[async_trait]
impl Worker<ReportArgs> for ReportWorker {
    async fn perform_with_context(
        &self,
        ctx: zug::WorkerContext,
        args: ReportArgs,
    ) -> zug::Result<()> {
        let state = ctx.state::<AppState>()?;
        run_report(&state.db, args).await
    }
}

let state = AppState { db };
let mut processor = zug::Processor::from_registered_workers(redis.clone())?
    .with_state(state);
```

The inventory registration is emitted by the derive macro. If workers live in another crate, make
sure that crate is linked by the final worker binary. Put the link marker at module scope in the
worker binary:

```rust
zug::register_worker!(my_app_workers::*);
```

This marker does not scan worker types itself; it ensures the crate containing the worker registration
submissions is linked into the final binary so `inventory` can collect them. Additional queue names
that are not any worker's default queue can be registered with
`zug::register_queue!("critical")` when you want the runner to discover them without a `queues`
entry in the TOML file.

The same inventory registrations can be used without the TOML runner. To consume every registered
worker's default queue, build the processor directly from registered workers:

```rust
zug::register_worker!(my_app_workers::*);
let mut processor = zug::Processor::from_registered_workers(redis.clone())?;
processor.run().await;
```

To consume an explicit queue subset while still registering all inventory workers, pass the processor
to the macro:

```rust
let mut processor = zug::Processor::with_queues(redis.clone(), vec!["critical".to_string()]);
zug::register_worker!(&mut processor, my_app_workers::*)?;
processor.run().await;
```

Then add a binary such as `src/bin/worker.rs`:

```rust

#[tokio::main]
async fn main() -> zug::Result<()> {
    tracing_subscriber::fmt::init();
    // Only needed when the registered workers live in another crate.
    zug::register_worker!(my_app_workers::*);
    zug::cli::run_cli().await
}
```

Run it with `cargo run --bin worker -- run --config zug.toml`. With no arguments, the runner
uses `zug.toml`; `ZUG_REDIS_URL` is used when `redis_url` is omitted.

```toml
redis_url = "redis://127.0.0.1/"
queues = [
    { name = "critical", concurrency = 4 },
    { name = "mailers" },
    { name = "reports", concurrency = 1 },
]

[processor]
concurrency = 8
balance_strategy = "round_robin"
poll_interval_ms = 500
lease_timeout_secs = 30

[[schedules]]
name = "daily email reminder"
worker_name = "email.reminder"
cron = "0 0 8 * * *"
queue = "mailers"
retry = 3
args = { user_guid = "USR-123" }
```

`queues` is the set of queues this process will fetch from. When `queues` is omitted, the runner
uses registered queue names plus the default queues from registered workers. `[processor].concurrency`
sets the shared worker pool concurrency. A queue entry with `concurrency` gets its own worker pool
and is not processed by the shared pool; for example, `{ name = "reports", concurrency = 1 }` makes
report jobs run serially in this process.
Scheduled jobs must set exactly one of `worker` or `worker_name`. `worker` is the exact serialized
worker class name; by default this is the full Rust type path, such as
`my_app::workers::EmailWorker`. `worker_name` is the stable config alias registered by
`#[zug(name = "email.reminder")]` on `#[derive(Worker)]`. Duplicate `worker_name` aliases are
rejected when the runner builds the processor. When `queue` is omitted, the runner uses the worker's
default queue from `Worker::opts`.

Zug uses at-least-once execution. A job remains in Redis until it succeeds or reaches a terminal
failure state. Fetching a job creates an `in-progress:<job_id>` lease with `SET NX EX`; if a worker
process exits before acknowledging the job, the lease eventually expires and another processor can
run the same job again. Design worker code to be idempotent when duplicate execution would matter.
Job ids are generated as UUID v7 strings.

`poll_interval` controls how often an idle worker scans Redis for due jobs. When a job is enqueued
or retried, Zug also publishes a wake notification to that queue's RESP3 pub/sub channel. In a
distributed deployment, processors on different servers may all receive the notification and wake
up, but only the processor that acquires the Redis lease executes the job. Pub/sub messages are not
used as the source of truth; if a message is lost, the next poll still finds due work.

The main Redis keys are:

- `queue:<name>`: sorted set of job ids scored by their ready time.
- `job:<job_id>`: serialized job payload.
- `in-progress:<job_id>`: lease key with a TTL while a worker is running the job.
- `job-status`: hash of latest job status values with Redis 7.4 hash-field TTLs.

### Redis Operation Sketch

The core queue operations are intentionally small. Immediate enqueueing uses `FCALL` to write the
job payload, add the job id to the target queue zset with a score of now, and record status with its
field TTL. After the function succeeds, Zug publishes a wake hint:

```redis
SET job:<job_id> <serialized job>
ZADD queue:<name> <now> <job_id>
HSET job-status <job_id> queued
HEXPIRE job-status <status_ttl_seconds> FIELDS 1 <job_id>
PUBLISH queue:<name>:wake <job_id>
```

Delayed jobs and retries use the same queue zset with a future score. The job is visible in Redis,
but processors will not claim it until its score is due:

```redis
SET job:<job_id> <serialized job>
ZADD queue:<name> <ready_at> <job_id>
HSET job-status <job_id> deferred
HEXPIRE job-status <seconds until ready_at + status ttl> FIELDS 1 <job_id>
PUBLISH queue:<name>:wake <job_id>
```

Fetching work uses `FCALL` to scan due job ids, acquire a tokenized lease, read the payload, clean up
orphaned queue entries, and mark the job `in_progress`. This is the distributed coordination point:
every worker may see the same due job id, but only one worker can create `in-progress:<job_id>`
before the lease expires. The lease value is a per-fetch token that must match when the worker later
completes or retries the job.

```redis
ZRANGEBYSCORE queue:<name> -inf <now> LIMIT 0 50
SET in-progress:<job_id> <lease_token> NX EX <lease_timeout_seconds>
GET job:<job_id>
HSET job-status <job_id> in_progress
HEXPIRE job-status <status_ttl_seconds> FIELDS 1 <job_id>
```

When work finishes successfully, or when retries are disabled or exhausted, Zug uses `FCALL` to
acknowledge the job by removing queue state and keeping only the terminal status for the configured
status TTL. The completion function first verifies that `in-progress:<job_id>` still contains the
worker's lease token; stale workers cannot complete jobs after another worker has reacquired them.

```redis
ZREM queue:<name> <job_id>
DEL job:<job_id>
DEL in-progress:<job_id>
HSET job-status <job_id> complete
HEXPIRE job-status <status_ttl_seconds> FIELDS 1 <job_id>
```

When a job should retry, Zug verifies the same lease token, then updates the payload, moves the same
job id to its retry time, clears the current lease, and records `deferred`:

```redis
SET job:<job_id> <serialized job with retry_count and error fields>
ZADD queue:<retry queue> <retry_at> <job_id>
ZREM queue:<previous queue> <job_id>
DEL in-progress:<job_id>
HSET job-status <job_id> deferred
HEXPIRE job-status <seconds until retry_at + status ttl> FIELDS 1 <job_id>
PUBLISH queue:<retry queue>:wake <job_id>
```

## Scheduled Jobs

Scheduled jobs run from cron expressions. Schedule definitions live in the processor's memory, like
arq cron jobs, so deploying updated code and restarting workers updates the active schedules. Each
scheduled run uses a deterministic job id derived from the schedule name and scheduled timestamp, so
multiple processors can safely evaluate the same schedule without enqueueing duplicate jobs for the
same time.

```rust
zug::schedule::builder("0 0 8 * * *")?
    .name("Daily email reminder")
    .queue("mailers")
    .args(EmailArgs {
        user_guid: "USR-123".to_string(),
    })?
    .register(&mut processor, EmailWorker)
    .await?;
```

The cron expression includes seconds. The example above runs at 08:00:00 UTC every day.

## Middleware

Server middleware can inspect or short-circuit jobs before the worker runs. Middleware receives the
job payload, the worker reference, and the Redis pool.

```rust
use async_trait::async_trait;
use zug::{ChainIter, Job, RedisPool, Result, ServerMiddleware, WorkerRef};
use std::sync::Arc;

struct SkipExpiredUsers;

#[async_trait]
impl ServerMiddleware for SkipExpiredUsers {
    async fn call(
        &self,
        chain: ChainIter,
        job: &Job,
        worker: Arc<WorkerRef>,
        redis: RedisPool,
    ) -> Result<()> {
        if job.args.to_string().contains("USR-EXPIRED") {
            return Ok(());
        }

        chain.next(job, worker, redis).await
    }
}

processor.using(SkipExpiredUsers).await;
```

## Redis Connection Pools

`RedisConnectionManager` configures redis-rs connections for RESP3 automatically, so the URL does
not need a `?protocol=resp3` suffix.

Using separate Redis pools for enqueueing and fetching can keep request-path enqueueing responsive
when many workers are busy fetching or retrying jobs.

```rust
let manager = zug::RedisConnectionManager::new("redis://127.0.0.1/")?;
let redis_enqueue = bb8::Pool::builder().build(manager.clone()).await?;
let redis_fetch = bb8::Pool::builder().build(manager).await?;

let mut processor = zug::Processor::new(redis_fetch);
processor.register(EmailWorker);
tokio::spawn(async move {
    processor.run().await;
});

EmailWorker::perform_async(
    &redis_enqueue,
    EmailArgs {
        user_guid: "USR-123".to_string(),
    },
)
.await?;
```

## Redis Namespacing

Use a connection customizer to prefix all Redis keys used by Zug. This is useful when multiple
applications share the same Redis database.

```rust
let manager = zug::RedisConnectionManager::new("redis://127.0.0.1/")?;
let redis = bb8::Pool::builder()
    .connection_customizer(zug::with_custom_namespace("my_app".to_string()))
    .build(manager)
    .await?;
```

## Worker State

Workers are regular Rust values and only need to implement `Clone` when you want to clone shared
clients into them before registration.

```rust
#[derive(Clone)]
struct ReportWorker {
    redis: zug::RedisPool,
}

processor.register(ReportWorker {
    redis: redis.clone(),
});
```

## Worker Names and Argument Coercion

By default, Zug uses the full Rust type path as the serialized worker name, including the crate
and module path. This avoids collisions when two modules define workers with the same type name.
The config-driven runner does not guess short type names. In TOML schedules, use `worker` for the
exact serialized class name, or use `worker_name` for a stable alias declared on the derive macro.

```rust
use zug::Worker;

#[derive(Worker, Clone, Default)]
#[zug(name = "email.reminder")]
struct EmailWorker;
```

```toml
[[schedules]]
name = "daily email reminder"
worker_name = "email.reminder"
cron = "0 0 8 * * *"
```

Override `class_name` only when you intentionally want to change the serialized worker class stored
in Redis.

```rust
#[async_trait]
impl Worker<EmailArgs> for EmailWorker {
    fn class_name() -> String
    where
        Self: Sized,
    {
        "email.reminder".to_string()
    }

    async fn perform(&self, args: EmailArgs) -> Result<()> {
        info!(user_guid = %args.user_guid, "Sending email");
        Ok(())
    }
}
```

Zug unwraps single JSON-array arguments before deserializing worker args. If your worker expects a
`Vec<T>` that may contain one item, or a single-item tuple `(T,)`, disable argument coercion.

```rust
#[async_trait]
impl Worker<Vec<EmailArgs>> for BatchEmailWorker {
    fn disable_argument_coercion(&self) -> bool {
        true
    }

    async fn perform(&self, args: Vec<EmailArgs>) -> Result<()> {
        for arg in args {
            info!(user_guid = %arg.user_guid, "Sending batch email");
        }

        Ok(())
    }
}
```

## Feature Flags

The `rss-stats` feature is enabled by default. It publishes an `rss` field in worker process
heartbeats by reading the current process's resident memory with `sysinfo`. When disabled, Zug
still publishes process metadata but reports `rss` as `0`.

```toml
zug = { version = "0.1", default-features = false }
```

Job enqueueing, fetching, execution, retries, delayed jobs, and scheduled jobs do not depend on this
feature.

## Examples

- [zug/examples/demo.rs](zug/examples/demo.rs) shows workers, enqueueing, middleware, and scheduled jobs.
- [zug/examples/cli_worker.rs](zug/examples/cli_worker.rs) shows the TOML-driven runner and worker registration macros.
- [zug/examples/delayed.rs](zug/examples/delayed.rs) shows one-time delayed jobs.
- [zug/examples/unique.rs](zug/examples/unique.rs) shows unique job locks.
- [zug/examples/namespaced_demo.rs](zug/examples/namespaced_demo.rs) shows Redis key namespacing.

## License and Origin

Zug is distributed under the MIT license. This crate is inspired by
`sidekiq-rs`

https://github.com/film42/sidekiq-rs

and `arq`

https://github.com/python-arq/arq

