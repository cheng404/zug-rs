use super::Result;
use crate::{
    current_timestamp_score, fetch_job_atomic_direct, queue_key, queue_wake_channel, registry,
    schedule::{self, ScheduledJob},
    Chain, Counter, Error, JobStatus, RedisPool, ServerMiddleware, StatsPublisher, UnitOfWork,
    Worker, WorkerContext, WorkerRef,
};
use indexmap::IndexSet;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use tokio::select;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const FETCH_CANDIDATE_LIMIT: isize = 50;

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum WorkFetcher {
    NoWorkFound,
    Done,
}

#[derive(Clone)]
pub struct Processor {
    redis: RedisPool,
    queues: VecDeque<String>,
    queue_names: IndexSet<String>,
    auto_register_queues: bool,
    scheduled_jobs: Vec<ScheduledJob>,
    workers: BTreeMap<String, Arc<WorkerRef>>,
    chain: Chain,
    busy_jobs: Counter,
    wake_notify: Arc<Notify>,
    cancellation_token: CancellationToken,
    config: ProcessorConfig,
    context: WorkerContext,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct ProcessorConfig {
    /// The number of worker tasks that can run at the same time. Adjust as needed based on
    /// your workload and resource (cpu/memory/etc) usage.
    ///
    /// This config value controls how many workers are spawned to handle the queues this processor
    /// consumes. These workers will be shared across all of these queues.
    ///
    /// If your workload is largely CPU-bound (computationally expensive), this should probably
    /// match your CPU count. This is the default.
    ///
    /// If your workload is largely IO-bound (e.g. reading from a DB, making web requests and
    /// waiting for responses, etc), this can probably be quite a bit higher than your CPU count.
    pub concurrency: usize,

    /// The strategy for balancing the priority of fetching queues' jobs from Redis. Defaults
    /// to [`BalanceStrategy::RoundRobin`].
    ///
    /// Queue zsets are checked in the order the queues are registered. This means that if the first
    /// queue in the list always has due jobs, the other queues will never have their jobs run. To
    /// mitigate this, a [`BalanceStrategy`] can be provided to allow ensuring that no queue is
    /// starved indefinitely.
    pub balance_strategy: BalanceStrategy,

    /// Queue-specific worker pools. Queues specified here are removed from the shared worker pool
    /// and are only processed by their configured concurrency.
    pub queue_configs: BTreeMap<String, QueueConfig>,

    /// How long an idle worker waits between due-job scans if no Redis pub/sub wake notification
    /// arrives. Pub/sub is only a latency optimization; this interval is the reliability fallback.
    pub poll_interval: std::time::Duration,

    /// How long a fetched job is leased to one worker before another worker may execute it again.
    /// Set this above your expected job runtime, or make jobs idempotent when using shorter leases.
    pub lease_timeout: std::time::Duration,
}

#[derive(Default, Clone)]
#[non_exhaustive]
pub enum BalanceStrategy {
    /// Rotate the list of queues by 1 every time jobs are fetched from Redis. This allows each
    /// queue in the list to have an equal opportunity to have its jobs run.
    #[default]
    RoundRobin,
    /// Do not modify the list of queues. Warning: This can lead to queue starvation! For example,
    /// if the first registered queue is heavily used and always has a job available to run, then
    /// the jobs in the other queues will never run.
    None,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct QueueConfig {
    /// The maximum number of worker tasks that can process this queue concurrently.
    pub concurrency: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self { concurrency: 1 }
    }
}

impl ProcessorConfig {
    #[must_use]
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    #[must_use]
    pub fn balance_strategy(mut self, balance_strategy: BalanceStrategy) -> Self {
        self.balance_strategy = balance_strategy;
        self
    }

    #[must_use]
    pub fn queue_config(mut self, queue: String, config: QueueConfig) -> Self {
        self.queue_configs.insert(queue, config);
        self
    }

    #[must_use]
    pub fn poll_interval(mut self, poll_interval: std::time::Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    #[must_use]
    pub fn lease_timeout(mut self, lease_timeout: std::time::Duration) -> Self {
        self.lease_timeout = lease_timeout;
        self
    }
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            concurrency: num_cpus::get(),
            balance_strategy: Default::default(),
            queue_configs: Default::default(),
            poll_interval: std::time::Duration::from_millis(500),
            lease_timeout: std::time::Duration::from_secs(30),
        }
    }
}

impl QueueConfig {
    #[must_use]
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

fn shared_worker_queues_for(
    queues: &VecDeque<String>,
    queue_configs: &BTreeMap<String, QueueConfig>,
) -> VecDeque<String> {
    let dedicated_queues = queue_configs
        .keys()
        .map(|queue| queue_key(queue))
        .collect::<IndexSet<_>>();

    queues
        .iter()
        .filter(|queue| !dedicated_queues.contains(*queue))
        .cloned()
        .collect()
}

async fn run_wake_listener(
    redis: RedisPool,
    channels: Vec<String>,
    wake_notify: Arc<Notify>,
    cancellation_token: CancellationToken,
    retry_interval: std::time::Duration,
) {
    if channels.is_empty() {
        return;
    }

    loop {
        if cancellation_token.is_cancelled() {
            break;
        }

        let subscription = match redis.get().await {
            Ok(connection) => connection.subscribe(channels.clone()).await,
            Err(err) => {
                error!(
                    "Error acquiring Redis connection for wake listener: {:?}",
                    err
                );
                select! {
                    _ = tokio::time::sleep(retry_interval) => {}
                    _ = cancellation_token.cancelled() => break,
                }
                continue;
            }
        };

        let mut subscription = match subscription {
            Ok(subscription) => subscription,
            Err(err) => {
                error!("Error subscribing to Redis wake channels: {:?}", err);
                select! {
                    _ = tokio::time::sleep(retry_interval) => {}
                    _ = cancellation_token.cancelled() => break,
                }
                continue;
            }
        };

        loop {
            select! {
                message = subscription.recv() => {
                    if message.is_none() {
                        break;
                    }
                    wake_notify.notify_waiters();
                }
                _ = cancellation_token.cancelled() => return,
            }
        }
    }

    debug!("Broke out of loop for Redis wake listener");
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(err) => {
            error!("Error installing SIGTERM handler: {:?}", err);
            let _ = ctrl_c.await;
            return;
        }
    };

    let mut hangup = match signal(SignalKind::hangup()) {
        Ok(signal) => signal,
        Err(err) => {
            error!("Error installing SIGHUP handler: {:?}", err);
            let _ = ctrl_c.await;
            return;
        }
    };

    select! {
        result = &mut ctrl_c => {
            if let Err(err) = result {
                error!("Error waiting for Ctrl-C: {:?}", err);
            }
        }
        _ = terminate.recv() => {}
        _ = hangup.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        error!("Error waiting for Ctrl-C: {:?}", err);
    }
}

impl Processor {
    #[must_use]
    pub fn new(redis: RedisPool) -> Self {
        let busy_jobs = Counter::new(0);
        let context = WorkerContext::new(redis.clone());

        Self {
            chain: Chain::new_with_stats(busy_jobs.clone()),
            workers: BTreeMap::new(),
            scheduled_jobs: vec![],
            busy_jobs,
            wake_notify: Arc::new(Notify::new()),

            redis,
            queues: VecDeque::new(),
            queue_names: IndexSet::new(),
            auto_register_queues: true,
            cancellation_token: CancellationToken::new(),
            config: Default::default(),
            context,
        }
    }

    #[must_use]
    pub fn with_queues(redis: RedisPool, queues: Vec<String>) -> Self {
        let mut processor = Self::new(redis);
        processor.auto_register_queues = false;
        processor.add_queue_names(queues);
        processor
    }

    pub fn from_registered_workers(redis: RedisPool) -> Result<Self> {
        registry::validate_unique_worker_aliases()?;

        let mut processor = Self::new(redis);
        processor.register_registered_workers()?;

        if processor.queue_names().is_empty() {
            return Err(Error::Message(
                "no registered queues; derive Worker for at least one worker or register queues with `register_queue!`"
                    .to_string(),
            ));
        }

        Ok(processor)
    }

    pub fn with_config(mut self, config: ProcessorConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_state<T>(mut self, state: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.context.insert_state(state);
        self
    }

    /// Return the worker context used by this processor.
    #[must_use]
    pub fn context(&self) -> WorkerContext {
        self.context.clone()
    }

    pub async fn fetch(&mut self) -> Result<Option<UnitOfWork>> {
        self.run_balance_strategy();

        let mut redis = self.redis.get().await?;
        let now = current_timestamp_score();
        let lease_timeout = usize::try_from(self.config.lease_timeout.as_secs())
            .unwrap_or(usize::MAX)
            .max(1);

        for queue in self.queues.clone() {
            if let Some(job) = fetch_job_atomic_direct(
                &mut redis,
                queue.clone(),
                now,
                FETCH_CANDIDATE_LIMIT,
                lease_timeout,
            )
            .await?
            {
                return Ok(Some(UnitOfWork { queue, job }));
            }
        }

        Ok(None)
    }

    /// Re-order the `Processor#queues` based on the `ProcessorConfig#balance_strategy`.
    fn run_balance_strategy(&mut self) {
        if self.queues.is_empty() {
            return;
        }

        match self.config.balance_strategy {
            BalanceStrategy::RoundRobin => self.queues.rotate_right(1),
            BalanceStrategy::None => {}
        }
    }

    pub async fn process_one(&mut self) -> Result<()> {
        loop {
            if self.cancellation_token.is_cancelled() {
                return Ok(());
            }

            if let WorkFetcher::NoWorkFound = self.process_one_tick_once().await? {
                self.wait_for_work().await;
                continue;
            }

            return Ok(());
        }
    }

    pub async fn process_one_tick_once(&mut self) -> Result<WorkFetcher> {
        let work = self.fetch().await?;

        if work.is_none() {
            // If there is no job to handle, we need to add a `yield_now` in order to allow tokio's
            // scheduler to wake up another task that may be waiting to acquire a connection from
            // the Redis connection pool.
            tokio::task::yield_now().await;
            return Ok(WorkFetcher::NoWorkFound);
        }
        let mut work = work.expect("polled and found some work");

        let started = std::time::Instant::now();

        info!({
            "status" = JobStatus::InProgress.as_str(),
            "class" = &work.job.class,
            "queue" = &work.job.queue,
            "job_id" = &work.job.job_id
        }, "zug");

        if let Some(worker) = self.workers.get_mut(&work.job.class) {
            let outcome = self
                .chain
                .call(
                    &work.job,
                    worker.clone(),
                    self.redis.clone(),
                    self.context.clone(),
                )
                .await?;

            if outcome == crate::ChainOutcome::Success {
                work.complete(&self.redis).await?;
                info!({
                    "elapsed" = format!("{:?}", started.elapsed()),
                    "status" = JobStatus::Complete.as_str(),
                    "class" = &work.job.class,
                    "queue" = &work.job.queue,
                    "job_id" = &work.job.job_id}, "zug");
            }
        } else {
            error!({
                "status" = JobStatus::Complete.as_str(),
                "class" = &work.job.class,
                "queue" = &work.job.queue,
                "job_id" = &work.job.job_id
            },"!!! Worker not found !!!");
            if work.job.retry_count.is_some() {
                work.requeue(&self.redis).await?;
            } else {
                work.complete(&self.redis).await?;
            }
        }

        Ok(WorkFetcher::Done)
    }

    async fn wait_for_work(&self) {
        select! {
            _ = tokio::time::sleep(self.config.poll_interval) => {}
            _ = self.wake_notify.notified() => {}
            _ = self.cancellation_token.cancelled() => {}
        }
    }

    pub fn register<
        Args: Sync + Send + for<'de> serde::Deserialize<'de> + 'static,
        W: Worker<Args> + 'static,
    >(
        &mut self,
        worker: W,
    ) {
        if self.auto_register_queues {
            self.add_worker_queue_names::<Args, W>();
        }

        self.workers
            .insert(W::class_name(), Arc::new(WorkerRef::wrap(Arc::new(worker))));
    }

    fn add_worker_queue_names<Args, W>(&mut self)
    where
        Args: Send + 'static,
        W: Worker<Args>,
    {
        let opts = W::opts().into_opts();
        self.add_queue_name(opts.queue_name().to_string());
        if let Some(retry_queue) = opts.retry_queue_name() {
            self.add_queue_name(retry_queue.to_string());
        }
    }

    fn add_queue_names(&mut self, queues: impl IntoIterator<Item = String>) {
        for queue in queues {
            self.add_queue_name(queue);
        }
    }

    fn add_queue_name(&mut self, queue: String) {
        if !self.queue_names.insert(queue.clone()) {
            return;
        }

        self.queues.push_back(queue_key(&queue));
    }

    #[must_use]
    pub fn queue_names(&self) -> &IndexSet<String> {
        &self.queue_names
    }

    #[must_use]
    pub fn registered_worker_names(&self) -> BTreeSet<String> {
        self.workers.keys().cloned().collect()
    }

    pub fn register_registered_workers(&mut self) -> Result<()> {
        registry::register_workers(self)?;
        if self.auto_register_queues {
            self.add_queue_names(registry::registered_queue_names());
        }
        Ok(())
    }

    pub fn get_cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub(crate) fn register_schedule(&mut self, scheduled_job: ScheduledJob) {
        info!({
            "args" = ?scheduled_job.args,
            "class" = &scheduled_job.class,
            "queue" = &scheduled_job.queue,
            "name" = &scheduled_job.name,
            "cron" = &scheduled_job.cron,
        },"Registering scheduled job");

        self.scheduled_jobs.push(scheduled_job);
    }

    pub async fn enqueue_due_schedules(
        &mut self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize> {
        schedule::enqueue_due(self.redis.clone(), &mut self.scheduled_jobs, now).await
    }

    fn shared_worker_queues(&self) -> VecDeque<String> {
        shared_worker_queues_for(&self.queues, &self.config.queue_configs)
    }

    fn total_worker_process_count(&self) -> usize {
        let shared_workers = if self.shared_worker_queues().is_empty() {
            0
        } else {
            self.config.concurrency
        };
        let dedicated_workers = self
            .config
            .queue_configs
            .values()
            .map(|config| config.concurrency)
            .sum::<usize>();

        shared_workers + dedicated_workers
    }

    /// Takes self to consume the processor. This is for life-cycle management, not
    /// memory safety because you can clone processor pretty easily.
    pub async fn run(self) {
        let mut join_set: JoinSet<()> = JoinSet::new();
        let shared_queues = self.shared_worker_queues();
        let total_worker_process_count = self.total_worker_process_count();

        // Logic for spawning shared workers (workers that handles multiple queues) and dedicated
        // workers (workers that handle a single queue).
        let spawn_worker = |mut processor: Processor,
                            cancellation_token: CancellationToken,
                            num: usize,
                            dedicated_queue_name: Option<String>| {
            async move {
                loop {
                    if let Err(err) = processor.process_one().await {
                        error!("Error leaked out the bottom: {:?}", err);
                    }

                    if cancellation_token.is_cancelled() {
                        break;
                    }
                }

                let dedicated_queue_str = dedicated_queue_name
                    .map(|name| format!(" dedicated to queue '{name}'"))
                    .unwrap_or_default();
                debug!("Broke out of loop for worker {num}{dedicated_queue_str}");
            }
        };

        // Start shared worker routines for queues without queue-specific worker pools.
        if !shared_queues.is_empty() {
            for i in 0..self.config.concurrency {
                join_set.spawn({
                    let mut processor = self.clone();
                    processor.queues = shared_queues.clone();
                    spawn_worker(processor, self.cancellation_token.clone(), i, None)
                });
            }
        }

        // Start dedicated worker routines.
        for (queue, config) in &self.config.queue_configs {
            for i in 0..config.concurrency {
                join_set.spawn({
                    let mut processor = self.clone();
                    processor.queues = [queue_key(queue)].into();
                    spawn_worker(
                        processor,
                        self.cancellation_token.clone(),
                        i,
                        Some(queue.clone()),
                    )
                });
            }
        }

        // Start worker process metrics publisher.
        join_set.spawn({
            let redis = self.redis.clone();
            let queues = self.queue_names.iter().cloned().collect();
            let busy_jobs = self.busy_jobs.clone();
            let cancellation_token = self.cancellation_token.clone();
            async move {
                let hostname = if let Some(host) = gethostname::gethostname().to_str() {
                    host.to_string()
                } else {
                    "UNKNOWN_HOSTNAME".to_string()
                };

                let stats_publisher =
                    StatsPublisher::new(hostname, queues, busy_jobs, total_worker_process_count);
                let period = std::time::Duration::from_secs(5);
                let mut ticker =
                    tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    select! {
                        _ = ticker.tick() => {}
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }

                    if let Err(err) = stats_publisher.publish_stats(redis.clone()).await {
                        error!("Error publishing processor stats: {:?}", err);
                    }
                }

                debug!("Broke out of loop web metrics");
            }
        });

        // Start a best-effort Redis pub/sub wake listener. Polling remains the source of truth.
        join_set.spawn({
            let redis = self.redis.clone();
            let channels = self
                .queue_names
                .iter()
                .map(|queue| queue_wake_channel(queue))
                .collect();
            let wake_notify = self.wake_notify.clone();
            let cancellation_token = self.cancellation_token.clone();
            let retry_interval = self.config.poll_interval;
            async move {
                run_wake_listener(
                    redis,
                    channels,
                    wake_notify,
                    cancellation_token,
                    retry_interval,
                )
                .await;
            }
        });

        // Watch for scheduled jobs and enqueue due work.
        join_set.spawn({
            let redis = self.redis.clone();
            let mut scheduled_jobs = self.scheduled_jobs.clone();
            let cancellation_token = self.cancellation_token.clone();
            let period = self.config.poll_interval;
            async move {
                let mut ticker =
                    tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    select! {
                        _ = ticker.tick() => {}
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }

                    if let Err(err) = schedule::enqueue_due(
                        redis.clone(),
                        &mut scheduled_jobs,
                        chrono::Utc::now(),
                    )
                    .await
                    {
                        error!("Error in scheduled job poller routine: {}", err);
                    }
                }

                debug!("Broke out of loop for scheduled jobs");
            }
        });

        join_set.spawn({
            let cancellation_token = self.cancellation_token.clone();
            let cancellation_observer = cancellation_token.clone();
            async move {
                select! {
                    _ = wait_for_shutdown_signal() => {
                        info!("Shutdown signal received; waiting for running jobs to finish");
                        cancellation_token.cancel();
                    }
                    _ = cancellation_observer.cancelled() => {}
                }
            }
        });

        while let Some(result) = join_set.join_next().await {
            if let Err(err) = result {
                error!("Processor had a spawned task return an error: {}", err);
            }
        }
    }

    pub async fn using<M>(&mut self, middleware: M)
    where
        M: ServerMiddleware + Send + Sync + 'static,
    {
        self.chain.using(Box::new(middleware)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_configs_remove_queues_from_shared_pool() {
        let queues = VecDeque::from([queue_key("default"), queue_key("serial")]);
        let mut queue_configs = BTreeMap::new();
        queue_configs.insert("serial".to_string(), QueueConfig::default().concurrency(1));

        assert_eq!(
            shared_worker_queues_for(&queues, &queue_configs),
            VecDeque::from([queue_key("default")])
        );
    }

    #[test]
    fn processor_config_has_concurrency_by_default() {
        let cfg = ProcessorConfig::default();

        assert!(
            cfg.concurrency > 0,
            "concurrency should be greater than 0 (using num cpu)"
        );

        let cfg = cfg.concurrency(1000);

        assert_eq!(cfg.concurrency, 1000);
    }
}
