#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use serial_test::serial;
    use std::sync::{Arc, Mutex};
    use zug::{
        job_status, job_status_ttl, BalanceStrategy, JobStatus, Processor, ProcessorConfig,
        QueueConfig, RedisConnectionManager, RedisPool, Result, UnitOfWork, WorkFetcher, Worker,
        WorkerContext, DEFAULT_JOB_STATUS_TTL_SECONDS,
    };

    #[async_trait]
    trait FlushAll {
        async fn flushall(&self);
    }

    #[async_trait]
    impl FlushAll for RedisPool {
        async fn flushall(&self) {
            let mut conn = self.get().await.unwrap();
            conn.flushall().await.unwrap();
        }
    }

    async fn new_base_processor(queue: String) -> (Processor, RedisPool) {
        // Redis
        let redis_url =
            std::env::var("ZUG_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let manager = RedisConnectionManager::new(redis_url).unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        // Zug processor
        let p = Processor::with_queues(redis.clone(), vec![queue]).with_config(
            ProcessorConfig::default()
                .concurrency(1)
                .balance_strategy(BalanceStrategy::RoundRobin)
                .queue_config(
                    "dedicated queue 1".to_string(),
                    QueueConfig::default().concurrency(10),
                )
                .queue_config(
                    "dedicated queue 2".to_string(),
                    QueueConfig::default().concurrency(100),
                ),
        );

        (p, redis)
    }

    #[tokio::test]
    #[serial]
    async fn can_process_an_async_job() {
        #[derive(Clone)]
        struct TestWorker {
            did_process: Arc<Mutex<bool>>,
        }

        #[async_trait]
        impl Worker<()> for TestWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                let mut this = self.did_process.lock().unwrap();
                *this = true;

                Ok(())
            }
        }

        let worker = TestWorker {
            did_process: Arc::new(Mutex::new(false)),
        };
        let queue = "random123".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;

        p.register(worker.clone());

        TestWorker::opts()
            .queue(queue)
            .perform_async(&redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(*worker.did_process.lock().unwrap());
    }

    #[tokio::test]
    #[serial]
    async fn worker_can_access_processor_state() {
        #[derive(Clone)]
        struct AppState {
            did_process: Arc<Mutex<bool>>,
        }

        #[derive(Clone)]
        struct TestWorker;

        #[async_trait]
        impl Worker<()> for TestWorker {
            async fn perform_with_context(&self, context: WorkerContext, _args: ()) -> Result<()> {
                let state = context.state::<AppState>()?;
                let mut did_process = state.did_process.lock().unwrap();
                *did_process = true;

                Ok(())
            }
        }

        let did_process = Arc::new(Mutex::new(false));
        let queue = "worker_state".to_string();
        let (p, redis) = new_base_processor(queue.clone()).await;
        let mut p = p.with_state(AppState {
            did_process: did_process.clone(),
        });

        p.register(TestWorker);

        TestWorker::opts()
            .queue(queue)
            .perform_async(&redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(*did_process.lock().unwrap());
    }

    #[tokio::test]
    #[serial]
    async fn records_async_job_status_lifecycle() {
        #[derive(Clone)]
        struct TestWorker;

        #[async_trait]
        impl Worker<()> for TestWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                Ok(())
            }
        }

        let queue = "status_lifecycle".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;
        p.register(TestWorker);

        let job = TestWorker::opts()
            .queue(queue)
            .into_opts()
            .create_job(TestWorker::class_name(), ())
            .unwrap();
        let job_id = job.job_id.clone();

        UnitOfWork::from_job(job).enqueue(&redis).await.unwrap();

        let ttl = job_status_ttl(&redis, &job_id).await.unwrap().unwrap();
        assert!(ttl > 0);
        assert!(ttl <= DEFAULT_JOB_STATUS_TTL_SECONDS as i64);
        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::Queued
        );

        let mut fetched = p.fetch().await.unwrap().unwrap();
        assert_eq!(fetched.job.job_id, job_id);
        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::InProgress
        );
        fetched.complete(&redis).await.unwrap();

        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::Complete
        );
    }

    #[tokio::test]
    #[serial]
    async fn derives_active_status_from_queue_score() {
        #[derive(Clone)]
        struct TestWorker;

        #[async_trait]
        impl Worker<()> for TestWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                Ok(())
            }
        }

        let queue = "status_from_queue_score".to_string();
        let (_p, redis) = new_base_processor(queue.clone()).await;

        let job = TestWorker::opts()
            .queue(queue.clone())
            .into_opts()
            .create_job(TestWorker::class_name(), ())
            .unwrap();
        let job_id = job.job_id.clone();

        UnitOfWork::from_job(job).enqueue(&redis).await.unwrap();

        let mut conn = redis.get().await.unwrap();
        conn.hdel("job-status".to_string(), &job_id).await.unwrap();

        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::Queued
        );

        let _: usize = conn
            .zadd(
                format!("queue:{queue}"),
                job_id.clone(),
                chrono::Utc::now().timestamp() as f64 + 60.0,
            )
            .await
            .unwrap();
        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::Deferred
        );

        let _: usize = conn
            .zrem(format!("queue:{queue}"), job_id.clone())
            .await
            .unwrap();
        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::NotFound
        );
    }

    #[tokio::test]
    #[serial]
    async fn fetch_redelivers_after_lease_timeout() {
        #[derive(Clone)]
        struct TestWorker;

        #[async_trait]
        impl Worker<()> for TestWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                Ok(())
            }
        }

        let queue = "lease_redelivery".to_string();
        let redis_url =
            std::env::var("ZUG_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let manager = RedisConnectionManager::new(redis_url).unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        let mut p = Processor::with_queues(redis.clone(), vec![queue.clone()]).with_config(
            ProcessorConfig::default()
                .concurrency(1)
                .lease_timeout(std::time::Duration::from_secs(1)),
        );

        let job = TestWorker::opts()
            .queue(queue)
            .into_opts()
            .create_job(TestWorker::class_name(), ())
            .unwrap();
        let job_id = job.job_id.clone();

        UnitOfWork::from_job(job).enqueue(&redis).await.unwrap();

        let first = p.fetch().await.unwrap().unwrap();
        assert_eq!(first.job.job_id, job_id);
        assert!(p.fetch().await.unwrap().is_none());

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        let second = p.fetch().await.unwrap().unwrap();
        assert_eq!(second.job.job_id, job_id);
    }

    #[tokio::test]
    #[serial]
    async fn can_customize_job_status_ttl_from_worker_opts() {
        #[derive(Clone)]
        struct TestWorker;

        #[async_trait]
        impl Worker<()> for TestWorker {
            fn opts() -> zug::WorkerOpts<(), Self>
            where
                Self: Sized,
            {
                zug::WorkerOpts::new()
                    .queue("custom_status_ttl")
                    .job_status_ttl(std::time::Duration::from_secs(60))
            }

            async fn perform(&self, _args: ()) -> Result<()> {
                Ok(())
            }
        }

        let (mut p, redis) = new_base_processor("custom_status_ttl".to_string()).await;
        p.register(TestWorker);

        let job = TestWorker::opts()
            .into_opts()
            .create_job(TestWorker::class_name(), ())
            .unwrap();
        let job_id = job.job_id.clone();

        assert_eq!(job.status_ttl_seconds, 60);

        UnitOfWork::from_job(job).enqueue(&redis).await.unwrap();

        let ttl = job_status_ttl(&redis, &job_id).await.unwrap().unwrap();
        assert!(ttl > 0);
        assert!(ttl <= 60);

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);

        let ttl = job_status_ttl(&redis, &job_id).await.unwrap().unwrap();
        assert!(ttl > 0);
        assert!(ttl <= 60);
    }
}
