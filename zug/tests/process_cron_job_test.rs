#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use std::sync::{Arc, Mutex};
    use zug::{
        schedule, Processor, RedisConnectionManager, RedisPool, Result, WorkFetcher, Worker,
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
        let p = Processor::with_queues(redis.clone(), vec![queue]);

        (p, redis)
    }

    #[tokio::test]
    async fn can_process_a_cron_job() {
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
        let (mut p, _redis) = new_base_processor(queue.clone()).await;

        p.register(worker.clone());

        schedule::builder("* * * * * *")
            .unwrap()
            .name("Payment report processing for a user using json args")
            .queue(queue.clone())
            .register(&mut p, worker.clone())
            .await
            .unwrap();

        assert_eq!(
            p.process_one_tick_once().await.unwrap(),
            WorkFetcher::NoWorkFound
        );

        let n = p
            .enqueue_due_schedules(chrono::Utc::now() + chrono::Duration::seconds(2))
            .await
            .unwrap();

        assert_eq!(n, 1);

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);

        assert!(*worker.did_process.lock().unwrap());
    }

    #[tokio::test]
    async fn cron_enqueue_due_uses_deterministic_job_ids_across_processors() {
        #[derive(Clone)]
        struct TestWorker;

        #[async_trait]
        impl Worker<()> for TestWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                Ok(())
            }
        }

        let queue = "cron_idempotent".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;

        schedule::builder("* * * * * *")
            .unwrap()
            .name("idempotent cron job")
            .queue(queue.clone())
            .register(&mut p, TestWorker)
            .await
            .unwrap();

        let mut second_processor = p.clone();
        let now = chrono::Utc::now() + chrono::Duration::seconds(2);

        assert_eq!(p.enqueue_due_schedules(now).await.unwrap(), 1);
        assert_eq!(
            second_processor.enqueue_due_schedules(now).await.unwrap(),
            0
        );

        let mut conn = redis.get().await.unwrap();
        let queued_jobs = conn
            .zrange(format!("queue:{queue}"), isize::MIN, isize::MAX)
            .await
            .unwrap();
        assert_eq!(queued_jobs.len(), 1);
        assert!(queued_jobs[0].starts_with("schedule:"));
    }
}
