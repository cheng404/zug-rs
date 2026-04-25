#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use std::sync::{Arc, Mutex};
    use zug::{Processor, RedisConnectionManager, RedisPool, Result, WorkFetcher, Worker};

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
    async fn can_create_and_process_an_unique_job_only_once_within_the_ttl() {
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

        let first_job_id = TestWorker::opts()
            .queue(queue.clone())
            .unique_for(std::time::Duration::from_secs(5))
            .perform_async(&redis, ())
            .await
            .unwrap();
        assert!(first_job_id.is_some());

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(*worker.did_process.lock().unwrap());

        let duplicate_job_id = TestWorker::opts()
            .queue(queue)
            .unique_for(std::time::Duration::from_secs(5))
            .perform_async(&redis, ())
            .await
            .unwrap();
        assert_eq!(duplicate_job_id, None);

        assert_eq!(
            p.process_one_tick_once().await.unwrap(),
            WorkFetcher::NoWorkFound
        );
    }
}
