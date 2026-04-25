#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use std::sync::{Arc, Mutex};
    use zug::{
        job_status, JobStatus, Processor, RedisConnectionManager, RedisPool, Result, WorkFetcher,
        Worker,
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
    async fn can_process_a_scheduled_job() {
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

        let job_id = TestWorker::opts()
            .queue(queue.clone())
            .defer_by(std::time::Duration::from_secs(10))
            .perform_async(&redis, ())
            .await
            .unwrap()
            .expect("deferred job should be enqueued");

        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::Deferred
        );

        let mut conn = redis.get().await.unwrap();
        let scheduled_job_id = conn
            .zrange(format!("queue:{queue}"), isize::MIN, isize::MAX)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(scheduled_job_id, job_id);

        assert_eq!(
            p.process_one_tick_once().await.unwrap(),
            WorkFetcher::NoWorkFound
        );

        let _: usize = conn
            .zadd(format!("queue:{queue}"), job_id.clone(), 0)
            .await
            .unwrap();

        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::Queued
        );

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);

        assert_eq!(
            job_status(&redis, &job_id).await.unwrap(),
            JobStatus::Complete
        );

        assert!(*worker.did_process.lock().unwrap());
    }
}
