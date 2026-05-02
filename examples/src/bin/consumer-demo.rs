use async_trait::async_trait;
use bb8::Pool;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use zug::{
    ChainIter, Job, Processor, RedisConnectionManager, Result, ServerMiddleware, Worker, WorkerRef,
};

#[derive(Clone)]
struct HelloWorker;

#[async_trait]
impl Worker<()> for HelloWorker {
    async fn perform(&self, _args: ()) -> Result<()> {
        // I don't use any args. I do my own work.
        Ok(())
    }
}

#[derive(Clone)]
struct PaymentReportWorker;

impl PaymentReportWorker {
    async fn send_report(&self, user_guid: String) -> Result<()> {
        // Some actual work goes here...
        info!({"user_guid" = user_guid, "class_name" = Self::class_name()}, "Sending payment report to user");

        Ok(())
    }
}

#[derive(Deserialize, Debug, Serialize)]
struct PaymentReportArgs {
    user_guid: String,
}

#[async_trait]
impl Worker<PaymentReportArgs> for PaymentReportWorker {
    fn opts() -> zug::WorkerOpts<PaymentReportArgs, Self> {
        zug::WorkerOpts::new().queue("yolo")
    }

    async fn perform(&self, args: PaymentReportArgs) -> Result<()> {
        self.send_report(args.user_guid).await
    }
}

struct FilterExpiredUsersMiddleware;

#[derive(Deserialize)]
struct FiltereExpiredUsersArgs {
    user_guid: String,
}

impl FiltereExpiredUsersArgs {
    fn is_expired(&self) -> bool {
        self.user_guid == "USR-123-EXPIRED"
    }
}

#[async_trait]
impl ServerMiddleware for FilterExpiredUsersMiddleware {
    async fn call(
        &self,
        chain: ChainIter,
        job: &Job,
        worker: Arc<WorkerRef>,
        redis: Pool<RedisConnectionManager>,
    ) -> Result<()> {
        let args: std::result::Result<(FiltereExpiredUsersArgs,), serde_json::Error> =
            serde_json::from_value(job.args.clone());

        // If we can safely deserialize then attempt to filter based on user guid.
        if let Ok((filter,)) = args {
            if filter.is_expired() {
                error!({
                    "class" = &job.class,
                    "job_id" = &job.job_id,
                    "user_guid" = filter.user_guid,
                }, "Detected an expired user, skipping this job");
                return Ok(());
            }
        }

        chain.next(job, worker, redis).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Redis
    let manager = RedisConnectionManager::new("redis://127.0.0.1/")?;
    let redis = Pool::builder().build(manager).await?;
    //
    //    tokio::spawn({
    //        let mut redis = redis.clone();
    //
    //        async move {
    //            loop {
    //                PaymentReportWorker::perform_async(
    //                    &mut redis,
    //                    PaymentReportArgs {
    //                        user_guid: "USR-123".into(),
    //                    },
    //                )
    //                .await
    //                .unwrap();
    //
    //                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    //            }
    //        }
    //    });
    //
    //    // Enqueue a job with the worker! There are many ways to do this.
    //    PaymentReportWorker::perform_async(
    //        &mut redis,
    //        PaymentReportArgs {
    //            user_guid: "USR-123".into(),
    //        },
    //    )
    //    .await?;
    //
    //    PaymentReportWorker::opts()
    //        .defer_by(std::time::Duration::from_secs(10))
    //        .perform_async(
    //            &mut redis,
    //            PaymentReportArgs {
    //                user_guid: "USR-123".into(),
    //            },
    //        )
    //    .await?;
    //
    //    PaymentReportWorker::opts()
    //        .queue("brolo")
    //        .perform_async(
    //            &mut redis,
    //            PaymentReportArgs {
    //                user_guid: "USR-123-EXPIRED".into(),
    //            },
    //        )
    //        .await?;
    //
    //    zug::perform_async(
    //        &mut redis,
    //        "PaymentReportWorker".into(),
    //        "yolo".into(),
    //        PaymentReportArgs {
    //            user_guid: "USR-123".to_string(),
    //        },
    //    )
    //    .await?;
    //
    //    // Enqueue a job
    //    zug::perform_async(
    //        &mut redis,
    //        "PaymentReportWorker".into(),
    //        "yolo".into(),
    //        PaymentReportArgs {
    //            user_guid: "USR-123".to_string(),
    //        },
    //    )
    //    .await?;
    //
    //    // Enqueue a job with options
    //    zug::opts()
    //        .queue("yolo".to_string())
    //        .perform_async(
    //            &mut redis,
    //            "PaymentReportWorker".into(),
    //            PaymentReportArgs {
    //                user_guid: "USR-123".to_string(),
    //            },
    //        )
    //        .await?;

    // Zug processor
    let mut p =
        Processor::with_queues(redis.clone(), vec!["yolo".to_string(), "brolo".to_string()]);

    // Add known workers
    p.register(HelloWorker);
    p.register(PaymentReportWorker);

    // Custom Middlewares
    p.using(FilterExpiredUsersMiddleware).await;

    //    // Scheduled cron jobs
    //    schedule::builder("0 * * * * *")?
    //        .name("Payment report processing for a random user")
    //        .queue("yolo")
    //        //.args(PaymentReportArgs {
    //        //    user_guid: "USR-123-SCHEDULED".to_string(),
    //        //})?
    //        .args(json!({ "user_guid": "USR-123-SCHEDULED" }))?
    //        .register(&mut p, PaymentReportWorker::new(logger.clone()))
    //        .await?;

    p.run().await;
    Ok(())
}
