use async_trait::async_trait;
use bb8::Pool;
use serde::{Deserialize, Serialize};
use tracing::info;
use zug::{RedisConnectionManager, Result, Worker};

#[derive(Clone)]
struct PaymentReportWorker {}

impl PaymentReportWorker {
    async fn send_report(&self, user_guid: String) -> Result<()> {
        // TODO: Some actual work goes here...
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Redis
    let manager = RedisConnectionManager::new("redis://127.0.0.1/")?;
    let redis = Pool::builder().build(manager).await?;

    let mut n = 0;
    let mut last = 0;
    let mut then = std::time::Instant::now();

    loop {
        PaymentReportWorker::perform_async(
            &redis,
            PaymentReportArgs {
                user_guid: "USR-123".into(),
            },
        )
        .await
        .unwrap();

        //tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        n += 1;

        if n % 100000 == 0 {
            let now = std::time::Instant::now();
            let delta = n - last;
            last = n;
            let delta_time = now - then;
            if delta_time.as_secs() == 0 {
                continue;
            }
            then = now;
            let rate = delta / delta_time.as_secs();
            println!("Iterations since last: {delta} at a rate of: {rate} iter/sec");
        }
    }
}
