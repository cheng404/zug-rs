use async_trait::async_trait;
use bb8::Pool;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use zug::{Processor, RedisConnectionManager, Result, Worker};

#[derive(Clone)]
struct EmailReminderWorker;

#[derive(Deserialize, Debug, Serialize)]
struct EmailReminderArgs {
    user_guid: String,
}

#[async_trait]
impl Worker<EmailReminderArgs> for EmailReminderWorker {
    fn opts() -> zug::WorkerOpts<EmailReminderArgs, Self> {
        zug::WorkerOpts::new().queue("reminders")
    }

    async fn perform(&self, args: EmailReminderArgs) -> Result<()> {
        tracing::info!(user_guid = %args.user_guid, "Sending delayed email reminder");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let manager = RedisConnectionManager::new("redis://127.0.0.1/")?;
    let redis = Pool::builder().build(manager).await?;

    EmailReminderWorker::opts()
        .defer_by(Duration::from_secs(60))
        .perform_async(
            &redis,
            EmailReminderArgs {
                user_guid: "USR-123".to_string(),
            },
        )
        .await?;

    zug::opts()
        .queue("reminders")
        .defer_by(Duration::from_secs(120))
        .perform_async(
            &redis,
            "EmailReminderWorker".into(),
            EmailReminderArgs {
                user_guid: "USR-456".to_string(),
            },
        )
        .await?;

    let mut processor = Processor::new(redis);
    processor.register(EmailReminderWorker);
    processor.run().await;

    Ok(())
}
