use async_trait::async_trait;
use bb8::Pool;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;
use zug::{
    schedule, Error, Processor, ProcessorConfig, QueueConfig, RedisConnectionManager, Result,
    Worker,
};

#[derive(Worker, Clone, Default)]
#[zug(name = "email.reminder")]
struct EmailWorker;

#[derive(Debug, Deserialize, Serialize)]
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
    tracing_subscriber::fmt::init();

    let redis_url =
        std::env::var("ZUG_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let queues = std::env::var("ZUG_QUEUES")
        .map(|value| parse_queues(&value))
        .unwrap_or_else(|_| vec!["mailers".to_string()]);
    let concurrency = env_usize("ZUG_CONCURRENCY", 8)?;
    let mailers_concurrency = env_usize("ZUG_MAILERS_CONCURRENCY", 2)?;
    let lease_timeout_secs = env_usize("ZUG_LEASE_TIMEOUT_SECS", 30)?;

    let manager = RedisConnectionManager::new(redis_url)?;
    let redis = Pool::builder().build(manager).await?;

    let config = ProcessorConfig::default()
        .concurrency(concurrency)
        .queue_config(
            "mailers".to_string(),
            QueueConfig::default().concurrency(mailers_concurrency),
        )
        .lease_timeout(Duration::from_secs(lease_timeout_secs as u64));

    let mut processor = Processor::with_queues(redis, queues).with_config(config);

    schedule::builder("0 0 8 * * *")?
        .name("daily email reminder")
        .queue("mailers")
        .retry(3)
        .args(EmailArgs {
            user_guid: "USR-123".to_string(),
        })?
        .register(&mut processor, EmailWorker)
        .await?;

    processor.run().await;
    Ok(())
}

fn parse_queues(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|queue| !queue.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| Error::Message(format!("invalid {name}: {error}"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(Error::Message(format!("invalid {name}: {error}"))),
    }
}
