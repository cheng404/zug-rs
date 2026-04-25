use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;
use zug::{Result, Worker};

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
    zug::cli::run_cli().await
}
