use async_trait::async_trait;
use bb8::Pool;
use zug::{cli, registry, Processor, RedisConnectionManager, RedisPool, Result, Worker};

#[derive(Worker, Clone, Default)]
struct CliWorker;

#[derive(Worker, Clone, Default)]
struct StructCliWorker;

#[derive(Worker, Clone, Default)]
#[zug(name = "cli.named")]
struct NamedCliWorker;

#[derive(Worker, Clone, Default)]
struct RetryCliWorker;

async fn redis_pool() -> RedisPool {
    let redis_url =
        std::env::var("ZUG_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let manager = RedisConnectionManager::new(redis_url).unwrap();
    Pool::builder().build(manager).await.unwrap()
}

#[async_trait]
impl Worker<()> for CliWorker {
    fn opts() -> zug::WorkerOpts<(), Self> {
        zug::WorkerOpts::new().queue("cli_default")
    }

    async fn perform(&self, _args: ()) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Worker<()> for StructCliWorker {
    fn opts() -> zug::WorkerOpts<(), Self> {
        zug::WorkerOpts::new().queue("cli_struct")
    }

    async fn perform(&self, _args: ()) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Worker<()> for NamedCliWorker {
    fn opts() -> zug::WorkerOpts<(), Self> {
        zug::WorkerOpts::new().queue("cli_named")
    }

    async fn perform(&self, _args: ()) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Worker<()> for RetryCliWorker {
    fn opts() -> zug::WorkerOpts<(), Self> {
        zug::WorkerOpts::new()
            .queue("cli_retry_source")
            .retry_queue("cli_retry_destination")
    }

    async fn perform(&self, _args: ()) -> Result<()> {
        Ok(())
    }
}

zug::register_queue!("cli_scheduled");

#[test]
fn inventory_contains_macro_registrations() {
    let cli_worker = CliWorker::class_name();
    let struct_cli_worker = StructCliWorker::class_name();
    let named_cli_worker = NamedCliWorker::class_name();
    let retry_cli_worker = RetryCliWorker::class_name();

    assert!(registry::registered_worker_names().contains(&cli_worker));
    assert!(registry::registered_worker_names().contains(&struct_cli_worker));
    assert!(registry::registered_worker_names().contains(&named_cli_worker));
    assert!(registry::registered_worker_names().contains(&retry_cli_worker));
    assert!(registry::registered_worker_aliases().contains("cli.named"));
    assert_eq!(
        registry::registered_worker_class_name_for_alias("cli.named").unwrap(),
        named_cli_worker
    );
    assert_eq!(
        registry::registered_worker_default_queue(&cli_worker).as_deref(),
        Some("cli_default")
    );
    assert_eq!(
        registry::registered_worker_default_queue(&struct_cli_worker).as_deref(),
        Some("cli_struct")
    );
    assert_eq!(
        registry::registered_worker_default_queue(&named_cli_worker).as_deref(),
        Some("cli_named")
    );
    assert_eq!(
        registry::registered_worker_default_queue(&retry_cli_worker).as_deref(),
        Some("cli_retry_source")
    );
    assert!(registry::registered_queue_names().contains("cli_default"));
    assert!(registry::registered_queue_names().contains("cli_struct"));
    assert!(registry::registered_queue_names().contains("cli_named"));
    assert!(registry::registered_queue_names().contains("cli_retry_source"));
    assert!(registry::registered_queue_names().contains("cli_retry_destination"));
    assert!(registry::registered_queue_names().contains("cli_scheduled"));
}

#[tokio::test]
#[serial_test::serial]
async fn config_schedule_accepts_worker_name_alias() {
    let config = cli::Config::from_toml_str(
        r#"
queues = [{ name = "cli_named" }]

[[schedules]]
name = "named cli job"
worker_name = "cli.named"
cron = "0 0 0 * * *"
"#,
    )
    .unwrap();

    cli::build_processor(redis_pool().await, &config).unwrap();
}

#[tokio::test]
#[serial_test::serial]
async fn config_schedule_worker_requires_registered_class_name() {
    let config = cli::Config::from_toml_str(
        r#"
queues = [{ name = "cli_named" }]

[[schedules]]
name = "named cli job"
worker = "cli.named"
cron = "0 0 0 * * *"
"#,
    )
    .unwrap();

    let Err(error) = cli::build_processor(redis_pool().await, &config) else {
        panic!("worker should require a registered class name");
    };

    assert!(error
        .to_string()
        .contains("scheduled worker `cli.named` was not registered"));

    let config = cli::Config::from_toml_str(&format!(
        r#"
queues = [{{ name = "cli_named" }}]

[[schedules]]
name = "named cli job"
worker = "{}"
cron = "0 0 0 * * *"
"#,
        NamedCliWorker::class_name()
    ))
    .unwrap();

    cli::build_processor(redis_pool().await, &config).unwrap();
}

#[tokio::test]
#[serial_test::serial]
async fn processor_can_build_from_registered_workers() {
    let redis = redis_pool().await;

    let processor = Processor::from_registered_workers(redis).unwrap();

    assert!(processor.queue_names().contains(&"cli_named".to_string()));
    assert!(processor
        .queue_names()
        .contains(&"cli_retry_destination".to_string()));
    assert!(processor
        .registered_worker_names()
        .contains(&NamedCliWorker::class_name()));
}

#[tokio::test]
#[serial_test::serial]
async fn processor_new_registers_worker_queues() {
    let redis = redis_pool().await;

    let mut processor = Processor::new(redis);
    processor.register(RetryCliWorker);

    assert!(processor
        .queue_names()
        .contains(&"cli_retry_source".to_string()));
    assert!(processor
        .queue_names()
        .contains(&"cli_retry_destination".to_string()));
}

#[tokio::test]
#[serial_test::serial]
async fn processor_with_queues_keeps_explicit_queue_set() {
    let redis = redis_pool().await;

    let mut processor = Processor::with_queues(
        redis,
        vec!["only_cli_named".to_string(), "only_cli_named".to_string()],
    );
    processor.register(NamedCliWorker);

    assert_eq!(
        processor
            .queue_names()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["only_cli_named"]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn register_worker_macro_can_register_inventory_workers_on_manual_processor() {
    let redis = redis_pool().await;

    let mut processor = Processor::with_queues(redis, vec!["cli_named".to_string()]);
    zug::register_worker!(&mut processor, zug::*).unwrap();

    assert!(processor
        .registered_worker_names()
        .contains(&NamedCliWorker::class_name()));
}
