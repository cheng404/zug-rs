use async_trait::async_trait;
use bb8::Pool;
use zug::{registry, Processor, RedisConnectionManager, RedisPool, Result, Worker};

#[derive(Worker, Clone, Default)]
struct RegistryWorker;

#[derive(Worker, Clone, Default)]
struct StructRegistryWorker;

#[derive(Worker, Clone, Default)]
#[zug(name = "registry.named")]
struct NamedRegistryWorker;

#[derive(Worker, Clone, Default)]
struct RetryRegistryWorker;

async fn redis_pool() -> RedisPool {
    let redis_url =
        std::env::var("ZUG_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let manager = RedisConnectionManager::new(redis_url).unwrap();
    Pool::builder().build(manager).await.unwrap()
}

#[async_trait]
impl Worker<()> for RegistryWorker {
    fn opts() -> zug::WorkerOpts<(), Self> {
        zug::WorkerOpts::new().queue("registry_default")
    }

    async fn perform(&self, _args: ()) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Worker<()> for StructRegistryWorker {
    fn opts() -> zug::WorkerOpts<(), Self> {
        zug::WorkerOpts::new().queue("registry_struct")
    }

    async fn perform(&self, _args: ()) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Worker<()> for NamedRegistryWorker {
    fn opts() -> zug::WorkerOpts<(), Self> {
        zug::WorkerOpts::new().queue("registry_named")
    }

    async fn perform(&self, _args: ()) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Worker<()> for RetryRegistryWorker {
    fn opts() -> zug::WorkerOpts<(), Self> {
        zug::WorkerOpts::new()
            .queue("registry_retry_source")
            .retry_queue("registry_retry_destination")
    }

    async fn perform(&self, _args: ()) -> Result<()> {
        Ok(())
    }
}

zug::register_queue!("registry_scheduled");

#[test]
fn inventory_contains_macro_registrations() {
    let registry_worker = RegistryWorker::class_name();
    let struct_registry_worker = StructRegistryWorker::class_name();
    let named_registry_worker = NamedRegistryWorker::class_name();
    let retry_registry_worker = RetryRegistryWorker::class_name();

    assert!(registry::registered_worker_names().contains(&registry_worker));
    assert!(registry::registered_worker_names().contains(&struct_registry_worker));
    assert!(registry::registered_worker_names().contains(&named_registry_worker));
    assert!(registry::registered_worker_names().contains(&retry_registry_worker));
    assert!(registry::registered_worker_aliases().contains("registry.named"));
    assert_eq!(
        registry::registered_worker_class_name_for_alias("registry.named").unwrap(),
        named_registry_worker
    );
    assert_eq!(
        registry::registered_worker_default_queue(&registry_worker).as_deref(),
        Some("registry_default")
    );
    assert_eq!(
        registry::registered_worker_default_queue(&struct_registry_worker).as_deref(),
        Some("registry_struct")
    );
    assert_eq!(
        registry::registered_worker_default_queue(&named_registry_worker).as_deref(),
        Some("registry_named")
    );
    assert_eq!(
        registry::registered_worker_default_queue(&retry_registry_worker).as_deref(),
        Some("registry_retry_source")
    );
    assert!(registry::registered_queue_names().contains("registry_default"));
    assert!(registry::registered_queue_names().contains("registry_struct"));
    assert!(registry::registered_queue_names().contains("registry_named"));
    assert!(registry::registered_queue_names().contains("registry_retry_source"));
    assert!(registry::registered_queue_names().contains("registry_retry_destination"));
    assert!(registry::registered_queue_names().contains("registry_scheduled"));
}

#[tokio::test]
#[serial_test::serial]
async fn processor_can_build_from_registered_workers() {
    let redis = redis_pool().await;

    let processor = Processor::from_registered_workers(redis).unwrap();

    assert!(processor
        .queue_names()
        .contains(&"registry_named".to_string()));
    assert!(processor
        .queue_names()
        .contains(&"registry_retry_destination".to_string()));
    assert!(processor
        .registered_worker_names()
        .contains(&NamedRegistryWorker::class_name()));
}

#[tokio::test]
#[serial_test::serial]
async fn processor_new_registers_worker_queues() {
    let redis = redis_pool().await;

    let mut processor = Processor::new(redis);
    processor.register(RetryRegistryWorker);

    assert!(processor
        .queue_names()
        .contains(&"registry_retry_source".to_string()));
    assert!(processor
        .queue_names()
        .contains(&"registry_retry_destination".to_string()));
}

#[tokio::test]
#[serial_test::serial]
async fn processor_with_queues_keeps_explicit_queue_set() {
    let redis = redis_pool().await;

    let mut processor = Processor::with_queues(
        redis,
        vec![
            "only_registry_named".to_string(),
            "only_registry_named".to_string(),
        ],
    );
    processor.register(NamedRegistryWorker);

    assert_eq!(
        processor
            .queue_names()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["only_registry_named"]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn register_worker_macro_can_register_inventory_workers_on_manual_processor() {
    let redis = redis_pool().await;

    let mut processor = Processor::with_queues(redis, vec!["registry_named".to_string()]);
    zug::register_worker!(&mut processor, zug::*).unwrap();

    assert!(processor
        .registered_worker_names()
        .contains(&NamedRegistryWorker::class_name()));
}
