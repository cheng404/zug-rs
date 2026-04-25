use async_trait::async_trait;
use zug::{registry, Result, Worker};

mod first {
    use super::*;

    #[derive(Worker, Clone, Default)]
    #[zug(name = "duplicate.worker")]
    pub struct DuplicateWorker;

    #[async_trait]
    impl Worker<()> for DuplicateWorker {
        async fn perform(&self, _args: ()) -> Result<()> {
            Ok(())
        }
    }
}

mod second {
    use super::*;

    #[derive(Worker, Clone, Default)]
    #[zug(name = "duplicate.worker")]
    pub struct DuplicateWorker;

    #[async_trait]
    impl Worker<()> for DuplicateWorker {
        async fn perform(&self, _args: ()) -> Result<()> {
            Ok(())
        }
    }
}

#[test]
fn rejects_duplicate_worker_names() {
    let error = registry::validate_unique_worker_aliases()
        .unwrap_err()
        .to_string();

    assert!(error.contains("worker_name `duplicate.worker`"));
    assert!(error.contains(&first::DuplicateWorker::class_name()));
    assert!(error.contains(&second::DuplicateWorker::class_name()));
}
