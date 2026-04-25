use crate::job::{duration_as_seconds, EnqueueOpts, RetryOpts, DEFAULT_JOB_STATUS_TTL_SECONDS};
use crate::{Error, RedisPool, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

pub struct WorkerOpts<Args: Send + 'static, W: Worker<Args> + ?Sized> {
    queue: String,
    retry: RetryOpts,
    args: PhantomData<Args>,
    worker: PhantomData<W>,
    unique_for: Option<std::time::Duration>,
    retry_queue: Option<String>,
    defer_by: Option<std::time::Duration>,
    status_ttl_seconds: usize,
}

impl<Args, W> WorkerOpts<Args, W>
where
    Args: Send + 'static,
    W: Worker<Args>,
{
    #[must_use]
    pub fn new() -> Self {
        Self {
            queue: "default".into(),
            retry: RetryOpts::Yes,
            args: PhantomData,
            worker: PhantomData,
            unique_for: None,
            retry_queue: None,
            defer_by: None,
            status_ttl_seconds: DEFAULT_JOB_STATUS_TTL_SECONDS,
        }
    }

    #[must_use]
    pub fn retry<RO>(self, retry: RO) -> Self
    where
        RO: Into<RetryOpts>,
    {
        Self {
            retry: retry.into(),
            ..self
        }
    }

    #[must_use]
    pub fn retry_queue<S: Into<String>>(self, retry_queue: S) -> Self {
        Self {
            retry_queue: Some(retry_queue.into()),
            ..self
        }
    }

    #[must_use]
    pub fn queue<S: Into<String>>(self, queue: S) -> Self {
        Self {
            queue: queue.into(),
            ..self
        }
    }

    #[must_use]
    pub fn unique_for(self, unique_for: std::time::Duration) -> Self {
        Self {
            unique_for: Some(unique_for),
            ..self
        }
    }

    #[must_use]
    pub fn defer_by(self, defer_by: std::time::Duration) -> Self {
        Self {
            defer_by: Some(defer_by),
            ..self
        }
    }

    #[must_use]
    pub fn job_status_ttl(self, status_ttl: std::time::Duration) -> Self {
        Self {
            status_ttl_seconds: duration_as_seconds(status_ttl),
            ..self
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn into_opts(&self) -> EnqueueOpts {
        self.into()
    }

    pub async fn perform_async(
        &self,
        redis: &RedisPool,
        args: impl serde::Serialize + Send + 'static,
    ) -> Result<Option<String>> {
        self.into_opts()
            .perform_async(redis, W::class_name(), args)
            .await
    }
}

impl<Args, W> From<&WorkerOpts<Args, W>> for EnqueueOpts
where
    Args: Send + 'static,
    W: Worker<Args>,
{
    fn from(opts: &WorkerOpts<Args, W>) -> Self {
        Self {
            retry: opts.retry.clone(),
            queue: opts.queue.clone(),
            unique_for: opts.unique_for,
            retry_queue: opts.retry_queue.clone(),
            defer_by: opts.defer_by,
            status_ttl_seconds: opts.status_ttl_seconds,
        }
    }
}

impl<Args, W> Default for WorkerOpts<Args, W>
where
    Args: Send + 'static,
    W: Worker<Args>,
{
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
pub trait Worker<Args: Send + 'static>: Send + Sync {
    /// Signal to WorkerRef to not attempt to modify the JsonValue args
    /// before calling the perform function. This is useful if the args
    /// are expected to be a `Vec<T>` that might be `len() == 1` or a
    /// single sized tuple `(T,)`.
    fn disable_argument_coercion(&self) -> bool {
        false
    }

    #[must_use]
    fn opts() -> WorkerOpts<Args, Self>
    where
        Self: Sized,
    {
        WorkerOpts::new()
    }

    /// default to 5 retries
    fn max_retries(&self) -> usize {
        5
    }

    /// Derive a class name from the Worker type for serialized jobs.
    ///
    /// The default is the full Rust type path, including the crate and module path, so workers
    /// with the same type name in different modules do not collide. Override this when you need a
    /// shorter stable name for configs or serialized jobs.
    #[must_use]
    fn class_name() -> String
    where
        Self: Sized,
    {
        std::any::type_name::<Self>().to_string()
    }

    async fn perform_async(redis: &RedisPool, args: Args) -> Result<Option<String>>
    where
        Self: Sized,
        Args: Send + Sync + serde::Serialize + 'static,
    {
        Self::opts().perform_async(redis, args).await
    }

    /// Process a job with access to processor-level application state.
    ///
    /// Override this when the worker needs dependencies such as a database pool, HTTP client,
    /// configuration, or Zug's Redis pool from [`WorkerContext`]. Workers that do not need shared
    /// state can implement [`Worker::perform`] instead.
    async fn perform_with_context(&self, _context: WorkerContext, args: Args) -> Result<()> {
        self.perform(args).await
    }

    /// Process a job without processor-level application state.
    ///
    /// Implement this for simple workers. Workers that need injected state can implement
    /// [`Worker::perform_with_context`] instead.
    async fn perform(&self, _args: Args) -> Result<()> {
        Err(Error::Message(
            "worker must implement perform or perform_with_context".to_string(),
        ))
    }
}

/// Per-job context shared with workers and middleware.
///
/// This is Zug's equivalent of arq's `ctx` object or axum's typed state pattern. Attach typed
/// application state with [`Processor::with_state`](crate::Processor::with_state), then retrieve it
/// in a worker with [`WorkerContext::state`].
#[derive(Clone)]
pub struct WorkerContext {
    redis: RedisPool,
    state: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl WorkerContext {
    #[must_use]
    pub(crate) fn new(redis: RedisPool) -> Self {
        Self {
            redis,
            state: Arc::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn redis_pool(&self) -> RedisPool {
        self.redis.clone()
    }

    /// Return typed application state previously attached to the processor.
    ///
    /// The state is returned as an [`Arc<T>`], so it is cheap to clone and share across worker
    /// tasks. Store cloneable pools and clients directly, or wrap larger application structs in the
    /// state type.
    pub fn state<T>(&self) -> Result<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.state
            .get(&TypeId::of::<T>())
            .cloned()
            .ok_or_else(|| {
                Error::Message(format!(
                    "missing worker state: {}",
                    std::any::type_name::<T>()
                ))
            })?
            .downcast::<T>()
            .map_err(|_| {
                Error::Message(format!(
                    "worker state has unexpected type: {}",
                    std::any::type_name::<T>()
                ))
            })
    }

    pub(crate) fn insert_state<T>(&mut self, state: T)
    where
        T: Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.state).insert(TypeId::of::<T>(), Arc::new(state));
    }
}

#[derive(Clone)]
pub struct WorkerRef {
    #[allow(clippy::type_complexity)]
    work_fn: Arc<
        Box<dyn Fn(JsonValue) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>,
    >,
    #[allow(clippy::type_complexity)]
    context_work_fn: Arc<
        Box<
            dyn Fn(WorkerContext, JsonValue) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
                + Send
                + Sync,
        >,
    >,
    max_retries: usize,
}

async fn invoke_worker<Args, W>(args: JsonValue, worker: Arc<W>) -> Result<()>
where
    Args: Send + Sync + 'static,
    W: Worker<Args> + 'static,
    for<'de> Args: Deserialize<'de>,
{
    let args = if worker.disable_argument_coercion() {
        args
    } else if std::any::TypeId::of::<Args>() == std::any::TypeId::of::<()>() {
        JsonValue::Null
    } else {
        match args {
            JsonValue::Array(mut arr) if arr.len() == 1 => {
                arr.pop().expect("value change after size check")
            }
            _ => args,
        }
    };

    let args: Args = serde_json::from_value(args)?;
    worker.perform(args).await
}

async fn invoke_worker_with_context<Args, W>(
    context: WorkerContext,
    args: JsonValue,
    worker: Arc<W>,
) -> Result<()>
where
    Args: Send + Sync + 'static,
    W: Worker<Args> + 'static,
    for<'de> Args: Deserialize<'de>,
{
    let args = if worker.disable_argument_coercion() {
        args
    } else if std::any::TypeId::of::<Args>() == std::any::TypeId::of::<()>() {
        JsonValue::Null
    } else {
        match args {
            JsonValue::Array(mut arr) if arr.len() == 1 => {
                arr.pop().expect("value change after size check")
            }
            _ => args,
        }
    };

    let args: Args = serde_json::from_value(args)?;
    worker.perform_with_context(context, args).await
}

impl WorkerRef {
    pub(crate) fn wrap<Args, W>(worker: Arc<W>) -> Self
    where
        Args: Send + Sync + 'static,
        W: Worker<Args> + 'static,
        for<'de> Args: Deserialize<'de>,
    {
        Self {
            work_fn: Arc::new(Box::new({
                let worker = worker.clone();
                move |args: JsonValue| {
                    let worker = worker.clone();
                    Box::pin(async move { invoke_worker(args, worker).await })
                }
            })),
            context_work_fn: Arc::new(Box::new({
                let worker = worker.clone();
                move |context: WorkerContext, args: JsonValue| {
                    let worker = worker.clone();
                    Box::pin(async move { invoke_worker_with_context(context, args, worker).await })
                }
            })),
            max_retries: worker.max_retries(),
        }
    }

    #[must_use]
    pub fn max_retries(&self) -> usize {
        self.max_retries
    }

    pub async fn call(&self, args: JsonValue) -> Result<()> {
        (Arc::clone(&self.work_fn))(args).await
    }

    pub async fn call_with_context(&self, context: WorkerContext, args: JsonValue) -> Result<()> {
        (Arc::clone(&self.context_work_fn))(context, args).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    mod my {
        pub mod cool {
            pub mod workers {
                use crate::{Result, RetryOpts, Worker, WorkerOpts};
                use async_trait::async_trait;

                #[allow(dead_code)]
                pub struct TestOpts;

                #[async_trait]
                impl Worker<()> for TestOpts {
                    fn opts() -> WorkerOpts<(), Self>
                    where
                        Self: Sized,
                    {
                        WorkerOpts::new()
                            .retry(false)
                            .retry(42)
                            .retry(RetryOpts::Never)
                            .unique_for(std::time::Duration::from_secs(30))
                            .queue("yolo_quue")
                    }

                    async fn perform(&self, _args: ()) -> Result<()> {
                        Ok(())
                    }
                }

                pub struct X1Y2MyJob;

                #[async_trait]
                impl Worker<()> for X1Y2MyJob {
                    async fn perform(&self, _args: ()) -> Result<()> {
                        Ok(())
                    }
                }

                pub struct TestModuleWorker;

                #[async_trait]
                impl Worker<()> for TestModuleWorker {
                    async fn perform(&self, _args: ()) -> Result<()> {
                        Ok(())
                    }
                }

                pub struct TestCustomClassNameWorker;

                #[async_trait]
                impl Worker<()> for TestCustomClassNameWorker {
                    async fn perform(&self, _args: ()) -> Result<()> {
                        Ok(())
                    }

                    fn class_name() -> String
                    where
                        Self: Sized,
                    {
                        "My::Cool::Workers::TestCustomClassNameWorker".to_string()
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn default_class_name_includes_module_path() {
        assert_eq!(
            my::cool::workers::TestModuleWorker::class_name(),
            "zug::worker::test::my::cool::workers::TestModuleWorker".to_string()
        );
    }

    #[tokio::test]
    async fn default_class_name_preserves_type_name() {
        assert_eq!(
            my::cool::workers::X1Y2MyJob::class_name(),
            "zug::worker::test::my::cool::workers::X1Y2MyJob".to_string()
        );
    }

    #[tokio::test]
    async fn supports_custom_class_name_for_workers() {
        assert_eq!(
            my::cool::workers::TestCustomClassNameWorker::class_name(),
            "My::Cool::Workers::TestCustomClassNameWorker".to_string()
        );
    }

    #[derive(Clone, Deserialize, Serialize, Debug)]
    struct TestArg {
        name: String,
        age: i32,
    }

    struct TestGenericWorker;

    #[async_trait]
    impl Worker<TestArg> for TestGenericWorker {
        async fn perform(&self, _args: TestArg) -> Result<()> {
            Ok(())
        }
    }

    struct TestMultiArgWorker;

    #[async_trait]
    impl Worker<(TestArg, TestArg)> for TestMultiArgWorker {
        async fn perform(&self, _args: (TestArg, TestArg)) -> Result<()> {
            Ok(())
        }
    }

    struct TestTupleArgWorker;

    #[async_trait]
    impl Worker<(TestArg,)> for TestTupleArgWorker {
        fn disable_argument_coercion(&self) -> bool {
            true
        }

        async fn perform(&self, _args: (TestArg,)) -> Result<()> {
            Ok(())
        }
    }

    struct TestVecArgWorker;

    #[async_trait]
    impl Worker<Vec<TestArg>> for TestVecArgWorker {
        fn disable_argument_coercion(&self) -> bool {
            true
        }

        async fn perform(&self, _args: Vec<TestArg>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn can_have_a_vec_with_one_or_more_items() {
        let worker = Arc::new(TestVecArgWorker);
        let wrap = Arc::new(WorkerRef::wrap(worker));
        let arg = serde_json::to_value(vec![TestArg {
            name: "test A".into(),
            age: 1337,
        }])
        .unwrap();
        wrap.call(arg).await.unwrap();

        let worker = Arc::new(TestVecArgWorker);
        let wrap = Arc::new(WorkerRef::wrap(worker));
        let arg = serde_json::to_value(vec![
            TestArg {
                name: "test A".into(),
                age: 1337,
            },
            TestArg {
                name: "test A".into(),
                age: 1337,
            },
        ])
        .unwrap();
        wrap.call(arg).await.unwrap();
    }

    #[tokio::test]
    async fn can_have_multiple_arguments() {
        let worker = Arc::new(TestMultiArgWorker);
        let wrap = Arc::new(WorkerRef::wrap(worker));
        let arg = serde_json::to_value((
            TestArg {
                name: "test A".into(),
                age: 1337,
            },
            TestArg {
                name: "test B".into(),
                age: 1336,
            },
        ))
        .unwrap();
        wrap.call(arg).await.unwrap();
    }

    #[tokio::test]
    async fn can_have_a_single_tuple_argument() {
        let worker = Arc::new(TestTupleArgWorker);
        let wrap = Arc::new(WorkerRef::wrap(worker));
        let arg = serde_json::to_value((TestArg {
            name: "test".into(),
            age: 1337,
        },))
        .unwrap();
        wrap.call(arg).await.unwrap();
    }

    #[tokio::test]
    async fn can_have_a_single_argument() {
        let worker = Arc::new(TestGenericWorker);
        let wrap = Arc::new(WorkerRef::wrap(worker));
        let arg = serde_json::to_value(TestArg {
            name: "test".into(),
            age: 1337,
        })
        .unwrap();
        wrap.call(arg).await.unwrap();
    }
}
