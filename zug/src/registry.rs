use crate::{Error, Processor, Result, Worker};
use std::collections::{BTreeMap, BTreeSet};

pub struct WorkerRegistration {
    pub class_name: fn() -> String,
    pub default_queue: fn() -> String,
    pub retry_queue: fn() -> Option<String>,
    pub register: fn(&mut Processor) -> Result<()>,
}

pub struct WorkerAliasRegistration {
    pub name: &'static str,
    pub class_name: fn() -> String,
}

pub struct QueueRegistration {
    pub name: &'static str,
}

inventory::collect!(WorkerRegistration);
inventory::collect!(WorkerAliasRegistration);
inventory::collect!(QueueRegistration);

pub fn worker_class_name<Args, W>() -> String
where
    Args: Send + 'static,
    W: Worker<Args>,
{
    W::class_name()
}

pub fn worker_default_queue<Args, W>() -> String
where
    Args: Send + 'static,
    W: Worker<Args>,
{
    W::opts().into_opts().queue_name().to_string()
}

pub fn worker_retry_queue<Args, W>() -> Option<String>
where
    Args: Send + 'static,
    W: Worker<Args>,
{
    W::opts()
        .into_opts()
        .retry_queue_name()
        .map(ToOwned::to_owned)
}

pub fn register_default_worker<Args, W>(processor: &mut Processor) -> Result<()>
where
    Args: Sync + Send + for<'de> serde::Deserialize<'de> + 'static,
    W: Default + Worker<Args> + 'static,
{
    processor.register::<Args, W>(W::default());
    Ok(())
}

pub fn register_workers(processor: &mut Processor) -> Result<()> {
    for worker in inventory::iter::<WorkerRegistration> {
        (worker.register)(processor)?;
    }

    Ok(())
}

#[must_use]
pub fn registered_worker_names() -> BTreeSet<String> {
    inventory::iter::<WorkerRegistration>
        .into_iter()
        .map(|worker| (worker.class_name)())
        .collect()
}

#[must_use]
pub fn registered_worker_aliases() -> BTreeSet<String> {
    inventory::iter::<WorkerAliasRegistration>
        .into_iter()
        .map(|worker| worker.name.to_string())
        .collect()
}

pub fn registered_worker_class_name_for_alias(worker_name: &str) -> Result<String> {
    let matches = inventory::iter::<WorkerAliasRegistration>
        .into_iter()
        .filter(|worker| worker.name == worker_name)
        .map(|worker| (worker.class_name)())
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [worker] => Ok(worker.clone()),
        [] => Err(Error::Message(format!(
            "worker_name `{worker_name}` was not registered"
        ))),
        workers => Err(duplicate_worker_name_error(worker_name, workers)),
    }
}

pub fn validate_unique_worker_aliases() -> Result<()> {
    let mut aliases = BTreeMap::<&'static str, Vec<String>>::new();

    for worker in inventory::iter::<WorkerAliasRegistration> {
        aliases
            .entry(worker.name)
            .or_default()
            .push((worker.class_name)());
    }

    for (name, workers) in aliases {
        if workers.len() > 1 {
            return Err(duplicate_worker_name_error(name, &workers));
        }
    }

    Ok(())
}

fn duplicate_worker_name_error(worker_name: &str, workers: &[String]) -> Error {
    Error::Message(format!(
        "worker_name `{worker_name}` is registered by multiple workers: {}",
        workers.join(", ")
    ))
}

#[must_use]
pub fn registered_worker_default_queue(class_name: &str) -> Option<String> {
    inventory::iter::<WorkerRegistration>
        .into_iter()
        .find(|worker| (worker.class_name)() == class_name)
        .map(|worker| (worker.default_queue)())
}

#[must_use]
pub fn registered_queue_names() -> BTreeSet<String> {
    let mut queues: BTreeSet<String> = inventory::iter::<QueueRegistration>
        .into_iter()
        .map(|queue| queue.name.to_string())
        .collect();

    queues.extend(
        inventory::iter::<WorkerRegistration>
            .into_iter()
            .map(|worker| (worker.default_queue)()),
    );

    queues.extend(
        inventory::iter::<WorkerRegistration>
            .into_iter()
            .filter_map(|worker| (worker.retry_queue)()),
    );

    queues
}

#[macro_export]
macro_rules! register_worker {
    ($processor:expr $(,)?) => {{ $processor.register_registered_workers() }};
    ($processor:expr, $crate_name:ident :: * $(,)?) => {{
        extern crate $crate_name as _;
        $processor.register_registered_workers()
    }};
    ($processor:expr, $root:ident $(:: $segment:ident)+ :: * $(,)?) => {{
        #[allow(unused_imports)]
        use $root $(:: $segment)+ ::*;
        $processor.register_registered_workers()
    }};
    ($worker:ty, $args:ty $(,)?) => {
        $crate::__private::inventory::submit! {
            $crate::registry::WorkerRegistration {
                class_name: $crate::registry::worker_class_name::<$args, $worker>,
                default_queue: $crate::registry::worker_default_queue::<$args, $worker>,
                retry_queue: $crate::registry::worker_retry_queue::<$args, $worker>,
                register: $crate::registry::register_default_worker::<$args, $worker>,
            }
        }
    };
}

#[macro_export]
macro_rules! register_queue {
    ($queue:expr $(,)?) => {
        $crate::__private::inventory::submit! {
            $crate::registry::QueueRegistration { name: $queue }
        }
    };
}
