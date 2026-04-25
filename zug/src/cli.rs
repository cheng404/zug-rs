use crate::{
    registry, schedule, BalanceStrategy, Error, Processor, ProcessorConfig, QueueConfig,
    RedisConnectionManager, RedisPool, Result, RetryOpts,
};
use bb8::Pool;
use serde::{de, Deserialize, Deserializer};
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG_PATH: &str = "zug.toml";

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub redis_url: Option<String>,
    pub queues: Vec<QueueSettings>,
    pub processor: ProcessorSettings,
    pub schedules: Vec<ScheduleSettings>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ProcessorSettings {
    pub concurrency: Option<usize>,
    pub balance_strategy: Option<BalanceStrategySetting>,
    pub poll_interval_ms: Option<u64>,
    pub lease_timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BalanceStrategySetting {
    RoundRobin,
    None,
}

#[derive(Clone, Debug, Deserialize)]
pub struct QueueSettings {
    pub name: String,
    pub concurrency: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct ScheduleSettings {
    pub name: Option<String>,
    pub worker: Option<String>,
    pub worker_name: Option<String>,
    pub cron: String,
    pub queue: Option<String>,
    pub args: Option<JsonValue>,
    pub retry: Option<RetryOpts>,
}

#[derive(Deserialize)]
struct RawScheduleSettings {
    name: Option<String>,
    #[serde(alias = "class")]
    worker: Option<String>,
    worker_name: Option<String>,
    cron: String,
    queue: Option<String>,
    args: Option<JsonValue>,
    retry: Option<RetryOpts>,
}

impl<'de> Deserialize<'de> for ScheduleSettings {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawScheduleSettings::deserialize(deserializer)?;

        match (raw.worker.is_some(), raw.worker_name.is_some()) {
            (true, true) => {
                return Err(de::Error::custom(
                    "schedule must set either `worker` or `worker_name`, not both",
                ));
            }
            (false, false) => {
                return Err(de::Error::custom(
                    "schedule must set one of `worker` or `worker_name`",
                ));
            }
            _ => {}
        }

        Ok(Self {
            name: raw.name,
            worker: raw.worker,
            worker_name: raw.worker_name,
            cron: raw.cron,
            queue: raw.queue,
            args: raw.args,
            retry: raw.retry,
        })
    }
}

impl Config {
    pub fn from_toml_str(input: &str) -> Result<Self> {
        Ok(toml::from_str(input)?)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_toml_str(&std::fs::read_to_string(path)?)
    }

    #[must_use]
    pub fn redis_url(&self) -> String {
        self.redis_url
            .clone()
            .or_else(|| std::env::var("ZUG_REDIS_URL").ok())
            .unwrap_or_else(|| "redis://127.0.0.1/".to_string())
    }

    pub fn enabled_queues(&self) -> Result<Vec<String>> {
        let registered_queues = registry::registered_queue_names();
        let queues = if self.queues.is_empty() {
            registered_queues.iter().cloned().collect()
        } else {
            let mut seen = BTreeSet::new();
            let mut queues = Vec::new();

            for queue in &self.queues {
                if queue.name.trim().is_empty() {
                    return Err(Error::Message("queue name cannot be empty".to_string()));
                }

                if !seen.insert(queue.name.clone()) {
                    return Err(Error::Message(format!(
                        "queue `{}` is configured more than once",
                        queue.name
                    )));
                }

                queues.push(queue.name.clone());
            }

            queues
        };

        if queues.is_empty() {
            return Err(Error::Message(
                "no queues configured; add `queues = [{ name = \"default\" }]` to zug.toml or register queues with `register_queue!`"
                    .to_string(),
            ));
        }

        Ok(queues)
    }
}

impl ProcessorSettings {
    fn apply_to(&self, mut config: ProcessorConfig) -> ProcessorConfig {
        if let Some(concurrency) = self.concurrency {
            config = config.concurrency(concurrency);
        }

        if let Some(balance_strategy) = &self.balance_strategy {
            config = config.balance_strategy(match balance_strategy {
                BalanceStrategySetting::RoundRobin => BalanceStrategy::RoundRobin,
                BalanceStrategySetting::None => BalanceStrategy::None,
            });
        }

        if let Some(poll_interval_ms) = self.poll_interval_ms {
            config = config.poll_interval(std::time::Duration::from_millis(poll_interval_ms));
        }

        if let Some(lease_timeout_secs) = self.lease_timeout_secs {
            config = config.lease_timeout(std::time::Duration::from_secs(lease_timeout_secs));
        }

        config
    }
}

pub fn build_processor(redis: RedisPool, config: &Config) -> Result<Processor> {
    registry::validate_unique_worker_aliases()?;

    let queues = config.enabled_queues()?;
    let queue_set = queues.iter().cloned().collect::<BTreeSet<_>>();
    let mut processor_config = config.processor.apply_to(ProcessorConfig::default());

    for queue in &config.queues {
        if let Some(concurrency) = queue.concurrency {
            if concurrency == 0 {
                return Err(Error::Message(format!(
                    "queue `{}` must set concurrency to at least 1",
                    queue.name
                )));
            }
            let processor_queue_config = QueueConfig::default().concurrency(concurrency);
            processor_config =
                processor_config.queue_config(queue.name.clone(), processor_queue_config);
        }
    }

    let mut processor = Processor::with_queues(redis.clone(), queues).with_config(processor_config);
    registry::register_workers(&mut processor)?;
    register_schedules(&mut processor, config, &queue_set)?;

    Ok(processor)
}

pub async fn run(config: Config) -> Result<()> {
    let manager = RedisConnectionManager::new(config.redis_url())?;
    let redis = Pool::builder().build(manager).await?;
    let processor = build_processor(redis, &config)?;
    processor.run().await;
    Ok(())
}

pub async fn run_from_config_file(path: impl AsRef<Path>) -> Result<()> {
    run(Config::from_path(path)?).await
}

pub async fn run_cli() -> Result<()> {
    if let Some(path) = config_path_from_args(std::env::args().skip(1))? {
        run_from_config_file(path).await
    } else {
        print_usage();
        Ok(())
    }
}

fn register_schedules(
    processor: &mut Processor,
    config: &Config,
    enabled_queues: &BTreeSet<String>,
) -> Result<()> {
    let registered_workers = registry::registered_worker_names();

    for schedule_settings in &config.schedules {
        let worker = resolve_schedule_worker(schedule_settings, &registered_workers)?;

        let queue = schedule_settings
            .queue
            .clone()
            .or_else(|| registry::registered_worker_default_queue(&worker))
            .unwrap_or_else(|| "default".to_string());

        if !enabled_queues.contains(&queue) {
            return Err(Error::Message(format!(
                "schedule `{}` targets queue `{queue}`, but that queue is not enabled",
                schedule_settings
                    .name
                    .as_deref()
                    .or(schedule_settings.worker.as_deref())
                    .or(schedule_settings.worker_name.as_deref())
                    .unwrap_or("<unnamed>")
            )));
        }

        let mut builder = schedule::builder(&schedule_settings.cron)?.queue(queue);
        if let Some(name) = &schedule_settings.name {
            builder = builder.name(name.clone());
        }
        if let Some(args) = &schedule_settings.args {
            builder = builder.args(args.clone())?;
        }
        if let Some(retry) = &schedule_settings.retry {
            builder = builder.retry(retry.clone());
        }

        processor.register_schedule(builder.into_scheduled_job(worker)?);
    }

    Ok(())
}

fn resolve_schedule_worker(
    schedule_settings: &ScheduleSettings,
    registered_workers: &BTreeSet<String>,
) -> Result<String> {
    if let Some(worker) = &schedule_settings.worker {
        if !registered_workers.contains(worker) {
            return Err(Error::Message(format!(
                "scheduled worker `{worker}` was not registered with register_worker!"
            )));
        }

        return Ok(worker.clone());
    }

    let worker_name = schedule_settings.worker_name.as_deref().ok_or_else(|| {
        Error::Message("schedule must set one of `worker` or `worker_name`".to_string())
    })?;

    registry::registered_worker_class_name_for_alias(worker_name)
}

fn config_path_from_args(args: impl IntoIterator<Item = String>) -> Result<Option<PathBuf>> {
    let mut args = args.into_iter();
    let mut config_path = PathBuf::from(DEFAULT_CONFIG_PATH);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "run" => {}
            "-c" | "--config" => {
                let Some(path) = args.next() else {
                    return Err(Error::Message("missing value for --config".to_string()));
                };
                config_path = PathBuf::from(path);
            }
            "-h" | "--help" | "help" => return Ok(None),
            value if value.starts_with("--config=") => {
                config_path = PathBuf::from(value.trim_start_matches("--config="));
            }
            value => {
                return Err(Error::Message(format!(
                    "unknown zug CLI argument `{value}`\n{}",
                    usage()
                )));
            }
        }
    }

    Ok(Some(config_path))
}

fn print_usage() {
    println!("{}", usage());
}

fn usage() -> &'static str {
    "Usage: zug run [--config zug.toml]"
}

#[cfg(test)]
mod tests {
    use super::{config_path_from_args, Config, DEFAULT_CONFIG_PATH};
    use crate::RetryOpts;
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn parses_config_from_toml() {
        let config = Config::from_toml_str(
            r#"
redis_url = "redis://127.0.0.1:6380/"
queues = [
    { name = "default" },
    { name = "reports", concurrency = 1 },
]

[processor]
concurrency = 4
balance_strategy = "none"
poll_interval_ms = 250
lease_timeout_secs = 45

[[schedules]]
name = "nightly report"
worker_name = "report.nightly"
cron = "0 0 0 * * *"
queue = "reports"
retry = 3
args = { user_guid = "USR-123" }
"#,
        )
        .unwrap();

        assert_eq!(config.redis_url.as_deref(), Some("redis://127.0.0.1:6380/"));
        assert_eq!(config.queues[0].name, "default");
        assert_eq!(config.queues[0].concurrency, None);
        assert_eq!(config.queues[1].name, "reports");
        assert_eq!(config.queues[1].concurrency, Some(1));
        assert_eq!(config.processor.concurrency, Some(4));
        assert_eq!(config.schedules[0].worker, None);
        assert_eq!(
            config.schedules[0].worker_name.as_deref(),
            Some("report.nightly")
        );
        assert_eq!(config.schedules[0].retry, Some(RetryOpts::Max(3)));
        assert_eq!(
            config.schedules[0].args,
            Some(json!({ "user_guid": "USR-123" }))
        );
    }

    #[test]
    fn rejects_schedule_with_both_worker_fields() {
        let config = Config::from_toml_str(
            r#"
[[schedules]]
name = "nightly report"
worker = "my_app::reports::ReportWorker"
worker_name = "report.nightly"
cron = "0 0 0 * * *"
"#,
        );

        let error = config.unwrap_err().to_string();
        assert!(error.contains("either `worker` or `worker_name`, not both"));
    }

    #[test]
    fn rejects_schedule_without_worker_fields() {
        let config = Config::from_toml_str(
            r#"
[[schedules]]
name = "nightly report"
cron = "0 0 0 * * *"
"#,
        );

        let error = config.unwrap_err().to_string();
        assert!(error.contains("one of `worker` or `worker_name`"));
    }

    #[test]
    fn rejects_legacy_queue_configs_from_toml() {
        let config = Config::from_toml_str(
            r#"
queues = [{ name = "reports" }]

[queue_configs.reports]
concurrency = 1
"#,
        );

        assert!(config.is_err());
    }

    #[test]
    fn parses_cli_config_path() {
        assert_eq!(
            config_path_from_args(Vec::<String>::new()).unwrap(),
            Some(PathBuf::from(DEFAULT_CONFIG_PATH))
        );
        assert_eq!(
            config_path_from_args([
                "run".to_string(),
                "--config".to_string(),
                "worker.toml".to_string()
            ])
            .unwrap(),
            Some(PathBuf::from("worker.toml"))
        );
        assert_eq!(
            config_path_from_args(["--config=worker.toml".to_string()]).unwrap(),
            Some(PathBuf::from("worker.toml"))
        );
    }
}
