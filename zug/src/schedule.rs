use super::Result;
use crate::{
    Error, Job, Processor, RedisConnection, RedisPool, RetryOpts, UnitOfWork, Worker,
    DEFAULT_JOB_STATUS_TTL_SECONDS,
};
use chrono::{DateTime, Utc};
pub use cron::Schedule as Cron;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::str::FromStr;

pub fn parse(cron: &str) -> Result<Cron> {
    Ok(Cron::from_str(cron)?)
}

pub async fn enqueue_due(
    redis: RedisPool,
    scheduled_jobs: &mut [ScheduledJob],
    now: DateTime<Utc>,
) -> Result<usize> {
    let mut conn = redis.get().await?;
    let mut enqueued = 0;

    for scheduled_job in scheduled_jobs {
        if scheduled_job.enqueue_due_direct(&mut conn, now).await? {
            enqueued += 1;
        }
    }

    Ok(enqueued)
}

pub struct Builder {
    pub(crate) name: Option<String>,
    pub(crate) queue: Option<String>,
    pub(crate) args: Option<JsonValue>,
    pub(crate) retry: Option<RetryOpts>,
    pub(crate) cron: Cron,
}

pub fn builder(cron_str: &str) -> Result<Builder> {
    Ok(Builder {
        name: None,
        queue: None,
        args: None,
        retry: None,
        cron: Cron::from_str(cron_str)?,
    })
}

impl Builder {
    pub fn name<S: Into<String>>(self, name: S) -> Builder {
        Builder {
            name: Some(name.into()),
            ..self
        }
    }

    #[must_use]
    pub fn retry<RO>(self, retry: RO) -> Builder
    where
        RO: Into<RetryOpts>,
    {
        Self {
            retry: Some(retry.into()),
            ..self
        }
    }

    pub fn queue<S: Into<String>>(self, queue: S) -> Builder {
        Builder {
            queue: Some(queue.into()),
            ..self
        }
    }

    pub fn args<Args>(self, args: Args) -> Result<Builder>
    where
        Args: Sync + Send + for<'de> serde::Deserialize<'de> + serde::Serialize + 'static,
    {
        let args = serde_json::to_value(args)?;

        let args = if args.is_array() {
            args
        } else {
            JsonValue::Array(vec![args])
        };

        Ok(Self {
            args: Some(args),
            ..self
        })
    }

    pub async fn register<W, Args>(self, processor: &mut Processor, worker: W) -> Result<()>
    where
        Args: Sync + Send + for<'de> serde::Deserialize<'de> + 'static,
        W: Worker<Args> + 'static,
    {
        processor.register(worker);
        processor.register_schedule(self.into_scheduled_job(W::class_name())?);

        Ok(())
    }

    pub fn into_scheduled_job(&self, class_name: String) -> Result<ScheduledJob> {
        let name = self.name.clone().unwrap_or_else(|| class_name.clone());
        let cron = self.cron.to_string();

        let mut scheduled_job = ScheduledJob {
            name,
            class: class_name,
            cron: cron.clone(),
            queue: self.queue.clone(),
            args: self.args.clone(),
            retry: self.retry.clone(),
            cron_schedule: Cron::from_str(&cron)?,
            next_run: None,
        };
        scheduled_job.calculate_next_after(Utc::now())?;

        Ok(scheduled_job)
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledJob {
    pub(crate) name: String,
    pub(crate) class: String,
    pub(crate) cron: String,
    pub(crate) queue: Option<String>,
    pub(crate) args: Option<JsonValue>,
    retry: Option<RetryOpts>,
    cron_schedule: Cron,
    next_run: Option<DateTime<Utc>>,
}

impl ScheduledJob {
    #[must_use]
    pub fn next_run(&self) -> Option<DateTime<Utc>> {
        self.next_run
    }

    #[must_use]
    pub fn job_id_at(&self, scheduled_at: DateTime<Utc>) -> String {
        let digest = hex::encode(Sha256::digest(self.name.as_bytes()));
        format!(
            "schedule:{}:{}",
            &digest[..16],
            scheduled_at.timestamp_millis()
        )
    }

    fn calculate_next_after(&mut self, after: DateTime<Utc>) -> Result<()> {
        self.next_run = self.cron_schedule.after(&after).next();

        if self.next_run.is_some() {
            return Ok(());
        }

        Err(Error::Message(format!(
            "Unable to fetch next scheduled time for scheduled job: class: {}, name: {}",
            &self.class, &self.name
        )))
    }

    fn calculate_next_after_run(&mut self, previous_run: DateTime<Utc>) -> Result<()> {
        self.calculate_next_after(previous_run + chrono::Duration::seconds(1))
    }

    async fn enqueue_due_direct(
        &mut self,
        redis: &mut RedisConnection,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(next_run) = self.next_run else {
            self.calculate_next_after(now)?;
            return Ok(false);
        };

        if next_run > now {
            return Ok(false);
        }

        let job = self.to_job(next_run);
        let work = UnitOfWork::from_job(job);

        tracing::debug!({
            "args" = ?self.args,
            "class" = &work.job.class,
            "queue" = &work.queue,
            "name" = &self.name,
            "cron" = &self.cron,
            "job_id" = &work.job.job_id,
        }, "Enqueueing scheduled job");

        let enqueued = work.enqueue_direct(redis).await?;
        self.calculate_next_after_run(next_run)?;

        Ok(enqueued)
    }

    #[must_use]
    pub fn to_job(&self, scheduled_at: DateTime<Utc>) -> Job {
        let args = self.args.clone().unwrap_or(JsonValue::Null);

        Job {
            queue: self.queue.clone().unwrap_or_else(|| "default".to_string()),
            class: self.class.clone(),
            job_id: self.job_id_at(scheduled_at),
            status_ttl_seconds: DEFAULT_JOB_STATUS_TTL_SECONDS,
            created_at: chrono::Utc::now().timestamp() as f64,
            enqueued_at: None,
            retry: self.retry.clone().unwrap_or(RetryOpts::Never),
            args,
            error_message: None,
            error_class: None,
            failed_at: None,
            retry_count: None,
            retried_at: None,
            retry_queue: None,
            unique_for: None,
        }
    }
}
