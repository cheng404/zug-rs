use crate::job::ZUG_REDIS_FUNCTION_LIBRARY;
use bb8::{CustomizeConnection, ManageConnection, Pool};
use redis::AsyncCommands;
pub use redis::RedisError;
pub use redis::Value as RedisValue;
use redis::{aio::MultiplexedConnection as Connection, ErrorKind};
use redis::{AsyncConnectionConfig, Client, IntoConnectionInfo, ProtocolVersion};
use redis::{ToRedisArgs, ToSingleRedisArg};
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, OnceCell};

pub type RedisPool = Pool<RedisConnectionManager>;

#[derive(Debug)]
pub struct RedisNamespace {
    namespace: String,
}

impl CustomizeConnection<RedisConnection, RedisError> for RedisNamespace {
    fn on_acquire<'a>(
        &'a self,
        connection: &'a mut RedisConnection,
    ) -> Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send + 'a>> {
        Box::pin(async {
            // All Redis operations used by Zug will use this as a prefix.
            connection.set_namespace(self.namespace.clone());

            Ok(())
        })
    }
}

#[must_use]
pub fn with_custom_namespace(namespace: String) -> Box<RedisNamespace> {
    Box::new(RedisNamespace { namespace })
}

/// A `bb8::ManageConnection` for `redis::Client::get_async_connection` wrapped in a helper type
/// for namespacing.
#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: Client,
    functions_loaded: Arc<OnceCell<()>>,
}

impl RedisConnectionManager {
    /// Create a new `RedisConnectionManager`.
    /// See `redis::Client::open` for a description of the parameter types.
    pub fn new<T: IntoConnectionInfo>(info: T) -> Result<Self, RedisError> {
        let connection_info = info.into_connection_info()?;
        let redis_settings = connection_info
            .redis_settings()
            .clone()
            .set_protocol(ProtocolVersion::RESP3);
        let connection_info = connection_info.set_redis_settings(redis_settings);

        Ok(Self {
            client: Client::open(connection_info)?,
            functions_loaded: Arc::new(OnceCell::new()),
        })
    }

    async fn ensure_function_library_loaded(
        &self,
        connection: &mut Connection,
    ) -> Result<(), RedisError> {
        self.functions_loaded
            .get_or_try_init(|| async { load_zug_function_library(connection).await })
            .await
            .map(|_| ())
    }
}

async fn load_zug_function_library(connection: &mut Connection) -> Result<(), RedisError> {
    let _: String = redis::cmd("FUNCTION")
        .arg("LOAD")
        .arg("REPLACE")
        .arg(ZUG_REDIS_FUNCTION_LIBRARY)
        .query_async(connection)
        .await?;

    Ok(())
}

impl ManageConnection for RedisConnectionManager {
    type Connection = RedisConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        self.ensure_function_library_loaded(&mut connection).await?;

        Ok(RedisConnection::with_client(
            self.client.clone(),
            connection,
        ))
    }

    async fn is_valid(&self, mut conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn.deref_mut().connection)
            .await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((
                ErrorKind::Server(redis::ServerErrorKind::ResponseError),
                "ping request",
            )
                .into()),
        }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// A wrapper type for making the redis crate compatible with namespacing.
pub struct RedisConnection {
    client: Option<Client>,
    connection: Connection,
    namespace: Option<String>,
}

impl RedisConnection {
    #[must_use]
    pub fn new(connection: Connection) -> Self {
        Self {
            client: None,
            connection,
            namespace: None,
        }
    }

    #[must_use]
    pub fn with_client(client: Client, connection: Connection) -> Self {
        Self {
            client: Some(client),
            connection,
            namespace: None,
        }
    }

    pub fn set_namespace(&mut self, namespace: String) {
        self.namespace = Some(namespace);
    }

    #[must_use]
    pub fn with_namespace(self, namespace: String) -> Self {
        Self {
            client: self.client,
            connection: self.connection,
            namespace: Some(namespace),
        }
    }

    pub(crate) fn namespaced_key(&self, key: String) -> String {
        if let Some(ref namespace) = self.namespace {
            return format!("{namespace}:{key}");
        }

        key
    }

    /// Borrow the raw Redis connection for commands whose keys were already namespaced manually.
    pub(crate) fn unnamespaced_borrow_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    /// Execute a command whose key arguments have already been namespaced by the caller.
    pub(crate) async fn query_prepared_command<T>(
        &mut self,
        command: &mut redis::Cmd,
    ) -> Result<T, RedisError>
    where
        T: redis::FromRedisValue,
    {
        command.query_async(self.unnamespaced_borrow_mut()).await
    }

    pub async fn del(&mut self, key: String) -> Result<usize, RedisError> {
        self.connection.del(self.namespaced_key(key)).await
    }

    pub async fn exists(&mut self, key: String) -> Result<bool, RedisError> {
        self.connection.exists(self.namespaced_key(key)).await
    }

    pub async fn expire(&mut self, key: String, value: usize) -> Result<usize, RedisError> {
        self.connection
            .expire(self.namespaced_key(key), value as i64)
            .await
    }

    /// Clear the selected Redis database. This intentionally ignores Zug's key namespace.
    pub async fn flushall(&mut self) -> Result<(), RedisError> {
        redis::cmd("FLUSHALL")
            .arg("SYNC")
            .query_async(self.unnamespaced_borrow_mut())
            .await
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>, RedisError> {
        self.connection.get(self.namespaced_key(key)).await
    }

    pub async fn incr(&mut self, key: String, delta: usize) -> Result<usize, RedisError> {
        self.connection.incr(self.namespaced_key(key), delta).await
    }

    pub async fn set<V>(&mut self, key: String, value: V) -> Result<(), RedisError>
    where
        V: ToSingleRedisArg + Send + Sync,
    {
        self.connection.set(self.namespaced_key(key), value).await
    }

    pub async fn hget(&mut self, key: String, field: String) -> Result<Option<String>, RedisError> {
        self.connection.hget(self.namespaced_key(key), field).await
    }

    pub async fn hdel<V>(&mut self, key: String, field: V) -> Result<usize, RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        self.connection.hdel(self.namespaced_key(key), field).await
    }

    pub async fn hset_with_field_ttl<V>(
        &mut self,
        key: String,
        field: String,
        value: V,
        ttl_in_seconds: usize,
    ) -> Result<(), RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        let namespaced_key = self.namespaced_key(key);

        let _: usize = redis::cmd("HSET")
            .arg(&namespaced_key)
            .arg(&field)
            .arg(value)
            .query_async(self.unnamespaced_borrow_mut())
            .await?;

        let _: Vec<i64> = redis::cmd("HEXPIRE")
            .arg(namespaced_key)
            .arg(ttl_in_seconds)
            .arg("FIELDS")
            .arg(1)
            .arg(field)
            .query_async(self.unnamespaced_borrow_mut())
            .await?;

        Ok(())
    }

    pub async fn hfield_ttl(
        &mut self,
        key: String,
        field: String,
    ) -> Result<Option<i64>, RedisError> {
        let ttls: Vec<i64> = redis::cmd("HTTL")
            .arg(self.namespaced_key(key))
            .arg("FIELDS")
            .arg(1)
            .arg(field)
            .query_async(self.unnamespaced_borrow_mut())
            .await?;

        Ok(ttls.into_iter().next())
    }

    pub async fn sadd<V>(&mut self, key: String, value: V) -> Result<(), RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        self.connection.sadd(self.namespaced_key(key), value).await
    }

    pub async fn smembers(&mut self, key: String) -> Result<Vec<String>, RedisError> {
        self.connection.smembers(self.namespaced_key(key)).await
    }

    pub async fn set_nx_ex<V>(
        &mut self,
        key: String,
        value: V,
        ttl_in_seconds: usize,
    ) -> Result<RedisValue, RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        redis::cmd("SET")
            .arg(self.namespaced_key(key))
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_in_seconds)
            .query_async(self.unnamespaced_borrow_mut())
            .await
    }

    pub async fn set_nx_ex_bool<V>(
        &mut self,
        key: String,
        value: V,
        ttl_in_seconds: usize,
    ) -> Result<bool, RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        let response: Option<String> = redis::cmd("SET")
            .arg(self.namespaced_key(key))
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_in_seconds)
            .query_async(self.unnamespaced_borrow_mut())
            .await?;

        Ok(response.is_some())
    }

    pub async fn zrange(
        &mut self,
        key: String,
        lower: isize,
        upper: isize,
    ) -> Result<Vec<String>, RedisError> {
        self.connection
            .zrange(self.namespaced_key(key), lower, upper)
            .await
    }

    pub async fn zrangebyscore_limit<
        L: ToRedisArgs + ToSingleRedisArg + Send + Sync,
        U: ToRedisArgs + ToSingleRedisArg + Sync + Send,
    >(
        &mut self,
        key: String,
        lower: L,
        upper: U,
        offset: isize,
        limit: isize,
    ) -> Result<Vec<String>, RedisError> {
        self.connection
            .zrangebyscore_limit(self.namespaced_key(key), lower, upper, offset, limit)
            .await
    }

    pub async fn zadd<
        V: ToRedisArgs + ToSingleRedisArg + Send + Sync,
        S: ToRedisArgs + ToSingleRedisArg + Send + Sync,
    >(
        &mut self,
        key: String,
        value: V,
        score: S,
    ) -> Result<usize, RedisError> {
        self.connection
            .zadd(self.namespaced_key(key), value, score)
            .await
    }

    pub async fn zadd_ch<V: ToRedisArgs + Send + Sync, S: ToRedisArgs + Send + Sync>(
        &mut self,
        key: String,
        value: V,
        score: S,
    ) -> Result<bool, RedisError> {
        redis::cmd("ZADD")
            .arg(self.namespaced_key(key))
            .arg("CH")
            .arg(score)
            .arg(value)
            .query_async(self.unnamespaced_borrow_mut())
            .await
    }

    pub async fn zrem<V>(&mut self, key: String, value: V) -> Result<usize, RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        self.connection.zrem(self.namespaced_key(key), value).await
    }

    pub async fn zscore<V>(&mut self, key: String, value: V) -> Result<Option<f64>, RedisError>
    where
        V: ToSingleRedisArg + Send + Sync,
    {
        self.connection
            .zscore(self.namespaced_key(key), value)
            .await
    }

    pub async fn publish<V>(&mut self, channel: String, value: V) -> Result<usize, RedisError>
    where
        V: ToSingleRedisArg + Send + Sync,
    {
        self.connection
            .publish(self.namespaced_key(channel), value)
            .await
    }

    pub async fn subscribe(
        &self,
        channels: Vec<String>,
    ) -> Result<RedisWakeSubscription, RedisError> {
        let Some(client) = self.client.as_ref() else {
            return Err((
                ErrorKind::Client,
                "pub/sub requires a RedisConnection created by RedisConnectionManager",
            )
                .into());
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        let config = AsyncConnectionConfig::new().set_push_sender(sender);
        let mut connection = client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        for channel in channels {
            connection.subscribe(self.namespaced_key(channel)).await?;
        }

        Ok(RedisWakeSubscription {
            _connection: connection,
            receiver,
        })
    }
}

pub struct RedisWakeSubscription {
    _connection: Connection,
    receiver: mpsc::UnboundedReceiver<redis::PushInfo>,
}

impl RedisWakeSubscription {
    pub async fn recv(&mut self) -> Option<()> {
        self.receiver.recv().await.map(|_| ())
    }
}
