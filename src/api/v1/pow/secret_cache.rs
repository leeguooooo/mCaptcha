//! Last-known account secret per sitekey, so siteverify keeps working while
//! the database is unavailable.
//!
//! Secrets are kept in memory and, when Redis is configured, also in Redis so
//! that an instance restarted during a database outage can still verify.

use std::collections::HashMap;
use std::future::Future;
use std::sync::RwLock;
use std::time::Duration;

use db_core::errors::{DBError, DBResult};
use db_core::Secret;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use crate::errors::{ServiceError, ServiceResult};

const SECRET_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
const REDIS_TIMEOUT: Duration = Duration::from_millis(500);
const REDIS_KEY_PREFIX: &str = "mcaptcha:siteverify-secret:";

#[derive(Default)]
pub struct SecretCache {
    memory: RwLock<HashMap<String, String>>,
    redis: Option<ConnectionManager>,
}

impl SecretCache {
    pub fn new(redis: Option<ConnectionManager>) -> Self {
        Self {
            memory: RwLock::default(),
            redis,
        }
    }

    /// Connect the Redis-backed store. Failure only disables the Redis copy.
    pub async fn connect(redis_url: Option<&str>) -> Self {
        let Some(url) = redis_url else {
            return Self::default();
        };
        let redis = match redis::Client::open(url) {
            Ok(client) => ConnectionManager::new(client).await,
            Err(e) => Err(e),
        };
        match redis {
            Ok(redis) => Self::new(Some(redis)),
            Err(e) => {
                log::error!("secret cache: redis unavailable, memory only: {e}");
                Self::default()
            }
        }
    }

    /// Look the secret up in the database and remember it. When the database
    /// is unreachable (connection error or timeout), fall back to the last
    /// secret seen for this key. Logical errors such as an unknown key are
    /// returned as-is and evict the key.
    pub async fn resolve<F>(&self, key: &str, lookup: F) -> ServiceResult<Secret>
    where
        F: Future<Output = DBResult<Secret>>,
    {
        let unavailable = match tokio::time::timeout(SECRET_LOOKUP_TIMEOUT, lookup).await
        {
            Ok(Ok(secret)) => {
                self.remember(key, &secret.secret).await;
                return Ok(secret);
            }
            Ok(Err(DBError::DBError(e))) => DBError::DBError(e),
            Ok(Err(e)) => {
                self.forget(key).await;
                return Err(e.into());
            }
            Err(_) => DBError::DBError(
                format!("secret lookup timed out after {SECRET_LOOKUP_TIMEOUT:?}")
                    .into(),
            ),
        };

        if let Some(secret) = self.recall(key).await {
            log::warn!("secret lookup failed, using cached secret: {unavailable}");
            return Ok(Secret { secret });
        }
        Err(ServiceError::from(unavailable))
    }

    async fn remember(&self, key: &str, secret: &str) {
        let previous = self
            .memory
            .write()
            .unwrap()
            .insert(key.to_owned(), secret.to_owned());
        // Only write to Redis when this instance has not stored this value yet.
        if previous.as_deref() != Some(secret) {
            self.redis_op("store", |mut r| async move {
                r.set::<_, _, ()>(redis_key(key), secret).await
            })
            .await;
        }
    }

    async fn forget(&self, key: &str) {
        self.memory.write().unwrap().remove(key);
        self.redis_op("evict", |mut r| async move {
            r.del::<_, ()>(redis_key(key)).await
        })
        .await;
    }

    async fn recall(&self, key: &str) -> Option<String> {
        if let Some(secret) = self.memory.read().unwrap().get(key) {
            return Some(secret.clone());
        }
        let secret = self
            .redis_op("load", |mut r| async move {
                r.get::<_, Option<String>>(redis_key(key)).await
            })
            .await
            .flatten()?;
        self.memory
            .write()
            .unwrap()
            .insert(key.to_owned(), secret.clone());
        Some(secret)
    }

    /// Best-effort Redis call with a short timeout; errors are logged.
    async fn redis_op<T, F, Fut>(&self, op: &str, f: F) -> Option<T>
    where
        F: FnOnce(ConnectionManager) -> Fut,
        Fut: Future<Output = redis::RedisResult<T>>,
    {
        let redis = self.redis.clone()?;
        match tokio::time::timeout(REDIS_TIMEOUT, f(redis)).await {
            Ok(Ok(v)) => Some(v),
            Ok(Err(e)) => {
                log::warn!("secret cache: redis {op} failed: {e}");
                None
            }
            Err(_) => {
                log::warn!("secret cache: redis {op} timed out after {REDIS_TIMEOUT:?}");
                None
            }
        }
    }
}

fn redis_key(key: &str) -> String {
    format!("{REDIS_KEY_PREFIX}{key}")
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn secret(s: &str) -> DBResult<Secret> {
        Ok(Secret { secret: s.into() })
    }

    fn outage() -> DBResult<Secret> {
        Err(DBError::DBError(Box::new(io::Error::other(
            "connection refused",
        ))))
    }

    #[actix_rt::test]
    async fn falls_back_to_cached_secret_when_database_is_down() {
        let cache = SecretCache::default();
        assert!(cache.resolve("key", async { outage() }).await.is_err());

        assert_eq!(
            cache
                .resolve("key", async { secret("s1") })
                .await
                .unwrap()
                .secret,
            "s1"
        );
        assert_eq!(
            cache
                .resolve("key", async { outage() })
                .await
                .unwrap()
                .secret,
            "s1"
        );
        // other keys are not served from this key's entry
        assert!(cache.resolve("other", async { outage() }).await.is_err());
    }

    #[actix_rt::test]
    async fn refreshes_on_success_and_evicts_on_logical_error() {
        let cache = SecretCache::default();
        cache.resolve("key", async { secret("s1") }).await.unwrap();
        cache.resolve("key", async { secret("s2") }).await.unwrap();
        assert_eq!(
            cache
                .resolve("key", async { outage() })
                .await
                .unwrap()
                .secret,
            "s2"
        );

        let err = cache
            .resolve("key", async { Err(DBError::AccountNotFound) })
            .await
            .unwrap_err();
        assert_eq!(err, ServiceError::AccountNotFound);
        assert!(cache.resolve("key", async { outage() }).await.is_err());
    }

    #[actix_rt::test]
    async fn slow_lookup_times_out_and_uses_cache() {
        let cache = SecretCache::default();
        cache.resolve("key", async { secret("s1") }).await.unwrap();
        let slow = async {
            tokio::time::sleep(SECRET_LOOKUP_TIMEOUT * 2).await;
            secret("never")
        };
        assert_eq!(cache.resolve("key", slow).await.unwrap().secret, "s1");
    }

    #[actix_rt::test]
    async fn survives_restart_through_redis() {
        let settings = crate::settings::Settings::new().unwrap();
        let url = settings.redis.as_ref().map(|r| r.url.as_str());
        let key = format!("restart-{}", std::process::id());

        let before = SecretCache::connect(url).await;
        assert!(before.redis.is_some(), "test needs a reachable redis");
        before.resolve(&key, async { secret("s1") }).await.unwrap();

        // a fresh instance (empty memory) during a database outage
        let after = SecretCache::connect(url).await;
        assert_eq!(
            after
                .resolve(&key, async { outage() })
                .await
                .unwrap()
                .secret,
            "s1"
        );

        // an unknown key is evicted from redis too
        after
            .resolve(&key, async { Err(DBError::AccountNotFound) })
            .await
            .unwrap_err();
        let fresh = SecretCache::connect(url).await;
        assert!(fresh.resolve(&key, async { outage() }).await.is_err());
    }
}
