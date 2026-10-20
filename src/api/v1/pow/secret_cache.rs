//! Last-known account secret per sitekey, so siteverify keeps working while
//! the database is unavailable.

use std::collections::HashMap;
use std::future::Future;
use std::sync::RwLock;
use std::time::Duration;

use db_core::errors::{DBError, DBResult};
use db_core::Secret;

use crate::errors::{ServiceError, ServiceResult};

const SECRET_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Default)]
pub struct SecretCache(RwLock<HashMap<String, String>>);

impl SecretCache {
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
                self.0
                    .write()
                    .unwrap()
                    .insert(key.to_owned(), secret.secret.clone());
                return Ok(secret);
            }
            Ok(Err(DBError::DBError(e))) => DBError::DBError(e),
            Ok(Err(e)) => {
                self.0.write().unwrap().remove(key);
                return Err(e.into());
            }
            Err(_) => DBError::DBError(
                format!("secret lookup timed out after {SECRET_LOOKUP_TIMEOUT:?}")
                    .into(),
            ),
        };

        if let Some(secret) = self.0.read().unwrap().get(key) {
            log::warn!("secret lookup failed, using cached secret: {unavailable}");
            return Ok(Secret {
                secret: secret.clone(),
            });
        }
        Err(ServiceError::from(unavailable))
    }
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
}
