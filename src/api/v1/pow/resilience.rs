use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

const BACKGROUND_OPERATION_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn best_effort_with_timeout<F, T, E>(
    operation: &'static str,
    timeout: Duration,
    future: F,
) -> Option<T>
where
    F: Future<Output = Result<T, E>>,
    E: Display,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => Some(value),
        Ok(Err(error)) => {
            log::warn!("{operation} failed: {error}");
            None
        }
        Err(_) => {
            log::warn!("{operation} timed out after {timeout:?}");
            None
        }
    }
}

pub async fn best_effort<F, T, E>(operation: &'static str, future: F) -> Option<T>
where
    F: Future<Output = Result<T, E>>,
    E: Display,
{
    best_effort_with_timeout(operation, BACKGROUND_OPERATION_TIMEOUT, future).await
}

pub fn spawn_background<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    actix_web::rt::spawn(future);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_rt::test]
    async fn best_effort_returns_none_for_errors_and_timeouts() {
        let error = best_effort("error", async { Err::<(), _>("boom") }).await;
        assert!(error.is_none());

        let timeout =
            best_effort_with_timeout("timeout", Duration::from_millis(1), async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok::<_, &str>(())
            })
            .await;
        assert!(timeout.is_none());
    }
}
