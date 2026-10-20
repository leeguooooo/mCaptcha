use std::sync::Arc;

use async_trait::async_trait;
use db_core::errors::DBResult;
use db_core::{CreatePerformanceAnalytics, Secret};
use libmcaptcha::cache::messages::VerifyCaptchaResult;
use libmcaptcha::pow::Work;

use crate::data::Data;
use crate::errors::ServiceResult;

use super::resilience::{best_effort, spawn_background};

#[async_trait]
pub trait PowRuntime: Send + Sync {
    async fn verify_pow(&self, work: Work, ip: String) -> ServiceResult<(String, u32)>;
    async fn get_secret_from_captcha(&self, key: &str) -> ServiceResult<Secret>;
    async fn validate_verification_tokens(
        &self,
        payload: VerifyCaptchaResult,
    ) -> ServiceResult<bool>;

    async fn record_solve(&self, key: &str) -> DBResult<()>;
    async fn record_analytics(
        &self,
        key: &str,
        analytics: &CreatePerformanceAnalytics,
    ) -> DBResult<()>;
    async fn update_max_nonce(
        &self,
        key: &str,
        difficulty_factor: u32,
        nonce: u32,
    ) -> DBResult<()>;
    async fn record_confirm(&self, key: &str) -> DBResult<()>;
}

#[async_trait]
impl PowRuntime for Data {
    async fn verify_pow(&self, work: Work, ip: String) -> ServiceResult<(String, u32)> {
        Ok(self.captcha.verify_pow(work, ip).await?)
    }

    async fn get_secret_from_captcha(&self, key: &str) -> ServiceResult<Secret> {
        Ok(self.db.get_secret_from_captcha(key).await?)
    }

    async fn validate_verification_tokens(
        &self,
        payload: VerifyCaptchaResult,
    ) -> ServiceResult<bool> {
        Ok(self.captcha.validate_verification_tokens(payload).await?)
    }

    async fn record_solve(&self, key: &str) -> DBResult<()> {
        self.stats.record_solve(self, key).await
    }

    async fn record_analytics(
        &self,
        key: &str,
        analytics: &CreatePerformanceAnalytics,
    ) -> DBResult<()> {
        self.db.analysis_save(key, analytics).await
    }

    async fn update_max_nonce(
        &self,
        key: &str,
        difficulty_factor: u32,
        nonce: u32,
    ) -> DBResult<()> {
        self.db
            .update_max_nonce_for_level(key, difficulty_factor, nonce)
            .await
    }

    async fn record_confirm(&self, key: &str) -> DBResult<()> {
        self.stats.record_confirm(self, key).await
    }
}

pub struct SolveEvent {
    pub key: String,
    pub difficulty_factor: u32,
    pub nonce: u64,
    pub time: Option<u32>,
    pub worker_type: Option<String>,
}

pub fn spawn_solve(runtime: Arc<dyn PowRuntime>, event: SolveEvent) {
    spawn_background(async move {
        best_effort("record PoW solve", runtime.record_solve(&event.key)).await;

        if let (Some(time), Some(worker_type)) = (event.time, event.worker_type) {
            let analytics = CreatePerformanceAnalytics {
                difficulty_factor: event.difficulty_factor,
                time,
                worker_type,
            };
            best_effort(
                "record PoW performance analytics",
                runtime.record_analytics(&event.key, &analytics),
            )
            .await;
        }

        best_effort(
            "update maximum recorded nonce",
            runtime.update_max_nonce(
                &event.key,
                event.difficulty_factor,
                event.nonce as u32,
            ),
        )
        .await;
    });
}

pub fn spawn_confirm(runtime: Arc<dyn PowRuntime>, key: String) {
    spawn_background(async move {
        best_effort("record PoW confirmation", runtime.record_confirm(&key)).await;
    });
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

    use actix_web::body::to_bytes;
    use actix_web::http::StatusCode;
    use tokio::sync::mpsc;

    use super::*;
    use crate::api::v1::pow::verify_pow::{verify_pow_with, ApiWork, ValidationToken};
    use crate::api::v1::pow::verify_token::{
        siteverify_with, CaptchaValidateResp, VerifyCaptchaResultPayload,
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Event {
        Solve,
        Analytics,
        Nonce,
        Confirm,
    }

    struct FaultRuntime {
        events: mpsc::UnboundedSender<Event>,
        validation_result: bool,
    }

    impl FaultRuntime {
        fn new(
            validation_result: bool,
        ) -> (Arc<dyn PowRuntime>, mpsc::UnboundedReceiver<Event>) {
            let (events, receiver) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    events,
                    validation_result,
                }),
                receiver,
            )
        }

        fn failure() -> db_core::errors::DBError {
            db_core::errors::DBError::DBError(Box::new(io::Error::other(
                "injected telemetry failure",
            )))
        }
    }

    #[async_trait]
    impl PowRuntime for FaultRuntime {
        async fn verify_pow(&self, _: Work, _: String) -> ServiceResult<(String, u32)> {
            Ok(("issued-token".into(), 10))
        }

        async fn get_secret_from_captcha(&self, _: &str) -> ServiceResult<Secret> {
            Ok(Secret {
                secret: "account-secret".into(),
            })
        }

        async fn validate_verification_tokens(
            &self,
            _: VerifyCaptchaResult,
        ) -> ServiceResult<bool> {
            Ok(self.validation_result)
        }

        async fn record_solve(&self, _: &str) -> DBResult<()> {
            self.events.send(Event::Solve).unwrap();
            Err(Self::failure())
        }

        async fn record_analytics(
            &self,
            _: &str,
            _: &CreatePerformanceAnalytics,
        ) -> DBResult<()> {
            self.events.send(Event::Analytics).unwrap();
            Err(Self::failure())
        }

        async fn update_max_nonce(&self, _: &str, _: u32, _: u32) -> DBResult<()> {
            self.events.send(Event::Nonce).unwrap();
            Err(Self::failure())
        }

        async fn record_confirm(&self, _: &str) -> DBResult<()> {
            self.events.send(Event::Confirm).unwrap();
            Err(Self::failure())
        }
    }

    async fn next_event(receiver: &mut mpsc::UnboundedReceiver<Event>) -> Event {
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("background telemetry timed out")
            .expect("background telemetry channel closed")
    }

    #[actix_rt::test]
    async fn verify_success_survives_all_telemetry_errors() {
        let (runtime, mut events) = FaultRuntime::new(true);
        let response = verify_pow_with(
            runtime,
            ApiWork {
                string: "challenge".into(),
                result: "proof".into(),
                nonce: 42,
                key: "site-key".into(),
                time: Some(100),
                worker_type: Some("wasm".into()),
            },
            "127.0.0.1".into(),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body()).await.unwrap();
        let body: ValidationToken = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.token, "issued-token");
        assert_eq!(next_event(&mut events).await, Event::Solve);
        assert_eq!(next_event(&mut events).await, Event::Analytics);
        assert_eq!(next_event(&mut events).await, Event::Nonce);
    }

    #[actix_rt::test]
    async fn siteverify_success_survives_confirm_error() {
        let (runtime, mut events) = FaultRuntime::new(true);
        let response = siteverify_with(
            runtime,
            VerifyCaptchaResultPayload {
                secret: "account-secret".into(),
                key: "site-key".into(),
                token: "issued-token".into(),
            },
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body()).await.unwrap();
        let body: CaptchaValidateResp = serde_json::from_slice(&body).unwrap();
        assert!(body.valid);
        assert_eq!(next_event(&mut events).await, Event::Confirm);
    }

    #[actix_rt::test]
    async fn invalid_siteverify_does_not_record_confirm() {
        let (runtime, mut events) = FaultRuntime::new(false);
        let response = siteverify_with(
            runtime,
            VerifyCaptchaResultPayload {
                secret: "account-secret".into(),
                key: "site-key".into(),
                token: "invalid-token".into(),
            },
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body()).await.unwrap();
        let body: CaptchaValidateResp = serde_json::from_slice(&body).unwrap();
        assert!(!body.valid);
        assert!(events.recv().await.is_none());
    }
}
