use crate::config::{save_token_file, StoredAuthSession, StoredUserSummary};
use aegis_core::{
    AuthLoginRequest, AuthLoginResponse, AuthLogoutResponse, AuthRefreshResponse, AuthUserResponse,
    BacktestRequest, CandleAggregationRequest, CandleAggregationResult, CandleBackfillRequest,
    CandleBackfillResult, ExchangeTestnetPipelinePreview, ExchangeTestnetPipelinePreviewRequest,
    ExchangeTestnetPipelineSubmitRequest, ExecutionReadinessRequest, ExecutionReadinessResult,
    ExecutionReadinessSnapshot, MarketCandleCoverageSummary, OperatorReport, OperatorReportRequest,
    PaperTradingPipelineRequest, PaperTradingPipelineResult, ResearchCandidate,
    ResearchCandidateDecisionRequest, ResearchCandidateLifecycleEvent,
    ResearchCandidateObservationHistoryItem, ResearchCandidateObservationSummaryView,
    ResearchCandidateQualificationChange, ResearchCandidateQualificationEvaluation,
    ResearchCandidateQualificationHistory, ResearchCandidateQualificationResult,
    ResearchCandidateQualificationThresholds, ResearchCandidateQualificationTrend,
    ResearchCandidateReview, ResearchCandidateReviewRequest, ResearchCandidateReviewResult,
    ResearchCandidateShadowPerformance, ResearchCandidateShadowPromotionPreview,
    ResearchCandidateShadowPromotionRequest, ResearchCandidateShadowPromotionResult,
    ResearchCandidateShadowRunLink, ResearchCandidateTestnetReviewDossier,
    ResearchCandidateWatchlistEntry, ResearchDataCoverageResult, ResearchDatasetBuildRequest,
    ResearchDatasetBuildResult, RiskConfig, RiskConfigAuditEntry, RiskConfigValidationResult,
    RiskConfigVersion, StrategyCandidateObservationResult, StrategyComparisonSummary,
    StrategyConfigAuditEntry, StrategyConfigUpdateRequest, StrategyConfigValidationResult,
    StrategyConfigVersion, StrategyDecisionBreakdown, StrategyDiagnosticsResult,
    StrategyDryRunRequest, StrategyDryRunResult, StrategyExperimentRequest,
    StrategyExperimentResult, StrategyExperimentRun, StrategyMultiTimeframeExperimentRequest,
    StrategyMultiTimeframeExperimentResult, StrategyPerformanceSummary, StrategyWalkForwardRequest,
    StrategyWalkForwardResult, StrategyWalkForwardWindowResult, TestnetPromotionFunnelRow,
    TestnetPromotionFunnelSummary, TestnetPromotionLifecycleBreakdown,
    TestnetPromotionOutcomeBreakdown, TestnetShadowPromotionPreview, TestnetShadowPromotionRequest,
    TestnetShadowPromotionResult, TestnetShadowPromotionSubmitRequest, TestnetShadowRunRequest,
    TestnetShadowRunResult, TestnetShadowRunnerConfig, TestnetShadowRunnerConfigInput,
    TestnetShadowRunnerControlRequest, TestnetShadowRunnerState, TestnetShadowRunnerTickResult,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use reqwest::{Method, StatusCode, Url};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

const CLI_AUTH_MODE_HEADER: &str = "x-aegis-auth-mode";
const CLI_AUTH_MODE_VALUE: &str = "cli";

#[derive(Debug, thiserror::Error)]
pub enum ApiClientError {
    #[error("request to {endpoint} failed: {message}")]
    Transport { endpoint: String, message: String },
    #[error("request to {endpoint} returned HTTP {status}: {message}")]
    Http {
        endpoint: String,
        status: StatusCode,
        message: String,
        body: Option<String>,
    },
    #[error("failed to decode response from {endpoint}: {message}")]
    Decode { endpoint: String, message: String },
    #[error("{message}")]
    LoginRequired { message: String },
}

impl ApiClientError {
    pub fn is_login_required(&self) -> bool {
        matches!(self, Self::LoginRequired { .. })
    }
}

#[derive(Debug, Clone)]
struct ClientAuthState {
    session: StoredAuthSession,
    token_path: PathBuf,
    persist_session: bool,
}

type ResponseFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(StatusCode, Vec<u8>), String>> + Send + 'a>>;

trait HttpExecutor: Send + Sync {
    fn execute<'a>(&'a self, request: reqwest::Request) -> ResponseFuture<'a>;
}

struct ReqwestExecutor {
    client: reqwest::Client,
}

impl HttpExecutor for ReqwestExecutor {
    fn execute<'a>(&'a self, request: reqwest::Request) -> ResponseFuture<'a> {
        Box::pin(async move {
            let endpoint = request.url().path().to_string();
            let response = self
                .client
                .execute(request)
                .await
                .map_err(|err| format!("{endpoint}: {err}"))?;
            let status = response.status();
            let bytes = response
                .bytes()
                .await
                .map_err(|err| format!("{endpoint}: {err}"))?;
            Ok((status, bytes.to_vec()))
        })
    }
}

#[derive(Clone)]
pub struct ApiClient {
    base_url: Url,
    request_builder: reqwest::Client,
    http: Arc<dyn HttpExecutor>,
    auth: Arc<Mutex<Option<ClientAuthState>>>,
}

impl std::fmt::Debug for ApiClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl ApiClient {
    pub fn new(base_url: Url) -> Self {
        let request_builder = reqwest::Client::new();
        Self {
            base_url,
            request_builder: request_builder.clone(),
            http: Arc::new(ReqwestExecutor {
                client: request_builder,
            }),
            auth: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    fn new_with_test_handler<F>(base_url: Url, handler: F) -> Self
    where
        F: Fn(reqwest::Request) -> Result<(StatusCode, Vec<u8>), String> + Send + Sync + 'static,
    {
        struct TestExecutor<F> {
            handler: F,
        }

        impl<F> HttpExecutor for TestExecutor<F>
        where
            F: Fn(reqwest::Request) -> Result<(StatusCode, Vec<u8>), String> + Send + Sync,
        {
            fn execute<'a>(&'a self, request: reqwest::Request) -> ResponseFuture<'a> {
                Box::pin(async move { (self.handler)(request) })
            }
        }

        Self {
            base_url,
            request_builder: reqwest::Client::new(),
            http: Arc::new(TestExecutor { handler }),
            auth: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_auth_session(
        mut self,
        session: StoredAuthSession,
        token_path: PathBuf,
        persist_session: bool,
    ) -> Self {
        self.auth = Arc::new(Mutex::new(Some(ClientAuthState {
            session,
            token_path,
            persist_session,
        })));
        self
    }

    pub async fn get<T>(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<T, ApiClientError>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.request(Method::GET, endpoint, query, Option::<&Value>::None)
            .await
    }

    pub async fn post<T, B>(&self, endpoint: &str, body: &B) -> Result<T, ApiClientError>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize + ?Sized,
    {
        self.request(Method::POST, endpoint, &[], Some(body)).await
    }

    pub async fn get_value(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<Value, ApiClientError> {
        self.get(endpoint, query).await
    }

    pub async fn get_text(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<String, ApiClientError> {
        self.request_text(Method::GET, endpoint, query).await
    }

    fn endpoint_url(&self, endpoint: &str) -> Result<Url, ApiClientError> {
        let path = endpoint.trim_start_matches('/');
        self.base_url
            .join(path)
            .map_err(|err| ApiClientError::Transport {
                endpoint: endpoint.to_string(),
                message: format!("invalid endpoint URL: {err}"),
            })
    }

    fn current_session(&self) -> Result<Option<StoredAuthSession>, ApiClientError> {
        self.auth
            .lock()
            .map(|guard| guard.as_ref().map(|state| state.session.clone()))
            .map_err(|err| ApiClientError::Transport {
                endpoint: "auth/session".to_string(),
                message: format!("failed to lock auth session: {err}"),
            })
    }

    fn current_auth_header(&self) -> Result<Option<String>, ApiClientError> {
        Ok(self
            .current_session()?
            .map(|session| format!("Bearer {}", session.access_token)))
    }

    fn update_session(&self, session: StoredAuthSession) -> Result<(), ApiClientError> {
        let state = {
            let mut guard = self.auth.lock().map_err(|err| ApiClientError::Transport {
                endpoint: "auth/session".to_string(),
                message: format!("failed to lock auth session: {err}"),
            })?;
            if let Some(existing) = guard.as_mut() {
                existing.session = session.clone();
                Some(existing.clone())
            } else {
                None
            }
        };

        if let Some(state) = state {
            if state.persist_session {
                save_token_file(&state.token_path, &state.session).map_err(|err| {
                    ApiClientError::Transport {
                        endpoint: "auth/session".to_string(),
                        message: format!("failed to persist refreshed session: {err}"),
                    }
                })?;
            }
        }

        Ok(())
    }

    fn build_request(
        &self,
        method: Method,
        endpoint: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
        cli_auth: bool,
    ) -> Result<reqwest::Request, ApiClientError> {
        let mut url = self.endpoint_url(endpoint)?;
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in query {
                if !value.is_empty() {
                    pairs.append_pair(key, value);
                }
            }
        }

        let mut request = self.request_builder.request(method, url);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        if let Some(auth_header) = self.current_auth_header()? {
            request = request.header("authorization", auth_header);
        }
        if cli_auth {
            request = request.header(CLI_AUTH_MODE_HEADER, CLI_AUTH_MODE_VALUE);
        }
        if let Some(payload) = body {
            request = request.json(payload);
        }

        request.build().map_err(|err| ApiClientError::Transport {
            endpoint: endpoint.to_string(),
            message: err.to_string(),
        })
    }

    async fn execute_json_request(
        &self,
        method: Method,
        endpoint: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
        cli_auth: bool,
    ) -> Result<(StatusCode, Vec<u8>), ApiClientError> {
        let request = self.build_request(method, endpoint, query, body, cli_auth)?;
        self.http
            .execute(request)
            .await
            .map_err(|err| ApiClientError::Transport {
                endpoint: endpoint.to_string(),
                message: err,
            })
    }

    fn http_error(endpoint: &str, status: StatusCode, bytes: &[u8]) -> ApiClientError {
        let safe_body = String::from_utf8_lossy(bytes).trim().to_string();
        let message = if safe_body.is_empty() {
            format!("request failed with status {status}")
        } else {
            safe_body.clone()
        };
        ApiClientError::Http {
            endpoint: endpoint.to_string(),
            status,
            message,
            body: if safe_body.is_empty() {
                None
            } else {
                Some(safe_body)
            },
        }
    }

    fn should_auto_refresh(&self, endpoint: &str) -> bool {
        endpoint != "/auth/login" && endpoint != "/auth/refresh"
    }

    fn stored_session_from_auth(
        &self,
        user: &aegis_core::User,
        access_token: String,
        refresh_token: Option<String>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<StoredAuthSession, ApiClientError> {
        let refresh_token = refresh_token.ok_or_else(|| ApiClientError::Transport {
            endpoint: "auth/refresh".to_string(),
            message: "server did not return a refresh token for CLI auth".to_string(),
        })?;

        Ok(StoredAuthSession {
            access_token,
            refresh_token: Some(refresh_token),
            expires_at,
            user: Some(StoredUserSummary::from(user)),
            saved_at: Utc::now(),
        })
    }

    async fn refresh_session(&self) -> Result<AuthRefreshResponse, ApiClientError> {
        let refresh_token = self
            .current_session()?
            .and_then(|session| session.refresh_token.clone())
            .ok_or_else(|| ApiClientError::LoginRequired {
                message: "login required: run `aegis auth login` to create a refreshable session"
                    .to_string(),
            })?;
        let payload = build_auth_refresh_request(&refresh_token);
        let payload = serde_json::to_value(&payload).map_err(|err| ApiClientError::Transport {
            endpoint: "/auth/refresh".to_string(),
            message: format!("failed to encode refresh request: {err}"),
        })?;
        let (status, bytes) = self
            .execute_json_request(Method::POST, "/auth/refresh", &[], Some(&payload), true)
            .await?;
        if !status.is_success() {
            return Err(ApiClientError::LoginRequired {
                message: "login required: stored session could not be refreshed; run `aegis auth login` again".to_string(),
            });
        }
        let refreshed: AuthRefreshResponse =
            serde_json::from_slice(&bytes).map_err(|err| ApiClientError::Transport {
                endpoint: "/auth/refresh".to_string(),
                message: format!("failed to parse JSON response: {err}"),
            })?;
        let session = self.stored_session_from_auth(
            &refreshed.user,
            refreshed.access_token.clone(),
            refreshed.refresh_token.clone(),
            Some(refreshed.expires_at),
        )?;
        self.update_session(session)?;
        Ok(refreshed)
    }

    async fn try_auto_refresh(&self) -> Result<(), ApiClientError> {
        self.refresh_session().await.map(|_| ())
    }

    async fn request<T, B>(
        &self,
        method: Method,
        endpoint: &str,
        query: &[(&str, String)],
        body: Option<&B>,
    ) -> Result<T, ApiClientError>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize + ?Sized,
    {
        let body = body.map(serde_json::to_value).transpose().map_err(|err| {
            ApiClientError::Transport {
                endpoint: endpoint.to_string(),
                message: format!("failed to encode request body: {err}"),
            }
        })?;
        let (status, bytes) = self
            .execute_json_request(method.clone(), endpoint, query, body.as_ref(), false)
            .await?;
        if status == StatusCode::UNAUTHORIZED && self.should_auto_refresh(endpoint) {
            self.try_auto_refresh().await?;
            let (status, bytes) = self
                .execute_json_request(method, endpoint, query, body.as_ref(), false)
                .await?;
            if !status.is_success() {
                return Err(Self::http_error(endpoint, status, &bytes));
            }
            return serde_json::from_slice(&bytes).map_err(|err| ApiClientError::Transport {
                endpoint: endpoint.to_string(),
                message: format!("failed to parse JSON response: {err}"),
            });
        }
        if !status.is_success() {
            return Err(Self::http_error(endpoint, status, &bytes));
        }

        serde_json::from_slice(&bytes).map_err(|err| ApiClientError::Transport {
            endpoint: endpoint.to_string(),
            message: format!("failed to parse JSON response: {err}"),
        })
    }

    async fn request_text(
        &self,
        method: Method,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<String, ApiClientError> {
        let (status, bytes) = self
            .execute_json_request(method.clone(), endpoint, query, None, false)
            .await?;
        if status == StatusCode::UNAUTHORIZED && self.should_auto_refresh(endpoint) {
            self.try_auto_refresh().await?;
            let (status, bytes) = self
                .execute_json_request(method, endpoint, query, None, false)
                .await?;
            if !status.is_success() {
                return Err(Self::http_error(endpoint, status, &bytes));
            }
            return Ok(String::from_utf8_lossy(&bytes).into_owned());
        }
        if !status.is_success() {
            return Err(Self::http_error(endpoint, status, &bytes));
        }

        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    pub async fn system_health(&self) -> Result<HealthResponse, ApiClientError> {
        self.get("/system/health", &[]).await
    }

    pub async fn auth_login(
        &self,
        email: &str,
        password: &str,
    ) -> Result<AuthLoginResponse, ApiClientError> {
        let payload = serde_json::to_value(AuthLoginRequest {
            email: email.to_string(),
            password: password.to_string(),
        })
        .map_err(|err| ApiClientError::Transport {
            endpoint: "/auth/login".to_string(),
            message: format!("failed to encode login request: {err}"),
        })?;
        let (status, bytes) = self
            .execute_json_request(Method::POST, "/auth/login", &[], Some(&payload), true)
            .await?;
        if !status.is_success() {
            return Err(Self::http_error("/auth/login", status, &bytes));
        }
        serde_json::from_slice(&bytes).map_err(|err| ApiClientError::Transport {
            endpoint: "/auth/login".to_string(),
            message: format!("failed to parse JSON response: {err}"),
        })
    }

    pub async fn auth_refresh(&self) -> Result<AuthRefreshResponse, ApiClientError> {
        self.refresh_session().await
    }

    pub async fn auth_me(&self) -> Result<AuthUserResponse, ApiClientError> {
        self.get("/auth/me", &[]).await
    }

    pub async fn auth_logout(&self) -> Result<AuthLogoutResponse, ApiClientError> {
        self.post("/auth/logout", &serde_json::json!({})).await
    }

    pub async fn metrics(&self) -> Result<String, ApiClientError> {
        self.get_text("/metrics", &[]).await
    }

    pub async fn system_status(&self) -> Result<StatusResponse, ApiClientError> {
        self.get("/system/status", &[]).await
    }

    pub async fn risk_status(&self) -> Result<RiskStatusResponse, ApiClientError> {
        self.get("/risk/status", &[]).await
    }

    pub async fn risk_config(&self) -> Result<RiskConfigResponse, ApiClientError> {
        self.get("/risk/config", &[]).await
    }

    pub async fn validate_risk_config(
        &self,
        request: &RiskConfig,
    ) -> Result<RiskConfigValidationResponse, ApiClientError> {
        self.post("/risk/config/validate", request).await
    }

    pub async fn update_risk_config(
        &self,
        request: &RiskConfig,
    ) -> Result<RiskConfigResponse, ApiClientError> {
        self.post("/risk/config/update", request).await
    }

    pub async fn risk_config_versions(&self) -> Result<RiskConfigVersionsResponse, ApiClientError> {
        self.get("/risk/config/versions", &[]).await
    }

    pub async fn risk_config_audit(&self) -> Result<RiskConfigAuditResponse, ApiClientError> {
        self.get("/risk/config/audit", &[]).await
    }

    pub async fn market_feed_status(&self) -> Result<FeedStatusResponse, ApiClientError> {
        self.get("/market/feed-status", &[]).await
    }

    pub async fn backfill_candles(
        &self,
        request: &CandleBackfillRequest,
    ) -> Result<CandleBackfillResult, ApiClientError> {
        self.post("/market/backfill/candles", request).await
    }

    pub async fn list_backfill_runs(
        &self,
        limit: i64,
    ) -> Result<CandleBackfillRunsResponse, ApiClientError> {
        self.get("/market/backfill/runs", &[("limit", limit.to_string())])
            .await
    }

    pub async fn get_backfill_run(
        &self,
        run_id: Uuid,
    ) -> Result<CandleBackfillRunResponse, ApiClientError> {
        self.get(&format!("/market/backfill/runs/{run_id}"), &[])
            .await
    }

    pub async fn aggregate_candles(
        &self,
        request: &CandleAggregationRequest,
    ) -> Result<CandleAggregationResult, ApiClientError> {
        self.post("/market/candles/aggregate", request).await
    }

    pub async fn candle_coverage(
        &self,
        symbol: &str,
    ) -> Result<CandleCoverageResponse, ApiClientError> {
        self.get(
            "/market/candles/coverage",
            &[("symbol", symbol.to_string())],
        )
        .await
    }

    pub async fn get_research_data_coverage(
        &self,
        query: &ResearchDataCoverageQuery,
    ) -> Result<ResearchDataCoverageResponse, ApiClientError> {
        let mut params = vec![
            ("symbol", query.symbol.clone()),
            ("intervals", query.intervals.clone()),
            ("start_time", query.start_time.to_rfc3339()),
            ("end_time", query.end_time.to_rfc3339()),
        ];
        if let Some(exchange) = &query.exchange {
            params.push(("exchange", exchange.clone()));
        }
        if let Some(required_coverage_pct) = query.required_coverage_pct {
            params.push((
                "required_coverage_pct",
                required_coverage_pct.normalize().to_string(),
            ));
        }
        self.get("/research/data/coverage", &params).await
    }

    pub async fn build_research_dataset(
        &self,
        request: &ResearchDatasetBuildRequest,
    ) -> Result<ResearchDatasetBuildResponse, ApiClientError> {
        self.post("/research/data/build", request).await
    }

    pub async fn list_research_dataset_builds(
        &self,
        limit: i64,
    ) -> Result<ResearchDatasetBuildsResponse, ApiClientError> {
        self.get("/research/data/builds", &[("limit", limit.to_string())])
            .await
    }

    pub async fn get_research_dataset_build(
        &self,
        build_id: Uuid,
    ) -> Result<ResearchDatasetBuildResponse, ApiClientError> {
        self.get(&format!("/research/data/builds/{build_id}"), &[])
            .await
    }

    pub async fn list_research_candidates(
        &self,
        query: &ResearchCandidatesQuery,
    ) -> Result<ResearchCandidatesResponse, ApiClientError> {
        self.get("/research/candidates", &query.to_query_params())
            .await
    }

    pub async fn get_research_candidate(
        &self,
        candidate_id: Uuid,
    ) -> Result<ResearchCandidateResponse, ApiClientError> {
        self.get(&format!("/research/candidates/{candidate_id}"), &[])
            .await
    }

    pub async fn create_research_candidate(
        &self,
        request: &CreateResearchCandidateRequest,
    ) -> Result<ResearchCandidateResponse, ApiClientError> {
        self.post("/research/candidates", request).await
    }

    pub async fn create_research_candidate_from_experiment_run(
        &self,
        request: &CreateResearchCandidateFromExperimentRunRequest,
    ) -> Result<ResearchCandidateResponse, ApiClientError> {
        self.post("/research/candidates/from-experiment-run", request)
            .await
    }

    pub async fn observe_research_candidate(
        &self,
        candidate_id: Uuid,
    ) -> Result<ResearchCandidateObservationResponse, ApiClientError> {
        self.post(
            &format!("/research/candidates/{candidate_id}/observe"),
            &serde_json::json!({}),
        )
        .await
    }

    pub async fn list_research_candidate_events(
        &self,
        candidate_id: Uuid,
    ) -> Result<ResearchCandidateEventsResponse, ApiClientError> {
        self.get(&format!("/research/candidates/{candidate_id}/events"), &[])
            .await
    }

    pub async fn list_research_candidate_observations(
        &self,
        candidate_id: Uuid,
    ) -> Result<ResearchCandidateObservationsResponse, ApiClientError> {
        self.get(
            &format!("/research/candidates/{candidate_id}/observations"),
            &[],
        )
        .await
    }

    pub async fn get_research_candidate_observation_summary(
        &self,
        candidate_id: Uuid,
    ) -> Result<ResearchCandidateObservationSummaryResponse, ApiClientError> {
        self.get(
            &format!("/research/candidates/{candidate_id}/observation-summary"),
            &[],
        )
        .await
    }

    pub async fn get_research_candidate_qualification(
        &self,
        candidate_id: Uuid,
        thresholds: &ResearchCandidateQualificationThresholds,
    ) -> Result<ResearchCandidateQualificationResponse, ApiClientError> {
        self.get(
            &format!("/research/candidates/{candidate_id}/qualification"),
            &[
                ("min_shadow_runs", thresholds.min_shadow_runs.to_string()),
                (
                    "min_would_submit_count",
                    thresholds.min_would_submit_count.to_string(),
                ),
                (
                    "max_risk_rejection_rate_pct",
                    thresholds.max_risk_rejection_rate_pct.to_string(),
                ),
                (
                    "max_error_or_skipped_rate_pct",
                    thresholds.max_error_or_skipped_rate_pct.to_string(),
                ),
            ],
        )
        .await
    }

    pub async fn evaluate_research_candidate_qualification(
        &self,
        candidate_id: Uuid,
        thresholds: &ResearchCandidateQualificationThresholds,
    ) -> Result<ResearchCandidateQualificationEvaluateResponse, ApiClientError> {
        self.post(
            &format!("/research/candidates/{candidate_id}/qualification/evaluate"),
            &serde_json::json!({
                "min_shadow_runs": thresholds.min_shadow_runs,
                "min_would_submit_count": thresholds.min_would_submit_count,
                "max_risk_rejection_rate_pct": thresholds.max_risk_rejection_rate_pct,
                "max_error_or_skipped_rate_pct": thresholds.max_error_or_skipped_rate_pct,
            }),
        )
        .await
    }

    pub async fn get_research_candidate_qualification_history(
        &self,
        candidate_id: Uuid,
        limit: i64,
    ) -> Result<ResearchCandidateQualificationHistoryResponse, ApiClientError> {
        self.get(
            &format!("/research/candidates/{candidate_id}/qualification/history"),
            &[("limit", limit.to_string())],
        )
        .await
    }

    pub async fn get_research_candidate_testnet_review_dossier(
        &self,
        candidate_id: Uuid,
    ) -> Result<ResearchCandidateTestnetReviewDossierResponse, ApiClientError> {
        self.get(
            &format!("/research/candidates/{candidate_id}/testnet-review-dossier"),
            &[],
        )
        .await
    }

    pub async fn get_research_candidate_watchlist(
        &self,
        limit: i64,
    ) -> Result<ResearchCandidateWatchlistResponse, ApiClientError> {
        self.get(
            "/research/candidates/watchlist",
            &[("limit", limit.to_string())],
        )
        .await
    }

    pub async fn list_research_candidate_reviews(
        &self,
        candidate_id: Uuid,
    ) -> Result<ResearchCandidateReviewsResponse, ApiClientError> {
        self.get(&format!("/research/candidates/{candidate_id}/reviews"), &[])
            .await
    }

    pub async fn create_research_candidate_review(
        &self,
        candidate_id: Uuid,
        request: &ResearchCandidateReviewRequest,
    ) -> Result<ResearchCandidateReviewResponse, ApiClientError> {
        self.post(
            &format!("/research/candidates/{candidate_id}/reviews"),
            request,
        )
        .await
    }

    pub async fn get_research_candidate_shadow_performance(
        &self,
        candidate_id: Uuid,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<ResearchCandidateShadowPerformanceResponse, ApiClientError> {
        let mut query = Vec::new();
        if let Some(start_time) = start_time {
            query.push(("start_time", start_time.to_rfc3339()));
        }
        if let Some(end_time) = end_time {
            query.push(("end_time", end_time.to_rfc3339()));
        }
        self.get(
            &format!("/research/candidates/{candidate_id}/shadow-performance"),
            &query,
        )
        .await
    }

    pub async fn list_research_candidate_shadow_runs(
        &self,
        candidate_id: Uuid,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<ResearchCandidateShadowRunsResponse, ApiClientError> {
        let mut query = vec![("limit", limit.to_string())];
        if let Some(start_time) = start_time {
            query.push(("start_time", start_time.to_rfc3339()));
        }
        if let Some(end_time) = end_time {
            query.push(("end_time", end_time.to_rfc3339()));
        }
        self.get(
            &format!("/research/candidates/{candidate_id}/shadow-runs"),
            &query,
        )
        .await
    }

    pub async fn decide_research_candidate(
        &self,
        candidate_id: Uuid,
        request: &ResearchCandidateDecisionRequest,
    ) -> Result<ResearchCandidateResponse, ApiClientError> {
        self.post(
            &format!("/research/candidates/{candidate_id}/decision"),
            request,
        )
        .await
    }

    pub async fn preview_research_candidate_shadow_promotion(
        &self,
        candidate_id: Uuid,
        request: &ResearchCandidateShadowPromotionRequest,
    ) -> Result<ResearchCandidateShadowPromotionPreviewResponse, ApiClientError> {
        self.post(
            &format!("/research/candidates/{candidate_id}/promote-shadow/preview"),
            request,
        )
        .await
    }

    pub async fn apply_research_candidate_shadow_promotion(
        &self,
        candidate_id: Uuid,
        request: &ResearchCandidateShadowPromotionRequest,
    ) -> Result<ResearchCandidateShadowPromotionResultResponse, ApiClientError> {
        self.post(
            &format!("/research/candidates/{candidate_id}/promote-shadow/apply"),
            request,
        )
        .await
    }

    pub async fn activate_kill_switch(
        &self,
        reason: Option<String>,
    ) -> Result<RiskActionResponse, ApiClientError> {
        self.post("/risk/kill-switch", &KillSwitchRequest { reason })
            .await
    }

    pub async fn resume_trading(
        &self,
        confirmation_text: &str,
        reason: Option<String>,
    ) -> Result<RiskActionResponse, ApiClientError> {
        self.post(
            "/risk/resume",
            &ResumeRequest {
                confirmation_text: confirmation_text.to_string(),
                reason,
            },
        )
        .await
    }

    pub async fn run_pipeline(
        &self,
        request: &PaperTradingPipelineRequest,
    ) -> Result<PaperTradingPipelineResult, ApiClientError> {
        self.post("/paper/pipeline/run", request).await
    }

    pub async fn list_strategies(&self) -> Result<StrategyListResponse, ApiClientError> {
        self.get("/strategy/list", &[]).await
    }

    pub async fn strategy_config(
        &self,
        strategy_id: &str,
    ) -> Result<StrategyStatusResponse, ApiClientError> {
        self.get(&format!("/strategy/{strategy_id}/config"), &[])
            .await
    }

    pub async fn validate_strategy_config(
        &self,
        strategy_id: &str,
        request: &StrategyConfigUpdateRequest,
    ) -> Result<StrategyConfigValidationResponse, ApiClientError> {
        self.post(&format!("/strategy/{strategy_id}/config/validate"), request)
            .await
    }

    pub async fn update_strategy_config(
        &self,
        strategy_id: &str,
        request: &StrategyConfigUpdateRequest,
    ) -> Result<StrategyStatusResponse, ApiClientError> {
        self.post(&format!("/strategy/{strategy_id}/config/update"), request)
            .await
    }

    pub async fn strategy_config_versions(
        &self,
        strategy_id: &str,
    ) -> Result<StrategyConfigVersionsResponse, ApiClientError> {
        self.get(&format!("/strategy/{strategy_id}/config/versions"), &[])
            .await
    }

    pub async fn strategy_config_audit(
        &self,
        strategy_id: &str,
    ) -> Result<StrategyConfigAuditResponse, ApiClientError> {
        self.get(&format!("/strategy/{strategy_id}/config/audit"), &[])
            .await
    }

    pub async fn strategy_dry_run(
        &self,
        strategy_id: &str,
        symbol: Option<String>,
        timeframe: Option<String>,
    ) -> Result<StrategyDryRunResponse, ApiClientError> {
        self.post(
            &format!("/strategy/{strategy_id}/dry-run"),
            &StrategyDryRunRequest {
                symbol,
                timeframe,
                config_override: None,
                correlation_id: None,
            },
        )
        .await
    }

    pub async fn strategy_diagnostics(
        &self,
        strategy_id: &str,
        symbol: Option<String>,
        timeframe: Option<String>,
        limit: i64,
    ) -> Result<StrategyDiagnosticsResponse, ApiClientError> {
        let mut query = vec![("limit", limit.to_string())];
        if let Some(symbol) = symbol {
            query.push(("symbol", symbol));
        }
        if let Some(timeframe) = timeframe {
            query.push(("timeframe", timeframe));
        }
        self.get(&format!("/strategy/{strategy_id}/diagnostics"), &query)
            .await
    }

    pub async fn enable_strategy(
        &self,
        strategy_id: &str,
    ) -> Result<StrategyStatusResponse, ApiClientError> {
        self.post(&format!("/strategy/{strategy_id}/enable"), &EmptyRequest)
            .await
    }

    pub async fn disable_strategy(
        &self,
        strategy_id: &str,
    ) -> Result<StrategyStatusResponse, ApiClientError> {
        self.post(&format!("/strategy/{strategy_id}/disable"), &EmptyRequest)
            .await
    }

    pub async fn list_orders(&self) -> Result<OrdersResponse, ApiClientError> {
        self.get("/orders", &[]).await
    }

    pub async fn get_order(&self, order_id: Uuid) -> Result<OrderResponse, ApiClientError> {
        self.get(&format!("/orders/{order_id}"), &[]).await
    }

    pub async fn exchange_testnet_status(
        &self,
    ) -> Result<ExchangeTestnetStatusResponse, ApiClientError> {
        self.get("/exchange/testnet/status", &[]).await
    }

    pub async fn exchange_testnet_private_stream_status(
        &self,
    ) -> Result<ExchangePrivateStreamStatusResponse, ApiClientError> {
        self.get("/exchange/testnet/private-stream/status", &[])
            .await
    }

    pub async fn exchange_testnet_private_stream_events(
        &self,
        limit: i64,
        client_order_id: Option<String>,
        event_type: Option<String>,
    ) -> Result<ExchangePrivateStreamEventsResponse, ApiClientError> {
        let mut query = vec![("limit", limit.to_string())];
        if let Some(client_order_id) = client_order_id {
            query.push(("client_order_id", client_order_id));
        }
        if let Some(event_type) = event_type {
            query.push(("event_type", event_type));
        }
        self.get("/exchange/testnet/private-stream/events", &query)
            .await
    }

    pub async fn exchange_testnet_private_stream_listen_key(
        &self,
    ) -> Result<ExchangePrivateStreamListenKeyResponse, ApiClientError> {
        self.post("/exchange/testnet/private-stream/listen-key", &EmptyRequest)
            .await
    }

    pub async fn exchange_testnet_private_stream_keepalive(
        &self,
        listen_key: &str,
    ) -> Result<ExchangePrivateStreamListenKeyResponse, ApiClientError> {
        self.post(
            "/exchange/testnet/private-stream/listen-key/keepalive",
            &ExchangePrivateStreamListenKeyRequest {
                listen_key: Some(listen_key.to_string()),
                correlation_id: None,
            },
        )
        .await
    }

    pub async fn exchange_testnet_private_stream_close(
        &self,
        listen_key: &str,
    ) -> Result<ExchangePrivateStreamListenKeyResponse, ApiClientError> {
        self.post(
            "/exchange/testnet/private-stream/listen-key/close",
            &ExchangePrivateStreamListenKeyRequest {
                listen_key: Some(listen_key.to_string()),
                correlation_id: None,
            },
        )
        .await
    }

    pub async fn exchange_testnet_symbols(
        &self,
    ) -> Result<ExchangeTestnetSymbolsResponse, ApiClientError> {
        self.get("/exchange/testnet/symbols", &[]).await
    }

    pub async fn exchange_testnet_balances(
        &self,
    ) -> Result<ExchangeTestnetBalancesResponse, ApiClientError> {
        self.get("/exchange/testnet/balances", &[]).await
    }

    pub async fn exchange_testnet_orders(
        &self,
        limit: i64,
    ) -> Result<ExchangeTestnetOrdersResponse, ApiClientError> {
        self.get("/exchange/testnet/orders", &[("limit", limit.to_string())])
            .await
    }

    pub async fn exchange_testnet_order_get(
        &self,
        client_order_id: &str,
    ) -> Result<ExchangeTestnetOrderResponse, ApiClientError> {
        self.get(&format!("/exchange/testnet/orders/{client_order_id}"), &[])
            .await
    }

    pub async fn exchange_testnet_order_lifecycle(
        &self,
        client_order_id: &str,
    ) -> Result<ExchangeTestnetOrderLifecycleResponse, ApiClientError> {
        self.get(
            &format!("/exchange/testnet/orders/{client_order_id}/lifecycle"),
            &[],
        )
        .await
    }

    pub async fn exchange_testnet_order_submit(
        &self,
        request: &ExchangeTestnetOrderSubmitRequest,
    ) -> Result<ExchangeTestnetOrderResponse, ApiClientError> {
        self.post("/exchange/testnet/orders", request).await
    }

    pub async fn exchange_testnet_pipeline_preview(
        &self,
        request: &ExchangeTestnetPipelinePreviewRequest,
    ) -> Result<ExchangeTestnetPipelinePreviewResponse, ApiClientError> {
        self.post("/exchange/testnet/pipeline/preview", request)
            .await
    }

    pub async fn exchange_testnet_pipeline_submit(
        &self,
        request: &ExchangeTestnetPipelineSubmitRequest,
    ) -> Result<ExchangeTestnetPipelineSubmitResponse, ApiClientError> {
        self.post("/exchange/testnet/pipeline/submit", request)
            .await
    }

    pub async fn exchange_testnet_shadow_run(
        &self,
        request: &TestnetShadowRunRequest,
    ) -> Result<TestnetShadowRunResponse, ApiClientError> {
        self.post("/exchange/testnet/shadow/run", request).await
    }

    pub async fn exchange_testnet_shadow_runs(
        &self,
        limit: i64,
    ) -> Result<TestnetShadowRunsResponse, ApiClientError> {
        self.get(
            "/exchange/testnet/shadow/runs",
            &[("limit", limit.to_string())],
        )
        .await
    }

    pub async fn exchange_testnet_shadow_get(
        &self,
        run_id: Uuid,
    ) -> Result<TestnetShadowRunResponse, ApiClientError> {
        self.get(&format!("/exchange/testnet/shadow/runs/{run_id}"), &[])
            .await
    }

    pub async fn exchange_testnet_shadow_promotion_preview(
        &self,
        request: &TestnetShadowPromotionRequest,
    ) -> Result<TestnetShadowPromotionResponse, ApiClientError> {
        self.post("/exchange/testnet/shadow/promotions/preview", request)
            .await
    }

    pub async fn exchange_testnet_shadow_promotions(
        &self,
        limit: i64,
    ) -> Result<TestnetShadowPromotionsResponse, ApiClientError> {
        self.get(
            "/exchange/testnet/shadow/promotions",
            &[("limit", limit.to_string())],
        )
        .await
    }

    pub async fn exchange_testnet_shadow_promotion_get(
        &self,
        promotion_id: Uuid,
    ) -> Result<TestnetShadowPromotionResponse, ApiClientError> {
        self.get(
            &format!("/exchange/testnet/shadow/promotions/{promotion_id}"),
            &[],
        )
        .await
    }

    pub async fn exchange_testnet_shadow_promotion_submit(
        &self,
        promotion_id: Uuid,
        request: &TestnetShadowPromotionSubmitRequest,
    ) -> Result<TestnetShadowPromotionSubmitResponse, ApiClientError> {
        self.post(
            &format!("/exchange/testnet/shadow/promotions/{promotion_id}/submit"),
            request,
        )
        .await
    }

    pub async fn exchange_testnet_shadow_runner_status(
        &self,
    ) -> Result<TestnetShadowRunnerStatusResponse, ApiClientError> {
        self.get("/exchange/testnet/shadow-runner/status", &[])
            .await
    }

    pub async fn exchange_testnet_shadow_runner_config(
        &self,
    ) -> Result<TestnetShadowRunnerConfigResponse, ApiClientError> {
        self.get("/exchange/testnet/shadow-runner/config", &[])
            .await
    }

    pub async fn exchange_testnet_shadow_runner_config_validate(
        &self,
        request: &TestnetShadowRunnerConfigInput,
    ) -> Result<TestnetShadowRunnerConfigValidationResponse, ApiClientError> {
        self.post("/exchange/testnet/shadow-runner/config/validate", request)
            .await
    }

    pub async fn exchange_testnet_shadow_runner_config_update(
        &self,
        request: &TestnetShadowRunnerConfigInput,
    ) -> Result<TestnetShadowRunnerConfigResponse, ApiClientError> {
        self.post("/exchange/testnet/shadow-runner/config/update", request)
            .await
    }

    pub async fn exchange_testnet_shadow_runner_control(
        &self,
        request: &TestnetShadowRunnerControlRequest,
    ) -> Result<TestnetShadowRunnerControlResponse, ApiClientError> {
        self.post("/exchange/testnet/shadow-runner/control", request)
            .await
    }

    pub async fn exchange_testnet_order_cancel(
        &self,
        client_order_id: &str,
        confirmation_text: &str,
    ) -> Result<ExchangeTestnetOrderResponse, ApiClientError> {
        self.post(
            &format!("/exchange/testnet/orders/{client_order_id}/cancel"),
            &ExchangeTestnetOrderCancelRequest {
                confirmation_text: confirmation_text.to_string(),
                correlation_id: None,
            },
        )
        .await
    }

    pub async fn exchange_testnet_order_repair(
        &self,
        client_order_id: &str,
        request: &ExchangeTestnetOrderRepairRequest,
    ) -> Result<ExchangeTestnetRepairResponse, ApiClientError> {
        self.post(
            &format!("/exchange/testnet/orders/{client_order_id}/repair"),
            request,
        )
        .await
    }

    pub async fn exchange_testnet_order_repairs(
        &self,
        client_order_id: &str,
    ) -> Result<ExchangeTestnetRepairsResponse, ApiClientError> {
        self.get(
            &format!("/exchange/testnet/orders/{client_order_id}/repairs"),
            &[],
        )
        .await
    }

    pub async fn exchange_testnet_reconcile(
        &self,
        request: &ExchangeTestnetReconcileRequest,
    ) -> Result<ExchangeReconciliationResultResponse, ApiClientError> {
        self.post("/exchange/testnet/reconcile", request).await
    }

    pub async fn exchange_reconciliation_runs(
        &self,
        limit: i64,
    ) -> Result<ExchangeReconciliationRunsResponse, ApiClientError> {
        self.get(
            "/exchange/testnet/reconciliation/runs",
            &[("limit", limit.to_string())],
        )
        .await
    }

    pub async fn exchange_reconciliation_run(
        &self,
        run_id: Uuid,
    ) -> Result<ExchangeReconciliationRunResponse, ApiClientError> {
        self.get(
            &format!("/exchange/testnet/reconciliation/runs/{run_id}"),
            &[],
        )
        .await
    }

    pub async fn exchange_reconciliation_mismatches(
        &self,
        run_id: Uuid,
    ) -> Result<ExchangeReconciliationMismatchesResponse, ApiClientError> {
        self.get(
            &format!("/exchange/testnet/reconciliation/runs/{run_id}/mismatches"),
            &[],
        )
        .await
    }

    pub async fn paper_account(&self) -> Result<PaperAccountResponse, ApiClientError> {
        self.get("/paper/account", &[]).await
    }

    pub async fn paper_positions(
        &self,
        limit: i64,
        status: &str,
    ) -> Result<PaperPositionsResponse, ApiClientError> {
        self.get(
            "/paper/positions",
            &[("limit", limit.to_string()), ("status", status.to_string())],
        )
        .await
    }

    pub async fn paper_close(
        &self,
        position_id: Uuid,
        confirmation_text: &str,
        reason: Option<String>,
    ) -> Result<PaperClosePositionResponse, ApiClientError> {
        self.post(
            &format!("/paper/positions/{position_id}/close"),
            &PaperClosePositionRequest {
                confirmation_text: confirmation_text.to_string(),
                reason,
                close_mode: "MARKET_SIMULATED".to_string(),
                correlation_id: None,
            },
        )
        .await
    }

    pub async fn paper_pnl(&self) -> Result<PaperPnlResponse, ApiClientError> {
        self.get("/paper/pnl/daily", &[]).await
    }

    pub async fn paper_equity(&self, limit: i64) -> Result<PaperEquityResponse, ApiClientError> {
        self.get("/paper/equity", &[("limit", limit.to_string())])
            .await
    }

    pub async fn paper_journal(
        &self,
        limit: i64,
    ) -> Result<PaperTradeJournalResponse, ApiClientError> {
        self.get("/paper/trade-journal", &[("limit", limit.to_string())])
            .await
    }

    pub async fn paper_mark(&self) -> Result<PaperPnlResponse, ApiClientError> {
        self.post("/paper/account/mark-to-market", &EmptyRequest)
            .await
    }

    pub async fn recent_events(
        &self,
        query: &RecentEventsQuery,
    ) -> Result<RecentEventsResponse, ApiClientError> {
        let params = query.to_query_params();
        self.get("/events/recent", &params).await
    }

    pub async fn risk_decisions(
        &self,
        query: &RiskDecisionsQuery,
    ) -> Result<RiskDecisionsResponse, ApiClientError> {
        self.get("/risk/decisions", &query.to_query_params()).await
    }

    pub async fn run_backtest(
        &self,
        request: &BacktestRequest,
    ) -> Result<BacktestRunAcceptedResponse, ApiClientError> {
        self.post("/backtest/run", request).await
    }

    pub async fn backtest_runs(&self, limit: i64) -> Result<BacktestRunsResponse, ApiClientError> {
        self.get("/backtest/runs", &[("limit", limit.to_string())])
            .await
    }

    pub async fn backtest_run(&self, run_id: Uuid) -> Result<BacktestRunResponse, ApiClientError> {
        self.get(&format!("/backtest/runs/{run_id}"), &[]).await
    }

    pub async fn run_strategy_experiment(
        &self,
        request: &StrategyExperimentRequest,
    ) -> Result<StrategyExperimentRunAcceptedResponse, ApiClientError> {
        self.post("/experiments/strategy/run", request).await
    }

    pub async fn run_multi_timeframe_strategy_experiment(
        &self,
        request: &StrategyMultiTimeframeExperimentRequest,
    ) -> Result<StrategyMultiTimeframeExperimentAcceptedResponse, ApiClientError> {
        self.post("/experiments/strategy/multi-timeframe", request)
            .await
    }

    pub async fn run_strategy_walk_forward(
        &self,
        request: &StrategyWalkForwardRequest,
    ) -> Result<StrategyWalkForwardAcceptedResponse, ApiClientError> {
        self.post("/experiments/strategy/walk-forward", request)
            .await
    }

    pub async fn strategy_experiments(
        &self,
        limit: i64,
    ) -> Result<StrategyExperimentsResponse, ApiClientError> {
        self.get("/experiments/strategy", &[("limit", limit.to_string())])
            .await
    }

    pub async fn strategy_experiment(
        &self,
        experiment_id: Uuid,
    ) -> Result<StrategyExperimentResponse, ApiClientError> {
        self.get(&format!("/experiments/strategy/{experiment_id}"), &[])
            .await
    }

    pub async fn strategy_experiment_runs(
        &self,
        experiment_id: Uuid,
    ) -> Result<StrategyExperimentRunsResponse, ApiClientError> {
        self.get(&format!("/experiments/strategy/{experiment_id}/runs"), &[])
            .await
    }

    pub async fn strategy_multi_timeframe_comparison(
        &self,
        experiment_group_id: Uuid,
    ) -> Result<StrategyMultiTimeframeExperimentResponse, ApiClientError> {
        self.get(
            &format!("/experiments/strategy/{experiment_group_id}/comparison"),
            &[],
        )
        .await
    }

    pub async fn strategy_walk_forward_runs(
        &self,
        limit: i64,
    ) -> Result<StrategyWalkForwardRunsResponse, ApiClientError> {
        self.get(
            "/experiments/strategy/walk-forward",
            &[("limit", limit.to_string())],
        )
        .await
    }

    pub async fn strategy_walk_forward_run(
        &self,
        walk_forward_id: Uuid,
    ) -> Result<StrategyWalkForwardResponse, ApiClientError> {
        self.get(
            &format!("/experiments/strategy/walk-forward/{walk_forward_id}"),
            &[],
        )
        .await
    }

    pub async fn strategy_walk_forward_windows(
        &self,
        walk_forward_id: Uuid,
    ) -> Result<StrategyWalkForwardWindowsResponse, ApiClientError> {
        self.get(
            &format!("/experiments/strategy/walk-forward/{walk_forward_id}/windows"),
            &[],
        )
        .await
    }

    pub async fn strategy_performance(
        &self,
        strategy_id: Option<String>,
        symbol: Option<String>,
        timeframe: Option<String>,
        mode: String,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> Result<StrategyPerformanceSummaryResponse, ApiClientError> {
        let mut query = vec![("mode", mode)];
        if let Some(strategy_id) = strategy_id.filter(|value| !value.is_empty()) {
            query.push(("strategy_id", strategy_id));
        }
        if let Some(symbol) = symbol.filter(|value| !value.is_empty()) {
            query.push(("symbol", symbol));
        }
        if let Some(timeframe) = timeframe.filter(|value| !value.is_empty()) {
            query.push(("timeframe", timeframe));
        }
        if let Some(start_time) = start_time {
            query.push(("start_time", start_time.to_rfc3339()));
        }
        if let Some(end_time) = end_time {
            query.push(("end_time", end_time.to_rfc3339()));
        }
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get("/analytics/strategy/performance", &query).await
    }

    pub async fn strategy_rankings(
        &self,
        mode: String,
        symbol: Option<String>,
        timeframe: Option<String>,
        limit: i64,
    ) -> Result<StrategyPerformanceRankingsResponse, ApiClientError> {
        let mut query = vec![("mode", mode), ("limit", limit.to_string())];
        if let Some(symbol) = symbol.filter(|value| !value.is_empty()) {
            query.push(("symbol", symbol));
        }
        if let Some(timeframe) = timeframe.filter(|value| !value.is_empty()) {
            query.push(("timeframe", timeframe));
        }
        self.get("/analytics/strategy/rankings", &query).await
    }

    pub async fn strategy_decision_breakdown(
        &self,
        strategy_id: &str,
        symbol: Option<String>,
        timeframe: Option<String>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<StrategyDecisionBreakdownResponse, ApiClientError> {
        let mut query = Vec::new();
        if let Some(symbol) = symbol.filter(|value| !value.is_empty()) {
            query.push(("symbol", symbol));
        }
        if let Some(timeframe) = timeframe.filter(|value| !value.is_empty()) {
            query.push(("timeframe", timeframe));
        }
        if let Some(start_time) = start_time {
            query.push(("start_time", start_time.to_rfc3339()));
        }
        if let Some(end_time) = end_time {
            query.push(("end_time", end_time.to_rfc3339()));
        }
        self.get(
            &format!("/analytics/strategy/{strategy_id}/decision-breakdown"),
            &query,
        )
        .await
    }

    pub async fn testnet_promotion_funnel(
        &self,
        strategy_id: Option<String>,
        symbol: Option<String>,
        timeframe: Option<String>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<TestnetPromotionFunnelSummaryResponse, ApiClientError> {
        let mut query = Vec::new();
        if let Some(strategy_id) = strategy_id.filter(|value| !value.is_empty()) {
            query.push(("strategy_id", strategy_id));
        }
        if let Some(symbol) = symbol.filter(|value| !value.is_empty()) {
            query.push(("symbol", symbol));
        }
        if let Some(timeframe) = timeframe.filter(|value| !value.is_empty()) {
            query.push(("timeframe", timeframe));
        }
        if let Some(start_time) = start_time {
            query.push(("start_time", start_time.to_rfc3339()));
        }
        if let Some(end_time) = end_time {
            query.push(("end_time", end_time.to_rfc3339()));
        }
        self.get("/analytics/testnet/promotion-funnel", &query)
            .await
    }

    pub async fn testnet_promotion_outcomes(
        &self,
        strategy_id: Option<String>,
        symbol: Option<String>,
        timeframe: Option<String>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<TestnetPromotionFunnelOutcomesResponse, ApiClientError> {
        let mut query = Vec::new();
        if let Some(strategy_id) = strategy_id.filter(|value| !value.is_empty()) {
            query.push(("strategy_id", strategy_id));
        }
        if let Some(symbol) = symbol.filter(|value| !value.is_empty()) {
            query.push(("symbol", symbol));
        }
        if let Some(timeframe) = timeframe.filter(|value| !value.is_empty()) {
            query.push(("timeframe", timeframe));
        }
        if let Some(start_time) = start_time {
            query.push(("start_time", start_time.to_rfc3339()));
        }
        if let Some(end_time) = end_time {
            query.push(("end_time", end_time.to_rfc3339()));
        }
        self.get("/analytics/testnet/promotion-funnel/outcomes", &query)
            .await
    }

    pub async fn testnet_promotion_rows(
        &self,
        strategy_id: Option<String>,
        symbol: Option<String>,
        timeframe: Option<String>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> Result<TestnetPromotionFunnelRowsResponse, ApiClientError> {
        let mut query = Vec::new();
        if let Some(strategy_id) = strategy_id.filter(|value| !value.is_empty()) {
            query.push(("strategy_id", strategy_id));
        }
        if let Some(symbol) = symbol.filter(|value| !value.is_empty()) {
            query.push(("symbol", symbol));
        }
        if let Some(timeframe) = timeframe.filter(|value| !value.is_empty()) {
            query.push(("timeframe", timeframe));
        }
        if let Some(start_time) = start_time {
            query.push(("start_time", start_time.to_rfc3339()));
        }
        if let Some(end_time) = end_time {
            query.push(("end_time", end_time.to_rfc3339()));
        }
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        self.get("/analytics/testnet/promotion-funnel/rows", &query)
            .await
    }

    pub async fn generate_operator_report(
        &self,
        request: &OperatorReportRequest,
    ) -> Result<OperatorReportResponse, ApiClientError> {
        self.post("/reports/operator/daily", request).await
    }

    pub async fn list_operator_reports(
        &self,
        limit: i64,
    ) -> Result<OperatorReportsListResponse, ApiClientError> {
        self.get("/reports/operator", &[("limit", limit.to_string())])
            .await
    }

    pub async fn get_operator_report(
        &self,
        report_id: Uuid,
    ) -> Result<OperatorReportResponse, ApiClientError> {
        self.get(&format!("/reports/operator/{report_id}"), &[])
            .await
    }

    pub async fn execution_readiness_check(
        &self,
        request: &ExecutionReadinessRequest,
    ) -> Result<ExecutionReadinessResponse, ApiClientError> {
        self.post("/readiness/check", request).await
    }

    pub async fn execution_readiness_snapshots(
        &self,
        limit: i64,
    ) -> Result<ExecutionReadinessSnapshotsResponse, ApiClientError> {
        self.get("/readiness/snapshots", &[("limit", limit.to_string())])
            .await
    }

    pub async fn execution_readiness_get(
        &self,
        readiness_id: Uuid,
    ) -> Result<ExecutionReadinessResponse, ApiClientError> {
        let response = self
            .get::<ExecutionReadinessSnapshotsResponse>(
                &format!("/readiness/snapshots/{readiness_id}"),
                &[],
            )
            .await?;
        let readiness =
            response
                .snapshots
                .into_iter()
                .next()
                .ok_or_else(|| ApiClientError::Decode {
                    endpoint: format!("/readiness/snapshots/{readiness_id}"),
                    message: "snapshot response did not include a readiness item".to_string(),
                })?;

        Ok(ExecutionReadinessResponse {
            readiness: snapshot_to_result(readiness),
            request_id: response.request_id,
            correlation_id: response.correlation_id,
            timestamp: response.timestamp,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecentEventsQuery {
    pub limit: i64,
    pub event_type: Option<String>,
    pub source: Option<String>,
    pub correlation_id: Option<Uuid>,
}

impl RecentEventsQuery {
    pub fn to_query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![("limit", self.limit.to_string())];
        if let Some(event_type) = self.event_type.as_deref().filter(|value| !value.is_empty()) {
            params.push(("event_type", event_type.to_string()));
        }
        if let Some(source) = self.source.as_deref().filter(|value| !value.is_empty()) {
            params.push(("source", source.to_string()));
        }
        if let Some(correlation_id) = self.correlation_id {
            params.push(("correlation_id", correlation_id.to_string()));
        }
        params
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RiskDecisionsQuery {
    pub limit: i64,
    pub symbol: Option<String>,
}

impl RiskDecisionsQuery {
    pub fn to_query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![("limit", self.limit.to_string())];
        if let Some(symbol) = self.symbol.as_deref().filter(|value| !value.is_empty()) {
            params.push(("symbol", symbol.to_string()));
        }
        params
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchDataCoverageQuery {
    pub exchange: Option<String>,
    pub symbol: String,
    pub intervals: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub required_coverage_pct: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateResearchCandidateRequest {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub config: Value,
    pub notes: Option<String>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateResearchCandidateFromExperimentRunRequest {
    pub experiment_run_id: Uuid,
    pub notes: Option<String>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Default)]
pub struct ResearchCandidatesQuery {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub status: Option<String>,
    pub limit: i64,
}

impl ResearchCandidatesQuery {
    pub fn to_query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![("limit", self.limit.to_string())];
        if let Some(strategy_id) = &self.strategy_id {
            params.push(("strategy_id", strategy_id.clone()));
        }
        if let Some(symbol) = &self.symbol {
            params.push(("symbol", symbol.clone()));
        }
        if let Some(timeframe) = &self.timeframe {
            params.push(("timeframe", timeframe.clone()));
        }
        if let Some(status) = &self.status {
            params.push(("status", status.clone()));
        }
        params
    }
}

#[derive(Debug, Serialize)]
struct KillSwitchRequest {
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct ResumeRequest {
    confirmation_text: String,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ExchangeTestnetOrderSubmitRequest {
    pub symbol: String,
    pub side: String,
    #[serde(rename = "order_type")]
    pub order_type: String,
    pub time_in_force: Option<String>,
    pub quantity: Option<String>,
    pub quote_notional: Option<String>,
    pub limit_price: Option<String>,
    pub risk_decision_id: Option<Uuid>,
    pub confirmation_text: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct ExchangePrivateStreamListenKeyRequest {
    listen_key: Option<String>,
    correlation_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct ExchangeTestnetReconcileRequest {
    pub limit: Option<i64>,
    pub status_filter: Option<Vec<String>>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct ExchangeTestnetOrderCancelRequest {
    confirmation_text: String,
    correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthRefreshRequestPayload {
    pub refresh_token: String,
}

#[derive(Debug, Serialize)]
struct PaperClosePositionRequest {
    confirmation_text: String,
    reason: Option<String>,
    close_mode: String,
    correlation_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct EmptyRequest;

#[derive(Debug, Deserialize, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub environment: String,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StatusResponse {
    pub service: String,
    pub environment: String,
    pub market_mode: String,
    pub started_at: DateTime<Utc>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
    pub dependencies: Dependencies,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Dependencies {
    pub database: DependencyStatus,
    pub event_bus: DependencyStatus,
    pub exchange_execution: DependencyStatus,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DependencyStatus {
    pub status: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemStateSnapshot {
    pub enabled: bool,
    pub reason: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: ActorResponse,
    pub last_correlation_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ActorResponse {
    pub actor: String,
    pub actor_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskStatusResponse {
    pub status: String,
    pub market_mode: String,
    pub paper_trading_allowed: bool,
    pub live_trading_allowed: bool,
    pub resume_confirmation_required: String,
    pub kill_switch: SystemStateSnapshot,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskActionResponse {
    pub status: String,
    pub message: String,
    pub market_mode: String,
    pub paper_trading_allowed: bool,
    pub live_trading_allowed: bool,
    pub kill_switch: SystemStateSnapshot,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskConfigView {
    pub config_id: Uuid,
    pub max_open_positions: i32,
    pub max_daily_loss_pct: String,
    pub max_weekly_loss_pct: String,
    pub max_position_notional: String,
    pub max_slippage_pct: String,
    pub max_consecutive_losses: i32,
    pub cooldown_seconds: i32,
    pub max_signal_age_ms: i64,
    pub stale_feed_threshold_seconds: i32,
    pub config_version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskConfigResponse {
    pub config: RiskConfigView,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskConfigValidationResponse {
    pub validation: RiskConfigValidationResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskConfigVersionsResponse {
    pub versions: Vec<RiskConfigVersion>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskConfigAuditResponse {
    pub audit: Vec<RiskConfigAuditEntry>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MarketFeedStatusRecord {
    pub exchange: String,
    pub symbol: String,
    pub status: String,
    pub freshness_status: String,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub reconnect_count: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FeedStatusResponse {
    pub feeds: Vec<MarketFeedStatusRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CandleBackfillRunsResponse {
    pub runs: Vec<CandleBackfillResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CandleBackfillRunResponse {
    pub run: CandleBackfillResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CandleCoverageResponse {
    pub coverage: MarketCandleCoverageSummary,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResearchDataCoverageResponse {
    pub coverage: ResearchDataCoverageResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResearchDatasetBuildsResponse {
    pub builds: Vec<ResearchDatasetBuildResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResearchDatasetBuildResponse {
    pub build: ResearchDatasetBuildResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidatesResponse {
    pub candidates: Vec<ResearchCandidate>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateResponse {
    pub candidate: ResearchCandidate,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateEventsResponse {
    pub events: Vec<ResearchCandidateLifecycleEvent>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateReviewsResponse {
    pub reviews: Vec<ResearchCandidateReview>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateReviewResponse {
    pub result: ResearchCandidateReviewResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateObservationResponse {
    pub observation: StrategyCandidateObservationResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateObservationsResponse {
    pub observations: Vec<StrategyCandidateObservationResult>,
    pub history: Vec<ResearchCandidateObservationHistoryItem>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateObservationSummaryResponse {
    pub summary: ResearchCandidateObservationSummaryView,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateQualificationResponse {
    pub qualification: ResearchCandidateQualificationResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateQualificationEvaluateResponse {
    pub evaluation: ResearchCandidateQualificationEvaluation,
    pub change: Option<ResearchCandidateQualificationChange>,
    pub trend: ResearchCandidateQualificationTrend,
    pub qualification: ResearchCandidateQualificationResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateQualificationHistoryResponse {
    pub history: ResearchCandidateQualificationHistory,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateTestnetReviewDossierResponse {
    pub dossier: ResearchCandidateTestnetReviewDossier,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateWatchlistResponse {
    pub watchlist: Vec<ResearchCandidateWatchlistEntry>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateShadowPerformanceResponse {
    pub performance: ResearchCandidateShadowPerformance,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateShadowRunsResponse {
    pub runs: Vec<ResearchCandidateShadowRunLink>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateShadowPromotionPreviewResponse {
    pub preview: ResearchCandidateShadowPromotionPreview,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateShadowPromotionResultResponse {
    pub result: ResearchCandidateShadowPromotionResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyStatusView {
    pub strategy_id: String,
    pub enabled: bool,
    pub mode: String,
    pub symbols: Vec<String>,
    pub timeframe: String,
    pub suggested_notional: String,
    pub max_signal_age_ms: i64,
    pub cooldown_seconds: i32,
    pub lookback_candles: i32,
    pub confidence_floor: Option<String>,
    pub stop_loss_pct: Option<String>,
    pub take_profit_pct: Option<String>,
    pub holding_candles: Option<i32>,
    pub notes: Option<String>,
    pub config_version: i32,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub last_evaluation_reason: Option<String>,
    pub last_signal_id: Option<Uuid>,
    pub last_signal_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyListResponse {
    pub strategies: Vec<StrategyStatusView>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyStatusResponse {
    pub strategy: StrategyStatusView,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyConfigValidationResponse {
    pub validation: StrategyConfigValidationResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyConfigVersionsResponse {
    pub versions: Vec<StrategyConfigVersion>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyConfigAuditResponse {
    pub audit: Vec<StrategyConfigAuditEntry>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyDryRunResponse {
    pub result: StrategyDryRunResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyDiagnosticsResponse {
    pub result: StrategyDiagnosticsResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderRecord {
    pub order_id: Uuid,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub signal_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub risk_decision_id: Uuid,
    pub strategy_id: Option<String>,
    pub idempotency_key: String,
    pub requested_notional: Option<String>,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub filled_qty: String,
    pub limit_price: Option<String>,
    pub mode: String,
    pub market_mode: String,
    pub status: String,
    pub execution_state: String,
    pub status_reason: Option<String>,
    pub filled_price: Option<String>,
    pub avg_fill_price: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub rejected_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrdersResponse {
    pub orders: Vec<OrderRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderResponse {
    pub order: OrderRecord,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetStatusResponse {
    pub exchange: String,
    pub environment: String,
    pub rest_base_url: String,
    pub ws_base_url: String,
    pub configured: bool,
    pub request_mode: String,
    pub rate_limits: Value,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExchangePrivateStreamStateRecord {
    pub exchange: String,
    pub environment: String,
    pub status: String,
    pub listen_key_hash: Option<String>,
    pub connected_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub reconnect_count: i32,
    pub updated_at: DateTime<Utc>,
    pub is_stale: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangePrivateStreamEventRecord {
    pub id: Uuid,
    pub exchange: String,
    pub environment: String,
    pub source: String,
    pub event_type: String,
    pub symbol: Option<String>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub execution_type: Option<String>,
    pub order_status: Option<String>,
    pub payload: Value,
    pub event_time: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangePrivateStreamStatusResponse {
    pub state: ExchangePrivateStreamStateRecord,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangePrivateStreamEventsResponse {
    pub events: Vec<ExchangePrivateStreamEventRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangePrivateStreamListenKeyResponse {
    pub state: ExchangePrivateStreamStateRecord,
    pub listen_key_status: String,
    pub listen_key_masked: Option<String>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetSymbolInfo {
    pub exchange: String,
    pub environment: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    pub min_price: Option<String>,
    pub tick_size: Option<String>,
    pub min_qty: Option<String>,
    pub step_size: Option<String>,
    pub min_notional: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetSymbolsResponse {
    pub symbols: Vec<ExchangeTestnetSymbolInfo>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetBalanceRecord {
    pub exchange: String,
    pub environment: String,
    pub asset: String,
    pub free: String,
    pub locked: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetBalancesResponse {
    pub balances: Vec<ExchangeTestnetBalanceRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetOrderRecord {
    pub id: Uuid,
    pub exchange: String,
    pub environment: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: Option<String>,
    pub requested_qty: Option<String>,
    pub requested_notional: Option<String>,
    pub limit_price: Option<String>,
    pub status: String,
    pub execution_state: String,
    pub last_transition_at: Option<DateTime<Utc>>,
    pub ack_payload: Option<Value>,
    pub latest_status_payload: Option<Value>,
    pub risk_decision_id: Option<Uuid>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetOrdersResponse {
    pub orders: Vec<ExchangeTestnetOrderRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetOrderLifecycleEventRecord {
    pub previous_state: Option<String>,
    pub next_state: String,
    pub transition_source: String,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetOrderLifecycleResponse {
    pub client_order_id: String,
    pub current_state: String,
    pub events: Vec<ExchangeTestnetOrderLifecycleEventRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetOrderResponse {
    pub order: ExchangeTestnetOrderRecord,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetPipelinePreviewResponse {
    pub preview: ExchangeTestnetPipelinePreview,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetPipelineSubmitResponse {
    pub preview: ExchangeTestnetPipelinePreview,
    pub order: ExchangeTestnetOrderRecord,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowRunResponse {
    pub run: TestnetShadowRunResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowRunsResponse {
    pub runs: Vec<TestnetShadowRunResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowPromotionResponse {
    pub promotion: TestnetShadowPromotionPreview,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowPromotionsResponse {
    pub promotions: Vec<TestnetShadowPromotionPreview>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowPromotionSubmitResponse {
    pub result: TestnetShadowPromotionResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowRunnerStatusResponse {
    pub config: TestnetShadowRunnerConfig,
    pub state: TestnetShadowRunnerState,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowRunnerConfigResponse {
    pub config: TestnetShadowRunnerConfig,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowRunnerConfigValidationResponse {
    pub validation: Value,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetShadowRunnerControlResponse {
    pub state: TestnetShadowRunnerState,
    pub tick: Option<TestnetShadowRunnerTickResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetOrderRepairRequest {
    pub action: String,
    pub confirmation_text: String,
    pub reason: Option<String>,
    pub force: bool,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetRepairValidationIssue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetRepairResponse {
    pub client_order_id: String,
    pub action: String,
    pub status: String,
    pub previous_state: Option<String>,
    pub next_state: Option<String>,
    pub correlation_id: Uuid,
    pub issues: Vec<ExchangeTestnetRepairValidationIssue>,
    pub request_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetRepairActionRecord {
    pub id: Uuid,
    pub client_order_id: String,
    pub action: String,
    pub status: String,
    pub previous_state: Option<String>,
    pub next_state: Option<String>,
    pub reason: Option<String>,
    pub payload: Option<Value>,
    pub actor_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeTestnetRepairsResponse {
    pub client_order_id: String,
    pub repairs: Vec<ExchangeTestnetRepairActionRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeReconciliationRunRecord {
    pub id: Uuid,
    pub exchange: String,
    pub environment: String,
    pub status: String,
    pub checked_orders: i32,
    pub matched_orders: i32,
    pub mismatched_orders: i32,
    pub unknown_orders: i32,
    pub failed_reason: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeReconciliationMismatchRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub client_order_id: String,
    pub local_status: Option<String>,
    pub exchange_status: Option<String>,
    pub mismatch_kind: String,
    pub action: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeReconciliationResult {
    pub run_id: Uuid,
    pub status: String,
    pub checked_orders: i32,
    pub matched_orders: i32,
    pub mismatched_orders: i32,
    pub unknown_orders: i32,
    pub correlation_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeReconciliationResultResponse {
    pub result: ExchangeReconciliationResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeReconciliationRunsResponse {
    pub runs: Vec<ExchangeReconciliationRunRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeReconciliationRunResponse {
    pub run: ExchangeReconciliationRunRecord,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExchangeReconciliationMismatchesResponse {
    pub mismatches: Vec<ExchangeReconciliationMismatchRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperAccountRecord {
    pub id: Uuid,
    pub name: String,
    pub base_currency: String,
    pub initial_equity: String,
    pub current_equity: String,
    pub realized_pnl: String,
    pub unrealized_pnl: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperAccountResponse {
    pub account: PaperAccountRecord,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperPositionRecord {
    pub id: Uuid,
    pub account_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub entry_price: String,
    pub mark_price: Option<String>,
    pub price_status: String,
    pub notional: String,
    pub realized_pnl: String,
    pub unrealized_pnl: String,
    pub status: String,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub strategy_id: Option<String>,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub order_id: Option<Uuid>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperPositionsResponse {
    pub positions: Vec<PaperPositionRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperClosePositionResponse {
    pub status: String,
    pub position_id: Uuid,
    pub symbol: String,
    pub entry_price: String,
    pub exit_price: String,
    pub quantity: String,
    pub realized_pnl: String,
    pub fee: String,
    pub slippage_cost: String,
    pub close_fill_id: Uuid,
    pub journal_entry_id: Uuid,
    pub correlation_id: Uuid,
    pub closed_at: DateTime<Utc>,
    pub request_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperPnlRecord {
    pub realized_pnl: String,
    pub unrealized_pnl: String,
    pub equity: String,
    pub daily_pnl: String,
    pub drawdown_pct: String,
    pub price_status: String,
    pub open_positions_count: usize,
    pub calculated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperPnlResponse {
    pub pnl: PaperPnlRecord,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperEquitySnapshotRecord {
    pub id: Uuid,
    pub account_id: Uuid,
    pub equity: String,
    pub realized_pnl: String,
    pub unrealized_pnl: String,
    pub drawdown_pct: String,
    pub snapshot_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperEquityResponse {
    pub equity: Vec<PaperEquitySnapshotRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperTradeJournalRecord {
    pub id: Uuid,
    pub account_id: Uuid,
    pub position_id: Option<Uuid>,
    pub order_id: Option<Uuid>,
    pub event_type: String,
    pub symbol: Option<String>,
    pub pnl: Option<String>,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaperTradeJournalResponse {
    pub journal: Vec<PaperTradeJournalRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemEventRecord {
    pub event_id: Uuid,
    pub event_type: String,
    pub source: String,
    pub correlation_id: Uuid,
    pub payload: Option<Value>,
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RecentEventsResponse {
    pub events: Vec<SystemEventRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskDecisionRecord {
    pub id: Uuid,
    pub signal_id: Option<Uuid>,
    pub decision: String,
    pub approved_notional: Option<String>,
    pub risk_score: Option<String>,
    pub reasons: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Uuid,
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskDecisionsResponse {
    pub decisions: Vec<RiskDecisionRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BacktestRunAcceptedResponse {
    pub run_id: Uuid,
    pub status: String,
    pub strategy_id: String,
    pub symbol: String,
    pub trade_count: i32,
    pub pnl: String,
    pub pnl_pct: String,
    pub max_drawdown_pct: String,
    pub win_rate: String,
    pub fee_paid: String,
    pub slippage_cost: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BacktestResult {
    pub run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub final_equity: Decimal,
    pub pnl: Decimal,
    pub pnl_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub win_rate: Decimal,
    pub trade_count: i32,
    pub winning_trades: i32,
    pub losing_trades: i32,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub fee_paid: Decimal,
    pub slippage_cost: Decimal,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BacktestRunsResponse {
    pub runs: Vec<BacktestResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BacktestRunResponse {
    pub run: BacktestResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyExperimentRunAcceptedResponse {
    pub experiment: StrategyExperimentResult,
    pub runs: Vec<StrategyExperimentRun>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyMultiTimeframeExperimentAcceptedResponse {
    pub comparison: StrategyMultiTimeframeExperimentResult,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyExperimentsResponse {
    pub experiments: Vec<StrategyExperimentResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyExperimentResponse {
    pub experiment: StrategyExperimentResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyExperimentRunsResponse {
    pub runs: Vec<StrategyExperimentRun>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyWalkForwardAcceptedResponse {
    pub walk_forward: StrategyWalkForwardResult,
    pub windows: Vec<StrategyWalkForwardWindowResult>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyWalkForwardRunsResponse {
    pub walk_forwards: Vec<StrategyWalkForwardResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyWalkForwardResponse {
    pub walk_forward: StrategyWalkForwardResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyWalkForwardWindowsResponse {
    pub windows: Vec<StrategyWalkForwardWindowResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyMultiTimeframeExperimentResponse {
    pub comparison: StrategyMultiTimeframeExperimentResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyPerformanceSummaryResponse {
    pub summary: StrategyPerformanceSummary,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyPerformanceRankingsResponse {
    pub rankings: Vec<StrategyComparisonSummary>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyDecisionBreakdownResponse {
    pub breakdown: StrategyDecisionBreakdown,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetPromotionFunnelSummaryResponse {
    pub summary: TestnetPromotionFunnelSummary,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetPromotionFunnelOutcomesResponse {
    pub outcomes: Vec<TestnetPromotionOutcomeBreakdown>,
    pub lifecycle: Vec<TestnetPromotionLifecycleBreakdown>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TestnetPromotionFunnelRowsResponse {
    pub rows: Vec<TestnetPromotionFunnelRow>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorReportResponse {
    pub report: OperatorReport,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorReportListItem {
    pub report_id: Uuid,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub format: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorReportsListResponse {
    pub reports: Vec<OperatorReportListItem>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReadinessResponse {
    pub readiness: ExecutionReadinessResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReadinessSnapshotsResponse {
    pub snapshots: Vec<ExecutionReadinessSnapshot>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

fn snapshot_to_result(snapshot: ExecutionReadinessSnapshot) -> ExecutionReadinessResult {
    ExecutionReadinessResult {
        readiness_id: snapshot.id,
        target: snapshot.target,
        status: snapshot.status,
        score: snapshot.score,
        blocking_reasons: snapshot.blocking_reasons,
        warnings: snapshot.warnings,
        checks: snapshot.checks,
        recommendations: snapshot.recommendations,
        computed_at: snapshot.created_at,
        correlation_id: snapshot.correlation_id.unwrap_or_default(),
    }
}

pub fn build_backtest_request(
    args: &crate::cli::BacktestRunArgs,
) -> anyhow::Result<BacktestRequest> {
    let request = BacktestRequest {
        strategy_id: args.strategy.clone(),
        symbol: args.symbol.clone(),
        timeframe: args.timeframe.clone(),
        start_time: args.start,
        end_time: args.end,
        initial_capital: args.initial_capital,
        risk_config_id: args.risk_config_id,
        risk_config: None,
        fee_bps: args.fee_bps,
        slippage_bps: args.slippage_bps,
        correlation_id: args.correlation_id,
        holding_candles: args.holding_candles,
        strategy_config_override: None,
    };
    request.validate().context("invalid backtest request")?;
    Ok(request)
}

pub fn build_strategy_experiment_request(
    args: &crate::cli::StrategyExperimentRunArgs,
) -> anyhow::Result<StrategyExperimentRequest> {
    let request = StrategyExperimentRequest {
        strategy_id: args.strategy.clone(),
        symbol: args.symbol.clone(),
        timeframe: args.timeframe.clone(),
        start_time: args.start,
        end_time: args.end,
        initial_capital: args.initial_capital,
        fee_bps: args.fee_bps,
        slippage_bps: args.slippage_bps,
        lookback_candidates: args.lookbacks.clone(),
        trend_lookback_candidates: args.trend_lookbacks.clone(),
        momentum_lookback_candidates: args.momentum_lookbacks.clone(),
        breakout_lookback_candidates: args.breakout_lookbacks.clone(),
        holding_candles_candidates: args.holding_candles.clone(),
        stop_loss_pct_candidates: args.stop_loss_pct.clone(),
        take_profit_pct_candidates: args.take_profit_pct.clone(),
        max_signal_age_ms: args.max_signal_age_ms,
        max_runs: args.max_runs,
        correlation_id: args.correlation_id,
    };
    request
        .validate()
        .context("invalid strategy experiment request")?;
    Ok(request)
}

pub fn build_multi_timeframe_strategy_experiment_request(
    args: &crate::cli::StrategyMultiTimeframeExperimentRunArgs,
) -> anyhow::Result<StrategyMultiTimeframeExperimentRequest> {
    let request = StrategyMultiTimeframeExperimentRequest {
        strategy_id: args.strategy.clone(),
        symbol: args.symbol.clone(),
        timeframes: args.timeframes.clone(),
        start_time: args.start,
        end_time: args.end,
        initial_capital: args.initial_capital,
        fee_bps: args.fee_bps,
        slippage_bps: args.slippage_bps,
        lookback_candidates: args.lookbacks.clone(),
        trend_lookback_candidates: args.trend_lookbacks.clone(),
        momentum_lookback_candidates: args.momentum_lookbacks.clone(),
        breakout_lookback_candidates: args.breakout_lookbacks.clone(),
        holding_candles_candidates: args.holding_candles.clone(),
        stop_loss_pct_candidates: args.stop_loss_pct.clone(),
        take_profit_pct_candidates: args.take_profit_pct.clone(),
        max_signal_age_ms: args.max_signal_age_ms,
        max_runs: args.max_runs,
        correlation_id: args.correlation_id,
    };
    request
        .validate()
        .context("invalid multi-timeframe strategy experiment request")?;
    Ok(request)
}

pub fn build_strategy_walk_forward_request(
    args: &crate::cli::StrategyWalkForwardRunArgs,
) -> anyhow::Result<StrategyWalkForwardRequest> {
    let request = StrategyWalkForwardRequest {
        strategy_id: args.strategy.clone(),
        symbol: args.symbol.clone(),
        timeframe: args.timeframe.clone(),
        config: args.config_json.clone(),
        experiment_run_id: args.experiment_run_id,
        start_time: args.start,
        end_time: args.end,
        window_train_size_hours: args.train_hours,
        window_test_size_hours: args.test_hours,
        step_size_hours: args.step_hours,
        initial_capital: args.initial_capital,
        fee_bps: args.fee_bps,
        slippage_bps: args.slippage_bps,
        candidate_config: aegis_core::StrategyWalkForwardCandidate {
            lookback_candles: args.lookback_candles,
            trend_lookback_candles: args.trend_lookback,
            momentum_lookback_candles: args.momentum_lookback,
            breakout_lookback_candles: args.breakout_lookback,
            holding_candles: args.holding_candles,
            stop_loss_pct: args.stop_loss_pct,
            take_profit_pct: args.take_profit_pct,
            max_signal_age_ms: args.max_signal_age_ms,
        },
        min_required_test_windows: args.min_required_test_windows,
        correlation_id: args.correlation_id,
    };
    request
        .validate()
        .context("invalid strategy walk-forward request")?;
    Ok(request)
}

pub fn build_strategy_config_request(
    args: &crate::cli::StrategyConfigArgs,
) -> anyhow::Result<StrategyConfigUpdateRequest> {
    Ok(StrategyConfigUpdateRequest {
        strategy_id: args.strategy_id.clone(),
        enabled: args.enabled,
        mode: args.mode.parse().context("invalid strategy mode")?,
        symbols: args.symbols.clone(),
        timeframe: args.timeframe.clone(),
        suggested_notional: args.suggested_notional,
        max_signal_age_ms: args.max_signal_age_ms,
        cooldown_seconds: args.cooldown_seconds,
        lookback_candles: args.lookback_candles,
        trend_lookback_candles: args.trend_lookback_candles,
        momentum_lookback_candles: args.momentum_lookback_candles,
        breakout_lookback_candles: args.breakout_lookback_candles,
        confidence_floor: args.confidence_floor,
        stop_loss_pct: args.stop_loss_pct,
        take_profit_pct: args.take_profit_pct,
        holding_candles: args.holding_candles,
        notes: args.notes.clone(),
    })
}

pub fn build_risk_config_request(args: &crate::cli::RiskConfigArgs) -> anyhow::Result<RiskConfig> {
    let config = RiskConfig {
        max_open_positions: args.max_open_positions,
        max_daily_loss_pct: args.max_daily_loss_pct,
        max_weekly_loss_pct: args.max_weekly_loss_pct,
        max_position_notional: args.max_position_notional,
        max_slippage_pct: args.max_slippage_pct,
        max_consecutive_losses: args.max_consecutive_losses,
        cooldown_seconds: args.cooldown_seconds,
        max_signal_age_ms: args.max_signal_age_ms,
        stale_feed_threshold_seconds: args.stale_feed_threshold_seconds,
    };
    config.validate().context("invalid risk config request")?;
    Ok(config)
}

pub fn build_pipeline_request(args: &crate::cli::PipelineRunArgs) -> PaperTradingPipelineRequest {
    PaperTradingPipelineRequest {
        strategy_id: args.strategy.clone(),
        symbol: args.symbol.clone(),
        timeframe: args.timeframe.clone(),
        correlation_id: args.correlation_id,
    }
}

pub fn build_candle_backfill_request(
    args: &crate::cli::MarketBackfillArgs,
) -> anyhow::Result<CandleBackfillRequest> {
    let request = CandleBackfillRequest {
        exchange: args.exchange.parse()?,
        symbol: args.symbol.clone(),
        interval: args.timeframe.clone(),
        start_time: args.start,
        end_time: args.end,
        limit_per_request: args.limit_per_request,
        correlation_id: args.correlation_id,
    };
    request
        .validate()
        .context("invalid candle backfill request")?;
    Ok(request)
}

pub fn build_candle_aggregation_request(
    args: &crate::cli::MarketAggregateCandlesArgs,
) -> anyhow::Result<CandleAggregationRequest> {
    let request = CandleAggregationRequest {
        exchange: args.exchange.parse()?,
        symbol: args.symbol.clone(),
        source_interval: args.source_interval.clone(),
        target_interval: args.target_interval.clone(),
        start_time: args.start_time,
        end_time: args.end_time,
        correlation_id: args.correlation_id,
    };
    request
        .validate()
        .context("invalid candle aggregation request")?;
    Ok(request)
}

pub fn build_research_data_coverage_query(
    args: &crate::cli::ResearchDataCoverageArgs,
) -> ResearchDataCoverageQuery {
    ResearchDataCoverageQuery {
        exchange: Some(args.exchange.clone()),
        symbol: args.symbol.clone(),
        intervals: args.intervals.clone(),
        start_time: args.start,
        end_time: args.end,
        required_coverage_pct: args.required_coverage_pct,
    }
}

pub fn build_research_data_build_request(
    args: &crate::cli::ResearchDataBuildArgs,
) -> ResearchDatasetBuildRequest {
    ResearchDatasetBuildRequest {
        exchange: args.exchange.parse().expect("validated by clap input"),
        symbol: args.symbol.clone(),
        intervals: args
            .intervals
            .split(',')
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect(),
        start_time: args.start,
        end_time: args.end,
        required_coverage_pct: args.required_coverage_pct.unwrap_or(Decimal::new(95, 0)),
        correlation_id: args.correlation_id,
    }
}

pub fn build_auth_refresh_request(refresh_token: &str) -> AuthRefreshRequestPayload {
    AuthRefreshRequestPayload {
        refresh_token: refresh_token.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_auth_refresh_request, build_backtest_request, build_candle_aggregation_request,
        build_candle_backfill_request, build_pipeline_request, build_strategy_experiment_request,
        ApiClient, RecentEventsQuery, RiskDecisionsQuery, CLI_AUTH_MODE_HEADER,
        CLI_AUTH_MODE_VALUE,
    };
    use crate::cli::{
        BacktestRunArgs, MarketAggregateCandlesArgs, MarketBackfillArgs, PipelineRunArgs,
        StrategyExperimentRunArgs,
    };
    use crate::config::{clear_token_file, load_token_file, StoredAuthSession, StoredUserSummary};
    use aegis_core::{ResearchCandidateDecisionRequest, User, UserRole, UserStatus};
    use chrono::{TimeZone, Utc};
    use reqwest::{header::AUTHORIZATION, Method, StatusCode};
    use rust_decimal::Decimal;
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
    use uuid::Uuid;

    use crate::api::{
        CreateResearchCandidateFromExperimentRunRequest, CreateResearchCandidateRequest,
        ResearchCandidatesQuery,
    };

    fn sample_user() -> User {
        User {
            id: Uuid::from_u128(0x1234),
            email: "owner@example.com".to_string(),
            role: UserRole::Owner,
            status: UserStatus::Active,
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            last_login_at: Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap()),
        }
    }

    fn sample_stored_session() -> StoredAuthSession {
        StoredAuthSession {
            access_token: "expired-access-token".to_string(),
            refresh_token: Some("refresh-token-1".to_string()),
            expires_at: Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 15, 0).unwrap()),
            user: Some(StoredUserSummary::from(&sample_user())),
            saved_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap(),
        }
    }

    fn temp_token_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "aegis-cli-api-{name}-{}-{}.json",
            std::process::id(),
            Uuid::new_v4()
        ))
    }

    fn test_base_url() -> reqwest::Url {
        reqwest::Url::parse("http://aegis.test").expect("valid base url")
    }

    fn request_json<T: for<'de> serde::Deserialize<'de>>(request: &reqwest::Request) -> T {
        let body = request
            .body()
            .and_then(|body| body.as_bytes())
            .expect("request body should be available");
        serde_json::from_slice(body).expect("request body should be valid json")
    }

    fn json_response(value: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&value).expect("response should serialize")
    }

    #[test]
    fn recent_events_query_builds_expected_params() {
        let correlation_id =
            Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("valid uuid");
        let query = RecentEventsQuery {
            limit: 50,
            event_type: Some("risk.rejected".to_string()),
            source: Some("aegis-quant-api".to_string()),
            correlation_id: Some(correlation_id),
        };

        assert_eq!(
            query.to_query_params(),
            vec![
                ("limit", "50".to_string()),
                ("event_type", "risk.rejected".to_string()),
                ("source", "aegis-quant-api".to_string()),
                ("correlation_id", correlation_id.to_string()),
            ]
        );
    }

    #[test]
    fn risk_decisions_query_omits_blank_symbol() {
        let query = RiskDecisionsQuery {
            limit: 10,
            symbol: Some(String::new()),
        };

        assert_eq!(query.to_query_params(), vec![("limit", "10".to_string())]);
    }

    #[test]
    fn backtest_request_serializes_expected_wire_shape() {
        let args = BacktestRunArgs {
            strategy: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            initial_capital: Decimal::new(1000000, 0),
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            holding_candles: Some(3),
            risk_config_id: None,
            correlation_id: None,
        };

        let request = build_backtest_request(&args).expect("valid request");
        let value = serde_json::to_value(request).expect("serializes");

        assert_eq!(value["strategy_id"], "momentum_v1");
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["timeframe"], "1m");
        assert_eq!(value["initial_capital"], "1000000");
        assert_eq!(value["fee_bps"], "10");
        assert_eq!(value["slippage_bps"], "5");
        assert_eq!(value["holding_candles"], 3);
    }

    #[test]
    fn strategy_experiment_request_serializes_expected_wire_shape() {
        let args = StrategyExperimentRunArgs {
            strategy: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            initial_capital: Decimal::new(1000000, 0),
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            lookbacks: vec![3, 5, 10],
            trend_lookbacks: None,
            momentum_lookbacks: None,
            breakout_lookbacks: None,
            holding_candles: Some(vec![3, 5]),
            stop_loss_pct: None,
            take_profit_pct: None,
            max_signal_age_ms: Some(180_000),
            max_runs: Some(4),
            correlation_id: None,
        };

        let request = build_strategy_experiment_request(&args).expect("valid request");
        let value = serde_json::to_value(request).expect("serializes");

        assert_eq!(value["strategy_id"], "momentum_v1");
        assert_eq!(value["lookback_candidates"], serde_json::json!([3, 5, 10]));
        assert_eq!(
            value["holding_candles_candidates"],
            serde_json::json!([3, 5])
        );
        assert_eq!(value["max_runs"], 4);
    }

    #[test]
    fn pipeline_request_serializes_expected_wire_shape() {
        let correlation_id =
            Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("valid uuid");
        let args = PipelineRunArgs {
            strategy: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            correlation_id: Some(correlation_id),
        };

        let request = build_pipeline_request(&args);
        let value = serde_json::to_value(request).expect("serializes");

        assert_eq!(value["strategy_id"], "momentum_v1");
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["timeframe"], "1m");
        assert_eq!(value["correlation_id"], correlation_id.to_string());
    }

    #[test]
    fn candle_backfill_request_serializes_expected_wire_shape() {
        let args = MarketBackfillArgs {
            exchange: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            limit_per_request: Some(1000),
            correlation_id: None,
        };

        let request = build_candle_backfill_request(&args).expect("valid request");
        let value = serde_json::to_value(request).expect("serializes");

        assert_eq!(value["exchange"], "binance");
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["interval"], "1m");
        assert_eq!(value["limit_per_request"], 1000);
    }

    #[test]
    fn candle_aggregation_request_serializes_expected_wire_shape() {
        let args = MarketAggregateCandlesArgs {
            exchange: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            source_interval: "1m".to_string(),
            target_interval: "5m".to_string(),
            start_time: Utc.with_ymd_and_hms(2026, 5, 23, 0, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2026, 5, 24, 0, 0, 0).unwrap(),
            correlation_id: None,
        };

        let request = build_candle_aggregation_request(&args).expect("valid request");
        let value = serde_json::to_value(request).expect("serializes");

        assert_eq!(value["exchange"], "binance");
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["source_interval"], "1m");
        assert_eq!(value["target_interval"], "5m");
    }

    #[test]
    fn refresh_request_payload_serializes_expected_wire_shape() {
        let payload = build_auth_refresh_request("refresh-token-1");
        let value = serde_json::to_value(payload).expect("serializes");

        assert_eq!(value["refresh_token"], "refresh-token-1");
    }

    #[tokio::test]
    async fn auto_refresh_retries_once_and_persists_rotated_session() {
        let protected_hits = Arc::new(AtomicUsize::new(0));
        let token_path = temp_token_path("auto-refresh");
        let stored = sample_stored_session();
        crate::config::save_token_file(&token_path, &stored).expect("token file save");

        let hits = Arc::clone(&protected_hits);
        let client = ApiClient::new_with_test_handler(test_base_url(), move |request| {
            match (request.method().as_str(), request.url().path()) {
                ("GET", "/protected") => {
                    hits.fetch_add(1, Ordering::SeqCst);
                    let token = request
                        .headers()
                        .get(AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    if token == "Bearer refreshed-access-token" {
                        Ok((
                            StatusCode::OK,
                            json_response(serde_json::json!({ "ok": true })),
                        ))
                    } else {
                        Ok((
                            StatusCode::UNAUTHORIZED,
                            json_response(serde_json::json!({ "error": "expired" })),
                        ))
                    }
                }
                ("POST", "/auth/refresh") => {
                    let auth_mode = request
                        .headers()
                        .get(CLI_AUTH_MODE_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    let payload: super::AuthRefreshRequestPayload = request_json(&request);
                    if auth_mode != CLI_AUTH_MODE_VALUE
                        || payload.refresh_token != "refresh-token-1"
                    {
                        return Ok((
                            StatusCode::UNAUTHORIZED,
                            json_response(serde_json::json!({ "error": "invalid_refresh" })),
                        ));
                    }

                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "user": sample_user(),
                            "access_token": "refreshed-access-token",
                            "expires_at": Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap(),
                            "refresh_token": "refresh-token-2"
                        })),
                    ))
                }
                _ => Err(format!(
                    "unexpected request {} {}",
                    request.method(),
                    request.url()
                )),
            }
        })
        .with_auth_session(stored, token_path.clone(), true);

        let response = client
            .get_value("/protected", &[])
            .await
            .expect("protected request should succeed");

        assert_eq!(response["ok"], true);
        assert_eq!(protected_hits.load(Ordering::SeqCst), 2);

        let rotated = load_token_file(&token_path).expect("rotated token file");
        assert_eq!(rotated.access_token, "refreshed-access-token");
        assert_eq!(rotated.refresh_token.as_deref(), Some("refresh-token-2"));

        clear_token_file(&token_path).expect("token file clear");
    }

    #[tokio::test]
    async fn shadow_promotion_preview_serializes_expected_request_body() {
        let client = ApiClient::new_with_test_handler(test_base_url(), |request| {
            assert_eq!(request.method(), Method::POST);
            assert_eq!(
                request.url().path(),
                "/exchange/testnet/shadow/promotions/preview"
            );

            let payload: aegis_core::TestnetShadowPromotionRequest = request_json(&request);
            assert_eq!(
                payload.shadow_run_id,
                Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("valid uuid")
            );
            assert_eq!(
                payload.correlation_id,
                Some(Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("valid uuid"))
            );

            Ok((
                StatusCode::OK,
                json_response(serde_json::json!({
                    "promotion": {
                        "promotion_id": "11111111-1111-1111-1111-111111111111",
                        "shadow_run_id": payload.shadow_run_id,
                        "strategy_id": "momentum_v1",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m",
                        "signal_id": null,
                        "risk_decision_id": "22222222-2222-2222-2222-222222222222",
                        "would_submit_payload": {
                            "exchange": "binance",
                            "environment": "testnet",
                            "symbol": "BTCUSDT",
                            "side": "BUY",
                            "order_type": "MARKET",
                            "time_in_force": null,
                            "quantity": null,
                            "quote_notional": "100000",
                            "limit_price": null,
                            "risk_decision_id": "22222222-2222-2222-2222-222222222222"
                        },
                        "resolved_price": "100000",
                        "price_source": "market_tick",
                        "expires_at": "2026-05-24T00:05:00Z",
                        "reasons": [],
                        "status": "PREVIEWED",
                        "correlation_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                        "created_at": "2026-05-24T00:00:00Z",
                        "submitted_at": null,
                        "testnet_order_id": null,
                        "client_order_id": null
                    },
                    "request_id": "req-1",
                    "correlation_id": "corr-1",
                    "timestamp": "2026-05-24T00:00:00Z"
                })),
            ))
        });
        let response = client
            .exchange_testnet_shadow_promotion_preview(&aegis_core::TestnetShadowPromotionRequest {
                shadow_run_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                    .expect("valid uuid"),
                correlation_id: Some(
                    Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("valid uuid"),
                ),
            })
            .await
            .expect("preview should succeed");

        assert_eq!(response.promotion.symbol, "BTCUSDT");
    }

    #[tokio::test]
    async fn shadow_promotion_client_uses_expected_paths() {
        let promotion_id =
            Uuid::parse_str("33333333-3333-3333-3333-333333333333").expect("valid uuid");
        let client = ApiClient::new_with_test_handler(test_base_url(), move |request| {
            match (request.method().as_str(), request.url().path()) {
                ("GET", "/exchange/testnet/shadow/promotions") => {
                    assert_eq!(request.url().query(), Some("limit=25"));
                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "promotions": [],
                            "request_id": "req-list",
                            "correlation_id": "corr-list",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                ("GET", "/exchange/testnet/shadow/promotions/33333333-3333-3333-3333-333333333333") => {
                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "promotion": {
                                "promotion_id": promotion_id,
                                "shadow_run_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                                "strategy_id": "momentum_v1",
                                "symbol": "BTCUSDT",
                                "timeframe": "1m",
                                "signal_id": null,
                                "risk_decision_id": "22222222-2222-2222-2222-222222222222",
                                "would_submit_payload": {
                                    "exchange": "binance",
                                    "environment": "testnet",
                                    "symbol": "BTCUSDT",
                                    "side": "BUY",
                                    "order_type": "MARKET",
                                    "time_in_force": null,
                                    "quantity": null,
                                    "quote_notional": "100000",
                                    "limit_price": null,
                                    "risk_decision_id": "22222222-2222-2222-2222-222222222222"
                                },
                                "resolved_price": "100000",
                                "price_source": "market_tick",
                                "expires_at": "2026-05-24T00:05:00Z",
                                "reasons": [],
                                "status": "PREVIEWED",
                                "correlation_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                                "created_at": "2026-05-24T00:00:00Z",
                                "submitted_at": null,
                                "testnet_order_id": null,
                                "client_order_id": null
                            },
                            "request_id": "req-get",
                            "correlation_id": "corr-get",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                ("POST", "/exchange/testnet/shadow/promotions/33333333-3333-3333-3333-333333333333/submit") => {
                    let payload: aegis_core::TestnetShadowPromotionSubmitRequest =
                        request_json(&request);
                    assert_eq!(payload.confirmation_text, "PROMOTE TESTNET BTCUSDT");
                    Ok((
                        StatusCode::CREATED,
                        json_response(serde_json::json!({
                            "result": {
                                "promotion_id": promotion_id,
                                "shadow_run_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                                "testnet_order_id": "44444444-4444-4444-4444-444444444444",
                                "client_order_id": "aegis-testnet-1",
                                "execution_state": "EXCHANGE_ACKED",
                                "correlation_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
                            },
                            "request_id": "req-submit",
                            "correlation_id": "corr-submit",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                _ => Err(format!(
                    "unexpected request {} {}",
                    request.method(),
                    request.url()
                )),
            }
        });

        let list_response = client
            .exchange_testnet_shadow_promotions(25)
            .await
            .expect("list should succeed");
        assert!(list_response.promotions.is_empty());

        let get_response = client
            .exchange_testnet_shadow_promotion_get(promotion_id)
            .await
            .expect("get should succeed");
        assert_eq!(get_response.promotion.promotion_id, promotion_id);

        let submit_response = client
            .exchange_testnet_shadow_promotion_submit(
                promotion_id,
                &aegis_core::TestnetShadowPromotionSubmitRequest {
                    confirmation_text: "PROMOTE TESTNET BTCUSDT".to_string(),
                    correlation_id: None,
                },
            )
            .await
            .expect("submit should succeed");
        assert_eq!(submit_response.result.promotion_id, promotion_id);
    }

    #[tokio::test]
    async fn research_candidate_client_uses_expected_create_paths() {
        let experiment_run_id =
            Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("valid uuid");
        let client = ApiClient::new_with_test_handler(test_base_url(), move |request| {
            match (request.method().as_str(), request.url().path()) {
                ("POST", "/research/candidates/from-experiment-run") => {
                    let payload: CreateResearchCandidateFromExperimentRunRequest =
                        request_json(&request);
                    assert_eq!(payload.experiment_run_id, experiment_run_id);
                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "candidate": {
                                "id": "11111111-1111-1111-1111-111111111111",
                                "experiment_id": null,
                                "experiment_run_id": payload.experiment_run_id,
                                "strategy_id": "momentum_v1",
                                "symbol": "BTCUSDT",
                                "timeframe": "15m",
                                "config": {},
                                "score": "50",
                                "pnl_pct": "1.5",
                                "max_drawdown_pct": "0.7",
                                "trade_count": 12,
                                "win_rate": "0.58",
                                "fee_drag": "0.2",
                                "status": "DISCOVERED",
                                "rejection_reason": null,
                                "notes": null,
                                "created_at": "2026-05-24T00:00:00Z",
                                "updated_at": "2026-05-24T00:00:00Z",
                                "correlation_id": null
                            },
                            "request_id": "req-1",
                            "correlation_id": "corr-1",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                ("POST", "/research/candidates") => {
                    let payload: CreateResearchCandidateRequest = request_json(&request);
                    assert_eq!(payload.strategy_id, "momentum_v1");
                    assert_eq!(payload.symbol, "BTCUSDT");
                    assert_eq!(payload.timeframe, "15m");
                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "candidate": {
                                "id": "11111111-1111-1111-1111-111111111111",
                                "experiment_id": null,
                                "experiment_run_id": null,
                                "strategy_id": "momentum_v1",
                                "symbol": "BTCUSDT",
                                "timeframe": "15m",
                                "config": payload.config,
                                "score": null,
                                "pnl_pct": null,
                                "max_drawdown_pct": null,
                                "trade_count": null,
                                "win_rate": null,
                                "fee_drag": null,
                                "status": "DISCOVERED",
                                "rejection_reason": null,
                                "notes": payload.notes,
                                "created_at": "2026-05-24T00:00:00Z",
                                "updated_at": "2026-05-24T00:00:00Z",
                                "correlation_id": null
                            },
                            "request_id": "req-1",
                            "correlation_id": "corr-1",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                _ => Err(format!(
                    "unexpected request {} {}",
                    request.method(),
                    request.url()
                )),
            }
        });

        client
            .create_research_candidate_from_experiment_run(
                &CreateResearchCandidateFromExperimentRunRequest {
                    experiment_run_id,
                    notes: None,
                    correlation_id: None,
                },
            )
            .await
            .expect("experiment registration should succeed");
        client
            .create_research_candidate(&CreateResearchCandidateRequest {
                strategy_id: "momentum_v1".to_string(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "15m".to_string(),
                config: serde_json::json!({ "lookback_candles": 20 }),
                notes: Some("manual".to_string()),
                correlation_id: None,
            })
            .await
            .expect("manual creation should succeed");
    }

    #[tokio::test]
    async fn research_candidate_client_uses_expected_list_get_event_observe_history_summary_and_decide_paths(
    ) {
        let candidate_id =
            Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").expect("valid uuid");
        let client = ApiClient::new_with_test_handler(test_base_url(), move |request| {
            match (request.method().as_str(), request.url().path()) {
                ("GET", "/research/candidates") => {
                    assert_eq!(
                        request.url().query(),
                        Some("limit=25&strategy_id=momentum_v1&symbol=BTCUSDT&timeframe=15m&status=DISCOVERED")
                    );
                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "candidates": [],
                            "request_id": "req-list",
                            "correlation_id": "corr-list",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                ("GET", "/research/candidates/cccccccc-cccc-cccc-cccc-cccccccccccc") => Ok((
                    StatusCode::OK,
                    json_response(serde_json::json!({
                        "candidate": {
                            "id": candidate_id,
                            "experiment_id": null,
                            "experiment_run_id": null,
                            "strategy_id": "momentum_v1",
                            "symbol": "BTCUSDT",
                            "timeframe": "15m",
                            "config": {},
                            "score": "50",
                            "pnl_pct": null,
                            "max_drawdown_pct": null,
                            "trade_count": null,
                            "win_rate": null,
                            "fee_drag": null,
                            "status": "DISCOVERED",
                            "rejection_reason": null,
                            "notes": null,
                            "created_at": "2026-05-24T00:00:00Z",
                            "updated_at": "2026-05-24T00:00:00Z",
                            "correlation_id": null
                        },
                        "request_id": "req-get",
                        "correlation_id": "corr-get",
                        "timestamp": "2026-05-24T00:00:00Z"
                    })),
                )),
                ("GET", "/research/candidates/cccccccc-cccc-cccc-cccc-cccccccccccc/events") => {
                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "events": [],
                            "request_id": "req-events",
                            "correlation_id": "corr-events",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                (
                    "GET",
                    "/research/candidates/cccccccc-cccc-cccc-cccc-cccccccccccc/observations",
                ) => Ok((
                    StatusCode::OK,
                    json_response(serde_json::json!({
                        "observations": [],
                        "history": [],
                        "request_id": "req-observations",
                        "correlation_id": "corr-observations",
                        "timestamp": "2026-05-24T00:00:00Z"
                    })),
                )),
                (
                    "GET",
                    "/research/candidates/cccccccc-cccc-cccc-cccc-cccccccccccc/observation-summary",
                ) => Ok((
                    StatusCode::OK,
                    json_response(serde_json::json!({
                        "summary": {
                            "candidate_id": candidate_id,
                            "total_observations": 0,
                            "latest_observation_status": null,
                            "latest_runner_alignment": null,
                            "latest_readiness_status": null,
                            "latest_recommendations": [],
                            "stale_count": 0,
                            "alignment_mismatch_count": 0,
                            "runner_config_drift_count": 0,
                            "last_observed_at": null,
                            "current_accept_for_shadow_eligible": false,
                            "current_accept_for_shadow_blockers": ["observation_required"],
                            "computed_at": "2026-05-24T00:00:00Z"
                        },
                        "request_id": "req-observation-summary",
                        "correlation_id": "corr-observation-summary",
                        "timestamp": "2026-05-24T00:00:00Z"
                    })),
                )),
                ("POST", "/research/candidates/cccccccc-cccc-cccc-cccc-cccccccccccc/observe") => {
                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "observation": {
                                "observation_id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
                                "candidate_id": candidate_id,
                                "strategy_id": "momentum_v1",
                                "symbol": "BTCUSDT",
                                "timeframe": "15m",
                                "status": "INSUFFICIENT_DATA",
                                "requirements": {
                                  "candidate_id": candidate_id,
                                  "strategy_id": "momentum_v1",
                                  "symbol": "BTCUSDT",
                                  "timeframe": "15m",
                                  "min_observation_hours": 24,
                                  "min_shadow_runs": 30,
                                  "max_risk_rejection_rate": null,
                                  "min_would_submit_count": 1,
                                  "max_no_signal_rate": null,
                                  "require_readiness_ready": true
                                },
                                "runner_alignment": {
                                  "strategy_config_matches_runner": false,
                                  "runner_enabled": true,
                                  "runner_status": "RUNNING",
                                  "runner_timeframe": "1m",
                                  "runner_symbols": ["BTCUSDT"],
                                  "runner_strategies": ["momentum_v1"],
                                  "mismatch_reasons": ["runner timeframe 1m does not include candidate timeframe 15m"]
                                },
                                "summary": {
                                  "candidate_id": candidate_id,
                                  "window_start": "2026-05-24T00:00:00Z",
                                  "window_end": "2026-05-24T00:00:00Z",
                                  "shadow_runs": 0,
                                  "would_submit_count": 0,
                                  "no_signal_count": 0,
                                  "risk_rejected_count": 0,
                                  "skipped_count": 0,
                                  "risk_rejection_rate": "0",
                                  "no_signal_rate": "0",
                                  "latest_readiness_status": "DEGRADED",
                                  "latest_readiness_score": 80,
                                  "runner_alignment": {
                                    "strategy_config_matches_runner": false,
                                    "runner_enabled": true,
                                    "runner_status": "RUNNING",
                                    "runner_timeframe": "1m",
                                    "runner_symbols": ["BTCUSDT"],
                                    "runner_strategies": ["momentum_v1"],
                                    "mismatch_reasons": ["runner timeframe 1m does not include candidate timeframe 15m"]
                                  },
                                  "decision": "INSUFFICIENT_DATA",
                                  "findings": [],
                                  "recommendations": ["Update shadow runner config to include momentum_v1 BTCUSDT 15m."],
                                  "created_at": "2026-05-24T00:00:00Z"
                                },
                                "decision": "INSUFFICIENT_DATA",
                                "started_at": "2026-05-24T00:00:00Z",
                                "evaluated_at": "2026-05-24T00:00:00Z",
                                "created_by": null,
                                "correlation_id": null
                            },
                            "request_id": "req-observe",
                            "correlation_id": "corr-observe",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                ("POST", "/research/candidates/cccccccc-cccc-cccc-cccc-cccccccccccc/decision") => {
                    let payload: ResearchCandidateDecisionRequest = request_json(&request);
                    assert_eq!(payload.decision.as_str(), "REJECT");
                    Ok((
                        StatusCode::OK,
                        json_response(serde_json::json!({
                            "candidate": {
                                "id": candidate_id,
                                "experiment_id": null,
                                "experiment_run_id": null,
                                "strategy_id": "momentum_v1",
                                "symbol": "BTCUSDT",
                                "timeframe": "15m",
                                "config": {},
                                "score": "50",
                                "pnl_pct": null,
                                "max_drawdown_pct": null,
                                "trade_count": null,
                                "win_rate": null,
                                "fee_drag": null,
                                "status": "REJECTED",
                                "rejection_reason": payload.reason,
                                "notes": null,
                                "created_at": "2026-05-24T00:00:00Z",
                                "updated_at": "2026-05-24T00:01:00Z",
                                "correlation_id": null
                            },
                            "request_id": "req-decide",
                            "correlation_id": "corr-decide",
                            "timestamp": "2026-05-24T00:00:00Z"
                        })),
                    ))
                }
                _ => Err(format!(
                    "unexpected request {} {}",
                    request.method(),
                    request.url()
                )),
            }
        });

        let list = client
            .list_research_candidates(&ResearchCandidatesQuery {
                strategy_id: Some("momentum_v1".to_string()),
                symbol: Some("BTCUSDT".to_string()),
                timeframe: Some("15m".to_string()),
                status: Some("DISCOVERED".to_string()),
                limit: 25,
            })
            .await
            .expect("list should succeed");
        assert!(list.candidates.is_empty());

        let get = client
            .get_research_candidate(candidate_id)
            .await
            .expect("get should succeed");
        assert_eq!(get.candidate.id, candidate_id);

        let events = client
            .list_research_candidate_events(candidate_id)
            .await
            .expect("events should succeed");
        assert!(events.events.is_empty());

        let observations = client
            .list_research_candidate_observations(candidate_id)
            .await
            .expect("observations should succeed");
        assert!(observations.history.is_empty());

        let summary = client
            .get_research_candidate_observation_summary(candidate_id)
            .await
            .expect("summary should succeed");
        assert_eq!(summary.summary.candidate_id, candidate_id);

        let observation = client
            .observe_research_candidate(candidate_id)
            .await
            .expect("observe should succeed");
        assert_eq!(observation.observation.candidate_id, candidate_id);

        let decided = client
            .decide_research_candidate(
                candidate_id,
                &ResearchCandidateDecisionRequest {
                    decision: aegis_core::ResearchCandidateDecision::Reject,
                    reason: Some("bad drawdown".to_string()),
                    notes: None,
                    acknowledge_runner_mismatch: false,
                    correlation_id: None,
                },
            )
            .await
            .expect("decision should succeed");
        assert_eq!(decided.candidate.id, candidate_id);
    }
}
