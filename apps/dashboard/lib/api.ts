import type {
  AuthLoginRequest,
  AuthLoginResponse,
  AuthLogoutResponse,
  AuthRefreshResponse,
  AuthUserResponse,
  ApiError,
  BacktestEquityCurveResponse,
  BacktestRequest,
  BacktestRunAcceptedResponse,
  BacktestRunResponse,
  BacktestRunsResponse,
  BacktestTradesResponse,
  CandleBackfillRequest,
  CandleBackfillResult,
  CandleBackfillRunResponse,
  CandleBackfillRunsResponse,
  CandleAggregationRequest,
  CandleAggregationResult,
  CandleCoverageResponse,
  CandlesResponse,
  MarketDataQualityRequest,
  MarketDataQualityResponse,
  MarketDataRepairPlanRequest,
  MarketDataRepairPlanResponse,
  MarketDataRepairRunResponse,
  MarketDataRepairRunsResponse,
  EvaluateStrategyResponse,
  ExchangeReconciliationMismatchesResponse,
  ExchangePrivateStreamEventsResponse,
  ExchangePrivateStreamListenKeyResponse,
  ExchangePrivateStreamStatusResponse,
  ExchangeReconciliationRequest,
  ExchangeReconciliationResultResponse,
  ExchangeReconciliationRunResponse,
  ExchangeReconciliationRunsResponse,
  ExchangeTestnetBalancesResponse,
  ExchangeTestnetOrderLifecycleResponse,
  ExchangeTestnetOrderResponse,
  ExchangeTestnetOrdersResponse,
  ExchangeTestnetPipelinePreviewResponse,
  ExchangeTestnetPipelineSubmitResponse,
  ExchangeTestnetRepairResponse,
  ExchangeTestnetRepairsResponse,
  ExchangeTestnetStatusResponse,
  ExchangeTestnetSymbolsResponse,
  ExecutionReadinessRequest,
  ExecutionReadinessResponse,
  ExecutionReadinessSnapshotsResponse,
  FeedStatusResponse,
  HealthResponse,
  ProviderHealthResponse,
  MarketSymbolsResponse,
  MarketTickResponse,
  OrderResponse,
  OperatorReportRequest,
  OperatorReportResponse,
  OperatorReportsListResponse,
  OrdersResponse,
  PaperAccountResponse,
  PaperClosePositionRequest,
  PaperClosePositionResponse,
  PaperEquityResponse,
  PaperPipelineRequest,
  PaperPipelineResult,
  PaperPnlResponse,
  PaperPositionsResponse,
  PaperTradeJournalResponse,
  RecentSignalsResponse,
  ResearchDataCoverageResponse,
  ResearchBatchRequest,
  ResearchBatchResponse,
  ResearchBatchesResponse,
  ResearchBatchStepsResponse,
  ResearchBatchTriageResponse,
  ResearchCampaignBatchesResponse,
  ResearchCampaignRequest,
  ResearchCampaignResponse,
  ResearchCampaignsResponse,
  ResearchCampaignFailureAttributionResponse,
  ResearchHypothesisGenerationRequest,
  ResearchHypothesisGenerationResponse,
  ResearchHypothesesResponse,
  ResearchHypothesisResponse,
  ResearchExperimentPlanResponse,
  ResearchExperimentPlansResponse,
  ResearchRegimeStrategyLeaderboardResponse,
  ResearchCampaignSummaryResponse,
  ResearchDatasetBuildRequest,
  ResearchDatasetBuildResponse,
  ResearchDatasetBuildsResponse,
  ResearchRegimeCalibrationCandidatesResponse,
  ResearchRegimeCalibrationRequest,
  ResearchRegimeCalibrationResponse,
  ResearchRegimeCalibrationsResponse,
  ResearchRegimeDatasetFromDiscoveryRequest,
  ResearchRegimeDatasetRequest,
  ResearchRegimeDatasetResponse,
  ResearchRegimeDatasetsResponse,
  ResearchRegimeDatasetWindowsResponse,
  ResearchRegimeDiscoveriesResponse,
  ResearchRegimeDiscoveryRequest,
  ResearchRegimeDiscoveryResponse,
  ResearchRegimeDiscoveryWindowsResponse,
  ResearchCandidateQualificationResponse,
  ResearchCandidateQualificationEvaluateResponse,
  ResearchCandidateQualificationHistoryResponse,
  ResearchCandidateObservationSummaryResponse,
  ResearchCandidateReviewRequest,
  ResearchCandidateReviewResponse,
  ResearchCandidateReviewsResponse,
  ResearchCandidateShadowPerformanceResponse,
  ResearchShadowPnlAttributionResponse,
  ResearchCandidateTestnetReviewDossierResponse,
  CreateResearchCandidateFromExperimentRunRequest,
  CreateResearchCandidateRequest,
  ResearchCandidateDecisionRequest,
  ResearchCandidateEventsResponse,
  ResearchCandidateShadowPromotionPreviewResponse,
  ResearchCandidateShadowPromotionRequest,
  ResearchCandidateShadowPromotionResultResponse,
  ResearchCandidateShadowRunsResponse,
  ResearchCandidateWalkForwardEvidenceResponse,
  ResearchCandidateWatchlistResponse,
  ResearchCandidateResponse,
  ResearchCandidatesResponse,
  StrategyCandidateObservationsResponse,
  StrategyCandidateObservationResponse,
  RiskActionResponse,
  RiskConfig,
  RiskConfigAuditResponse,
  RiskConfigResponse,
  RiskConfigValidationResponse,
  RiskConfigVersionsResponse,
  RiskDecisionResponse,
  RiskDecisionsResponse,
  RiskStatusResponse,
  StatusResponse,
  StrategyDecisionBreakdownResponse,
  StrategyDiagnosticsResponse,
  StrategyExperimentRequest,
  StrategyExperimentResponse,
  StrategyExperimentRunAcceptedResponse,
  StrategyExperimentRunsResponse,
  StrategyExperimentsResponse,
  StrategyMultiTimeframeExperimentAcceptedResponse,
  StrategyMultiTimeframeExperimentRequest,
  StrategyMultiTimeframeExperimentResponse,
  StrategyWalkForwardAcceptedResponse,
  StrategyWalkForwardRequest,
  StrategyWalkForwardResponse,
  StrategyWalkForwardRunsResponse,
  StrategyWalkForwardWindowsResponse,
  StrategyRobustnessMatrixAcceptedResponse,
  StrategyRobustnessMatrixCellsResponse,
  StrategyRobustnessMatrixRequest,
  StrategyRobustnessMatrixResponse,
  StrategyRobustnessMatrixRunsResponse,
  StrategyListResponse,
  StrategyPerformanceMode,
  StrategyPerformanceRankingsResponse,
  StrategyPerformanceSummaryResponse,
  StrategyPnlBreakdownResponse,
  StrategySignalFeatureAttributionResponse,
  StrategyConfigAuditResponse,
  StrategyConfigUpdateRequest,
  StrategyConfigValidationResponse,
  StrategyConfigVersionsResponse,
  StrategyDryRunResponse,
  StrategyStatusResponse,
  StrategyExitAttributionResponse,
  StrategyOpportunityAnalysisResponse,
  StrategyToggleResponse,
  SystemEventRecord,
  TestnetPromotionFunnelOutcomesResponse,
  TestnetPromotionFunnelRowsResponse,
  TestnetPromotionFunnelSummaryResponse,
  TestnetShadowPromotionResponse,
  TestnetShadowPromotionSubmitResponse,
  TestnetShadowPromotionsResponse,
  TestnetShadowRunResponse,
  TestnetShadowRunnerConfigResponse,
  TestnetShadowRunnerControlResponse,
  TestnetShadowRunnerStatusResponse,
  TestnetShadowRunsResponse,
} from "@/lib/types";

function dashboardApiBaseUrl() {
  const configured =
    process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") ??
    "http://localhost:3000";

  if (typeof window === "undefined") {
    return configured;
  }

  const localDashboardHost =
    window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1";
  if (localDashboardHost && window.location.port === "3101") {
    return `${window.location.protocol}//${window.location.hostname}:3100`;
  }

  return configured;
}

const API_BASE_URL = dashboardApiBaseUrl();
const ACCESS_TOKEN_STORAGE_KEY = "aegis_dashboard_access_token";
let accessToken =
  typeof window !== "undefined"
    ? window.sessionStorage.getItem(ACCESS_TOKEN_STORAGE_KEY)
    : null;

class HttpError extends Error {
  status: number;
  payload?: ApiError;

  constructor(status: number, message: string, payload?: ApiError) {
    super(message);
    this.name = "HttpError";
    this.status = status;
    this.payload = payload;
  }
}

function withQuery(path: string, query?: Record<string, string | number | undefined>) {
  const url = new URL(`${API_BASE_URL}${path}`);
  Object.entries(query ?? {}).forEach(([key, value]) => {
    if (value !== undefined && value !== "") {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

async function refreshAccessToken(): Promise<boolean> {
  try {
    const response = await fetch(withQuery("/auth/refresh"), {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      credentials: "include",
      cache: "no-store",
      body: "{}",
    });
    if (!response.ok) {
      return false;
    }
    const payload = (await response.json()) as AuthRefreshResponse;
    api.setAccessToken(payload.access_token);
    return true;
  } catch {
    return false;
  }
}

async function request<T>(
  path: string,
  init?: RequestInit,
  query?: Record<string, string | number | undefined>,
): Promise<T> {
  const send = () =>
    fetch(withQuery(path, query), {
      ...init,
      headers: {
        "content-type": "application/json",
        ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
        ...(init?.headers ?? {}),
      },
      credentials: "include",
      cache: "no-store",
    });

  let response = await send();
  if (response.status === 401 && path !== "/auth/login" && path !== "/auth/refresh") {
    const refreshed = await refreshAccessToken();
    if (refreshed) {
      response = await send();
    }
  }

  if (!response.ok) {
    let payload: ApiError | undefined;
    try {
      payload = (await response.json()) as ApiError;
    } catch {
      payload = undefined;
    }
    throw new HttpError(
      response.status,
      payload?.message ?? `Request failed with status ${response.status}`,
      payload,
    );
  }

  return (await response.json()) as T;
}

async function requestText(
  path: string,
  init?: RequestInit,
  query?: Record<string, string | number | undefined>,
): Promise<string> {
  const send = () =>
    fetch(withQuery(path, query), {
      ...init,
      headers: {
        ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
        ...(init?.headers ?? {}),
      },
      credentials: "include",
      cache: "no-store",
    });

  let response = await send();
  if (response.status === 401 && path !== "/auth/refresh") {
    const refreshed = await refreshAccessToken();
    if (refreshed) {
      response = await send();
    }
  }

  if (!response.ok) {
    throw new HttpError(response.status, `Request failed with status ${response.status}`);
  }

  return response.text();
}

export function getErrorMessage(error: unknown) {
  if (!error) {
    return undefined;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return "Unknown error";
}

export function getApiErrorPayload(error: unknown): ApiError | undefined {
  if (error instanceof HttpError) {
    return error.payload;
  }
  return undefined;
}

export const api = {
  setAccessToken: (token: string | null) => {
    accessToken = token;
    if (typeof window === "undefined") {
      return;
    }
    if (token) {
      window.sessionStorage.setItem(ACCESS_TOKEN_STORAGE_KEY, token);
    } else {
      window.sessionStorage.removeItem(ACCESS_TOKEN_STORAGE_KEY);
    }
  },
  getAccessToken: () => accessToken,
  login: (payload: AuthLoginRequest) =>
    request<AuthLoginResponse>("/auth/login", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  me: () => request<AuthUserResponse>("/auth/me"),
  logout: () => request<AuthLogoutResponse>("/auth/logout", { method: "POST", body: "{}" }),
  getSystemHealth: () => request<HealthResponse>("/system/health"),
  getMetricsText: () => requestText("/metrics"),
  getSystemStatus: () => request<StatusResponse>("/system/status"),
  getRiskStatus: () => request<RiskStatusResponse>("/risk/status"),
  getRiskConfig: () => request<RiskConfigResponse>("/risk/config"),
  validateRiskConfig: (payload: RiskConfig) =>
    request<RiskConfigValidationResponse>("/risk/config/validate", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  updateRiskConfig: (payload: RiskConfig) =>
    request<RiskConfigResponse>("/risk/config/update", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getRiskConfigVersions: () => request<RiskConfigVersionsResponse>("/risk/config/versions"),
  getRiskConfigAudit: () => request<RiskConfigAuditResponse>("/risk/config/audit"),
  activateKillSwitch: (reason?: string) =>
    request<RiskActionResponse>("/risk/kill-switch", {
      method: "POST",
      body: JSON.stringify({ reason }),
    }),
  resumeTrading: (confirmationText: string, reason?: string) =>
    request<RiskActionResponse>("/risk/resume", {
      method: "POST",
      body: JSON.stringify({
        confirmation_text: confirmationText,
        reason,
      }),
    }),
  getMarketSymbols: () => request<MarketSymbolsResponse>("/market/symbols"),
  getMarketProviderHealth: () =>
    request<ProviderHealthResponse>("/market/provider-health", undefined, {
      provider: "binance",
      rest: "true",
    }),
  getLatestTick: (symbol: string) =>
    request<MarketTickResponse>("/market/ticks/latest", undefined, { symbol }),
  getMarketCandles: (symbol: string, interval: string, limit = 50) =>
    request<CandlesResponse>("/market/candles", undefined, {
      symbol,
      interval,
      limit,
    }),
  backfillMarketCandles: (payload: CandleBackfillRequest) =>
    request<CandleBackfillResult>("/market/backfill/candles", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getMarketBackfillRuns: (limit = 20) =>
    request<CandleBackfillRunsResponse>("/market/backfill/runs", undefined, { limit }),
  getMarketBackfillRun: (id: string) =>
    request<CandleBackfillRunResponse>(`/market/backfill/runs/${id}`),
  aggregateMarketCandles: (payload: CandleAggregationRequest) =>
    request<CandleAggregationResult>("/market/candles/aggregate", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getMarketCandleCoverage: (symbol: string) =>
    request<CandleCoverageResponse>("/market/candles/coverage", undefined, { symbol }),
  getMarketCandleQuality: (query: MarketDataQualityRequest) =>
    request<MarketDataQualityResponse>("/market/candles/quality", undefined, {
      symbol: query.symbol,
      interval: query.interval,
      start_time: query.start_time,
      end_time: query.end_time,
      exchange: query.exchange ?? "binance",
      ...(query.expected_interval_seconds
        ? { expected_interval_seconds: query.expected_interval_seconds }
        : {}),
      ...(query.max_allowed_gap_count !== undefined && query.max_allowed_gap_count !== null
        ? { max_allowed_gap_count: query.max_allowed_gap_count }
        : {}),
      ...(query.max_allowed_gap_pct ? { max_allowed_gap_pct: query.max_allowed_gap_pct } : {}),
    }),
  planMarketDataRepair: (payload: MarketDataRepairPlanRequest) =>
    request<MarketDataRepairPlanResponse>("/market/candles/repair/plan", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  runMarketDataRepair: (payload: MarketDataRepairPlanRequest) =>
    request<MarketDataRepairRunResponse>("/market/candles/repair/run", {
      method: "POST",
      body: JSON.stringify({ plan: payload }),
    }),
  getMarketDataRepairRuns: (limit = 20) =>
    request<MarketDataRepairRunsResponse>("/market/candles/repair/runs", undefined, { limit }),
  getResearchDataCoverage: (query: {
    exchange?: string;
    symbol: string;
    intervals: string;
    start_time: string;
    end_time: string;
    required_coverage_pct?: string;
  }) => request<ResearchDataCoverageResponse>("/research/data/coverage", undefined, query),
  buildResearchDataset: (payload: ResearchDatasetBuildRequest) =>
    request<ResearchDatasetBuildResponse>("/research/data/build", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  listResearchDatasetBuilds: (limit = 20) =>
    request<ResearchDatasetBuildsResponse>("/research/data/builds", undefined, { limit }),
  getResearchDatasetBuild: (id: string) =>
    request<ResearchDatasetBuildResponse>(`/research/data/builds/${id}`),
  buildResearchRegimeDataset: (payload: ResearchRegimeDatasetRequest) =>
    request<ResearchRegimeDatasetResponse>("/research/regime-datasets/build", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  buildResearchRegimeDatasetFromDiscovery: (payload: ResearchRegimeDatasetFromDiscoveryRequest) =>
    request<ResearchRegimeDatasetResponse>("/research/regime-datasets/from-discovery", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  listResearchRegimeDatasets: (limit = 20) =>
    request<ResearchRegimeDatasetsResponse>("/research/regime-datasets", undefined, { limit }),
  getResearchRegimeDataset: (id: string) =>
    request<ResearchRegimeDatasetResponse>(`/research/regime-datasets/${id}`),
  getResearchRegimeDatasetWindows: (id: string) =>
    request<ResearchRegimeDatasetWindowsResponse>(`/research/regime-datasets/${id}/windows`),
  runResearchRegimeDiscovery: (payload: ResearchRegimeDiscoveryRequest) =>
    request<ResearchRegimeDiscoveryResponse>("/research/regime-discovery/run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  runResearchRegimeCalibration: (payload: ResearchRegimeCalibrationRequest) =>
    request<ResearchRegimeCalibrationResponse>("/research/regime-calibration/run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  listResearchRegimeCalibrations: (limit = 20) =>
    request<ResearchRegimeCalibrationsResponse>("/research/regime-calibration", undefined, { limit }),
  getResearchRegimeCalibration: (id: string) =>
    request<ResearchRegimeCalibrationResponse>(`/research/regime-calibration/${id}`),
  getResearchRegimeCalibrationCandidates: (id: string) =>
    request<ResearchRegimeCalibrationCandidatesResponse>(
      `/research/regime-calibration/${id}/candidates`,
    ),
  listResearchRegimeDiscoveries: (limit = 20) =>
    request<ResearchRegimeDiscoveriesResponse>("/research/regime-discovery", undefined, { limit }),
  getResearchRegimeDiscovery: (id: string) =>
    request<ResearchRegimeDiscoveryResponse>(`/research/regime-discovery/${id}`),
  getResearchRegimeDiscoveryWindows: (id: string) =>
    request<ResearchRegimeDiscoveryWindowsResponse>(`/research/regime-discovery/${id}/windows`),
  runResearchBatch: (payload: ResearchBatchRequest) =>
    request<ResearchBatchResponse>("/research/batches/run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  listResearchBatches: (limit = 20) =>
    request<ResearchBatchesResponse>("/research/batches", undefined, { limit }),
  getResearchBatch: (id: string) =>
    request<ResearchBatchResponse>(`/research/batches/${id}`),
  getResearchBatchSteps: (id: string) =>
    request<ResearchBatchStepsResponse>(`/research/batches/${id}/steps`),
  getResearchBatchTriage: (id: string) =>
    request<ResearchBatchTriageResponse>(`/research/batches/${id}/triage`),
  runResearchCampaign: (payload: ResearchCampaignRequest) =>
    request<ResearchCampaignResponse>("/research/campaigns/run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  listResearchCampaigns: (limit = 20) =>
    request<ResearchCampaignsResponse>("/research/campaigns", undefined, { limit }),
  getResearchCampaign: (id: string) =>
    request<ResearchCampaignResponse>(`/research/campaigns/${id}`),
  getResearchCampaignBatches: (id: string) =>
    request<ResearchCampaignBatchesResponse>(`/research/campaigns/${id}/batches`),
  getResearchCampaignSummary: (id: string) =>
    request<ResearchCampaignSummaryResponse>(`/research/campaigns/${id}/summary`),
  getResearchCampaignFailureAttribution: (id: string) =>
    request<ResearchCampaignFailureAttributionResponse>(
      `/research/campaigns/${id}/failure-attribution`,
    ),
  getResearchCampaignRegimeLeaderboard: (id: string) =>
    request<ResearchRegimeStrategyLeaderboardResponse>(
      `/research/campaigns/${id}/regime-leaderboard`,
    ),
  generateResearchHypotheses: (payload: ResearchHypothesisGenerationRequest) =>
    request<ResearchHypothesisGenerationResponse>("/research/hypotheses/generate", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  listResearchHypotheses: (limit = 50) =>
    request<ResearchHypothesesResponse>("/research/hypotheses", undefined, { limit }),
  getResearchHypothesis: (id: string) =>
    request<ResearchHypothesisResponse>(`/research/hypotheses/${id}`),
  decideResearchHypothesis: (
    id: string,
    payload: { decision: string; reason?: string },
  ) =>
    request<ResearchHypothesisResponse>(`/research/hypotheses/${id}/decision`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  createResearchExperimentPlan: (id: string) =>
    request<ResearchExperimentPlanResponse>(`/research/hypotheses/${id}/plan`, {
      method: "POST",
      body: JSON.stringify({}),
    }),
  listResearchExperimentPlans: (limit = 50) =>
    request<ResearchExperimentPlansResponse>("/research/experiment-plans", undefined, { limit }),
  getResearchExperimentPlan: (id: string) =>
    request<ResearchExperimentPlanResponse>(`/research/experiment-plans/${id}`),
  validateResearchExperimentPlan: (id: string) =>
    request<ResearchExperimentPlanResponse>(`/research/experiment-plans/${id}/validate`, {
      method: "POST",
      body: JSON.stringify({}),
    }),
  archiveResearchExperimentPlan: (id: string, reason?: string) =>
    request<ResearchExperimentPlanResponse>(`/research/experiment-plans/${id}/archive`, {
      method: "POST",
      body: JSON.stringify({ reason }),
    }),
  createResearchCandidate: (payload: CreateResearchCandidateRequest) =>
    request<ResearchCandidateResponse>("/research/candidates", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  createResearchCandidateFromExperimentRun: (
    payload: CreateResearchCandidateFromExperimentRunRequest,
  ) =>
    request<ResearchCandidateResponse>("/research/candidates/from-experiment-run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  listResearchCandidates: (query?: Record<string, string | number | undefined>) =>
    request<ResearchCandidatesResponse>("/research/candidates", undefined, query),
  getResearchCandidate: (id: string) =>
    request<ResearchCandidateResponse>(`/research/candidates/${id}`),
  listResearchCandidateEvents: (id: string) =>
    request<ResearchCandidateEventsResponse>(`/research/candidates/${id}/events`),
  listResearchCandidateObservations: (id: string) =>
    request<StrategyCandidateObservationsResponse>(`/research/candidates/${id}/observations`),
  getResearchCandidateObservationSummary: (id: string) =>
    request<ResearchCandidateObservationSummaryResponse>(
      `/research/candidates/${id}/observation-summary`,
    ),
  getResearchCandidateQualification: (id: string) =>
    request<ResearchCandidateQualificationResponse>(`/research/candidates/${id}/qualification`),
  evaluateResearchCandidateQualification: (id: string) =>
    request<ResearchCandidateQualificationEvaluateResponse>(
      `/research/candidates/${id}/qualification/evaluate`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),
  getResearchCandidateQualificationHistory: (id: string, limit = 20) =>
    request<ResearchCandidateQualificationHistoryResponse>(
      `/research/candidates/${id}/qualification/history`,
      undefined,
      { limit },
    ),
  getResearchCandidateTestnetReviewDossier: (id: string) =>
    request<ResearchCandidateTestnetReviewDossierResponse>(
      `/research/candidates/${id}/testnet-review-dossier`,
    ),
  getResearchCandidateWalkForward: (id: string) =>
    request<ResearchCandidateWalkForwardEvidenceResponse>(
      `/research/candidates/${id}/walk-forward`,
    ),
  linkResearchCandidateWalkForward: (id: string, walkForwardRunId: string) =>
    request<ResearchCandidateWalkForwardEvidenceResponse>(
      `/research/candidates/${id}/walk-forward/link`,
      {
        method: "POST",
        body: JSON.stringify({ walk_forward_run_id: walkForwardRunId }),
      },
    ),
  getResearchCandidateWatchlist: (limit = 50) =>
    request<ResearchCandidateWatchlistResponse>(
      "/research/candidates/watchlist",
      undefined,
      { limit },
    ),
  getResearchCandidateReviews: (id: string) =>
    request<ResearchCandidateReviewsResponse>(`/research/candidates/${id}/reviews`),
  createResearchCandidateReview: (
    id: string,
    payload: ResearchCandidateReviewRequest,
  ) =>
    request<ResearchCandidateReviewResponse>(`/research/candidates/${id}/reviews`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getResearchCandidateShadowPerformance: (
    id: string,
    query?: { start_time?: string; end_time?: string },
  ) =>
    request<ResearchCandidateShadowPerformanceResponse>(
      `/research/candidates/${id}/shadow-performance`,
      undefined,
      query,
    ),
  getResearchCandidateShadowPnlAttribution: (
    id: string,
    query?: { holding_windows?: string; fee_bps?: string; slippage_bps?: string; limit?: number },
  ) =>
    request<ResearchShadowPnlAttributionResponse>(
      `/research/candidates/${id}/shadow-pnl-attribution`,
      undefined,
      query,
    ),
  getResearchCandidateShadowRuns: (
    id: string,
    query?: { start_time?: string; end_time?: string; limit?: number },
  ) =>
    request<ResearchCandidateShadowRunsResponse>(
      `/research/candidates/${id}/shadow-runs`,
      undefined,
      query,
    ),
  observeResearchCandidate: (id: string) =>
    request<StrategyCandidateObservationResponse>(`/research/candidates/${id}/observe`, {
      method: "POST",
      body: JSON.stringify({}),
    }),
  decideResearchCandidate: (
    id: string,
    payload: ResearchCandidateDecisionRequest,
  ) =>
    request<ResearchCandidateResponse>(
      `/research/candidates/${id}/decision`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    ),
  previewResearchCandidateShadowPromotion: (
    id: string,
    payload: ResearchCandidateShadowPromotionRequest,
  ) =>
    request<ResearchCandidateShadowPromotionPreviewResponse>(
      `/research/candidates/${id}/promote-shadow/preview`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    ),
  applyResearchCandidateShadowPromotion: (
    id: string,
    payload: ResearchCandidateShadowPromotionRequest,
  ) =>
    request<ResearchCandidateShadowPromotionResultResponse>(
      `/research/candidates/${id}/promote-shadow/apply`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    ),
  getExchangeTestnetStatus: () =>
    request<ExchangeTestnetStatusResponse>("/exchange/testnet/status"),
  getExchangeTestnetPrivateStreamStatus: () =>
    request<ExchangePrivateStreamStatusResponse>("/exchange/testnet/private-stream/status"),
  getExchangeTestnetPrivateStreamEvents: (
    limit = 50,
    clientOrderId?: string,
    eventType?: string,
  ) =>
    request<ExchangePrivateStreamEventsResponse>(
      "/exchange/testnet/private-stream/events",
      undefined,
      { limit, client_order_id: clientOrderId, event_type: eventType },
    ),
  createExchangeTestnetPrivateStreamListenKey: () =>
    request<ExchangePrivateStreamListenKeyResponse>(
      "/exchange/testnet/private-stream/listen-key",
      { method: "POST", body: "{}" },
    ),
  keepaliveExchangeTestnetPrivateStreamListenKey: (listenKey: string) =>
    request<ExchangePrivateStreamListenKeyResponse>(
      "/exchange/testnet/private-stream/listen-key/keepalive",
      { method: "POST", body: JSON.stringify({ listen_key: listenKey }) },
    ),
  closeExchangeTestnetPrivateStreamListenKey: (listenKey: string) =>
    request<ExchangePrivateStreamListenKeyResponse>(
      "/exchange/testnet/private-stream/listen-key/close",
      { method: "POST", body: JSON.stringify({ listen_key: listenKey }) },
    ),
  getExchangeTestnetSymbols: () =>
    request<ExchangeTestnetSymbolsResponse>("/exchange/testnet/symbols"),
  getExchangeTestnetBalances: () =>
    request<ExchangeTestnetBalancesResponse>("/exchange/testnet/balances"),
  getExchangeTestnetOrders: (limit = 20) =>
    request<ExchangeTestnetOrdersResponse>("/exchange/testnet/orders", undefined, { limit }),
  getExchangeTestnetOrderLifecycle: (clientOrderId: string) =>
    request<ExchangeTestnetOrderLifecycleResponse>(
      `/exchange/testnet/orders/${clientOrderId}/lifecycle`,
    ),
  repairExchangeTestnetOrder: (
    clientOrderId: string,
    payload: {
      action: string;
      confirmation_text: string;
      reason?: string;
      force?: boolean;
    },
  ) =>
    request<ExchangeTestnetRepairResponse>(
      `/exchange/testnet/orders/${clientOrderId}/repair`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    ),
  getExchangeTestnetOrderRepairs: (clientOrderId: string) =>
    request<ExchangeTestnetRepairsResponse>(
      `/exchange/testnet/orders/${clientOrderId}/repairs`,
    ),
  previewExchangeTestnetPipeline: (payload: Record<string, unknown>) =>
    request<ExchangeTestnetPipelinePreviewResponse>("/exchange/testnet/pipeline/preview", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  submitExchangeTestnetPipeline: (payload: Record<string, unknown>) =>
    request<ExchangeTestnetPipelineSubmitResponse>("/exchange/testnet/pipeline/submit", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  runExchangeTestnetShadow: (payload: {
    strategy_id: string;
    symbol: string;
    timeframe: string;
    correlation_id?: string;
  }) =>
    request<TestnetShadowRunResponse>("/exchange/testnet/shadow/run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getExchangeTestnetShadowRuns: (limit = 20) =>
    request<TestnetShadowRunsResponse>("/exchange/testnet/shadow/runs", undefined, { limit }),
  getExchangeTestnetShadowRun: (runId: string) =>
    request<TestnetShadowRunResponse>(`/exchange/testnet/shadow/runs/${runId}`),
  previewExchangeTestnetShadowPromotion: (payload: { shadow_run_id: string }) =>
    request<TestnetShadowPromotionResponse>("/exchange/testnet/shadow/promotions/preview", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getExchangeTestnetShadowPromotions: (limit = 20) =>
    request<TestnetShadowPromotionsResponse>(
      "/exchange/testnet/shadow/promotions",
      undefined,
      { limit },
    ),
  getExchangeTestnetShadowPromotion: (promotionId: string) =>
    request<TestnetShadowPromotionResponse>(
      `/exchange/testnet/shadow/promotions/${promotionId}`,
    ),
  submitExchangeTestnetShadowPromotion: (
    promotionId: string,
    payload: { confirmation_text: string },
  ) =>
    request<TestnetShadowPromotionSubmitResponse>(
      `/exchange/testnet/shadow/promotions/${promotionId}/submit`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    ),
  getExchangeTestnetShadowRunnerStatus: () =>
    request<TestnetShadowRunnerStatusResponse>("/exchange/testnet/shadow-runner/status"),
  getExchangeTestnetShadowRunnerConfig: () =>
    request<TestnetShadowRunnerConfigResponse>("/exchange/testnet/shadow-runner/config"),
  updateExchangeTestnetShadowRunnerConfig: (payload: Record<string, unknown>) =>
    request<TestnetShadowRunnerConfigResponse>("/exchange/testnet/shadow-runner/config/update", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  controlExchangeTestnetShadowRunner: (payload: Record<string, unknown>) =>
    request<TestnetShadowRunnerControlResponse>("/exchange/testnet/shadow-runner/control", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  reconcileExchangeTestnetOrders: (payload: ExchangeReconciliationRequest) =>
    request<ExchangeReconciliationResultResponse>("/exchange/testnet/reconcile", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getExchangeReconciliationRuns: (limit = 20) =>
    request<ExchangeReconciliationRunsResponse>(
      "/exchange/testnet/reconciliation/runs",
      undefined,
      { limit },
    ),
  getExchangeReconciliationRun: (runId: string) =>
    request<ExchangeReconciliationRunResponse>(
      `/exchange/testnet/reconciliation/runs/${runId}`,
    ),
  getExchangeReconciliationMismatches: (runId: string) =>
    request<ExchangeReconciliationMismatchesResponse>(
      `/exchange/testnet/reconciliation/runs/${runId}/mismatches`,
    ),
  submitExchangeTestnetOrder: (payload: Record<string, unknown>) =>
    request<ExchangeTestnetOrderResponse>("/exchange/testnet/orders", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  cancelExchangeTestnetOrder: (clientOrderId: string, confirmationText: string) =>
    request<ExchangeTestnetOrderResponse>(
      `/exchange/testnet/orders/${clientOrderId}/cancel`,
      {
        method: "POST",
        body: JSON.stringify({ confirmation_text: confirmationText }),
      },
    ),
  getMarketFeedStatus: () => request<FeedStatusResponse>("/market/feed-status"),
  getStrategyList: () => request<StrategyListResponse>("/strategy/list"),
  getStrategyStatus: (id: string) =>
    request<StrategyStatusResponse>(`/strategy/${id}/status`),
  getStrategyConfig: (id: string) =>
    request<StrategyStatusResponse>(`/strategy/${id}/config`),
  validateStrategyConfig: (id: string, payload: StrategyConfigUpdateRequest) =>
    request<StrategyConfigValidationResponse>(`/strategy/${id}/config/validate`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  updateStrategyConfig: (id: string, payload: StrategyConfigUpdateRequest) =>
    request<StrategyStatusResponse>(`/strategy/${id}/config/update`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getStrategyConfigVersions: (id: string) =>
    request<StrategyConfigVersionsResponse>(`/strategy/${id}/config/versions`),
  getStrategyConfigAudit: (id: string) =>
    request<StrategyConfigAuditResponse>(`/strategy/${id}/config/audit`),
  dryRunStrategy: (
    id: string,
    payload: { symbol?: string; timeframe?: string; config_override?: StrategyConfigUpdateRequest },
  ) =>
    request<StrategyDryRunResponse>(`/strategy/${id}/dry-run`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getStrategyDiagnostics: (
    id: string,
    params?: { symbol?: string; timeframe?: string; limit?: number },
  ) =>
    request<StrategyDiagnosticsResponse>(`/strategy/${id}/diagnostics`, undefined, params),
  getStrategyOpportunityAnalysis: (
    id: string,
    params: {
      symbol: string;
      timeframe: string;
      start_time: string;
      end_time: string;
      limit_samples?: number;
      include_examples?: string;
    },
  ) =>
    request<StrategyOpportunityAnalysisResponse>(
      `/strategy/${id}/opportunity-analysis`,
      undefined,
      params,
    ),
  getStrategyExitAttribution: (
    id: string,
    params: {
      symbol: string;
      timeframe: string;
      start_time: string;
      end_time: string;
      experiment_run_id?: string;
      holding_windows?: string;
      fee_bps: string;
      slippage_bps: string;
    },
  ) =>
    request<StrategyExitAttributionResponse>(
      `/strategy/${id}/exit-attribution`,
      undefined,
      params,
    ),
  getStrategySignalFeatureAttribution: (
    id: string,
    params: {
      symbol: string;
      timeframe: string;
      start_time: string;
      end_time: string;
      experiment_run_id?: string;
      holding_window?: string;
      fee_bps: string;
      slippage_bps: string;
      min_samples_per_bucket?: string;
    },
  ) =>
    request<StrategySignalFeatureAttributionResponse>(
      `/strategy/${id}/signal-feature-attribution`,
      undefined,
      params,
    ),
  enableStrategy: (id: string) =>
    request<StrategyToggleResponse>(`/strategy/${id}/enable`, { method: "POST" }),
  disableStrategy: (id: string) =>
    request<StrategyToggleResponse>(`/strategy/${id}/disable`, { method: "POST" }),
  evaluateStrategy: (id: string, symbol?: string) =>
    request<EvaluateStrategyResponse>(`/strategy/${id}/evaluate`, {
      method: "POST",
      body: JSON.stringify({ symbol }),
    }),
  getRecentSignals: (symbol?: string, limit = 20) =>
    request<RecentSignalsResponse>("/signals/recent", undefined, { symbol, limit }),
  runPaperPipeline: (payload: PaperPipelineRequest) =>
    request<PaperPipelineResult>("/paper/pipeline/run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getRiskDecisions: (symbol?: string, limit = 50) =>
    request<RiskDecisionsResponse>("/risk/decisions", undefined, { symbol, limit }),
  getRiskDecision: (id: string) =>
    request<RiskDecisionResponse>(`/risk/decisions/${id}`),
  getOrders: () => request<OrdersResponse>("/orders"),
  getOrder: (id: string) => request<OrderResponse>(`/orders/${id}`),
  getPaperAccount: () => request<PaperAccountResponse>("/paper/account"),
  getPaperPositions: (limit = 50, status = "ALL") =>
    request<PaperPositionsResponse>("/paper/positions", undefined, { limit, status }),
  closePaperPosition: (positionId: string, payload: PaperClosePositionRequest) =>
    request<PaperClosePositionResponse>(`/paper/positions/${positionId}/close`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getPaperPnl: () => request<PaperPnlResponse>("/paper/pnl/daily"),
  getPaperEquity: (limit = 50) =>
    request<PaperEquityResponse>("/paper/equity", undefined, { limit }),
  getPaperTradeJournal: (limit = 50) =>
    request<PaperTradeJournalResponse>("/paper/trade-journal", undefined, { limit }),
  markPaperToMarket: () =>
    request<PaperPnlResponse>("/paper/account/mark-to-market", { method: "POST" }),
  runBacktest: (payload: BacktestRequest) =>
    request<BacktestRunAcceptedResponse>("/backtest/run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getBacktestRuns: (limit = 20) =>
    request<BacktestRunsResponse>("/backtest/runs", undefined, { limit }),
  getBacktestRun: (id: string) =>
    request<BacktestRunResponse>(`/backtest/runs/${id}`),
  getBacktestTrades: (id: string) =>
    request<BacktestTradesResponse>(`/backtest/runs/${id}/trades`),
  getBacktestEquity: (id: string) =>
    request<BacktestEquityCurveResponse>(`/backtest/runs/${id}/equity`),
  runStrategyExperiment: (payload: StrategyExperimentRequest) =>
    request<StrategyExperimentRunAcceptedResponse>("/experiments/strategy/run", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  runMultiTimeframeStrategyExperiment: (payload: StrategyMultiTimeframeExperimentRequest) =>
    request<StrategyMultiTimeframeExperimentAcceptedResponse>(
      "/experiments/strategy/multi-timeframe",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    ),
  runStrategyWalkForward: (payload: StrategyWalkForwardRequest) =>
    request<StrategyWalkForwardAcceptedResponse>("/experiments/strategy/walk-forward", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getStrategyExperiments: (limit = 20) =>
    request<StrategyExperimentsResponse>("/experiments/strategy", undefined, { limit }),
  getStrategyExperiment: (id: string) =>
    request<StrategyExperimentResponse>(`/experiments/strategy/${id}`),
  getStrategyExperimentRuns: (id: string) =>
    request<StrategyExperimentRunsResponse>(`/experiments/strategy/${id}/runs`),
  getStrategyMultiTimeframeComparison: (id: string) =>
    request<StrategyMultiTimeframeExperimentResponse>(`/experiments/strategy/${id}/comparison`),
  getStrategyWalkForwards: (limit = 20) =>
    request<StrategyWalkForwardRunsResponse>("/experiments/strategy/walk-forward", undefined, {
      limit,
    }),
  getStrategyWalkForward: (id: string) =>
    request<StrategyWalkForwardResponse>(`/experiments/strategy/walk-forward/${id}`),
  getStrategyWalkForwardWindows: (id: string) =>
    request<StrategyWalkForwardWindowsResponse>(
      `/experiments/strategy/walk-forward/${id}/windows`,
    ),
  runStrategyRobustnessMatrix: (payload: StrategyRobustnessMatrixRequest) =>
    request<StrategyRobustnessMatrixAcceptedResponse>(
      "/research/strategy-robustness-matrix/run",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    ),
  getStrategyRobustnessMatrices: (limit = 20) =>
    request<StrategyRobustnessMatrixRunsResponse>(
      "/research/strategy-robustness-matrix",
      undefined,
      { limit },
    ),
  getStrategyRobustnessMatrix: (id: string) =>
    request<StrategyRobustnessMatrixResponse>(`/research/strategy-robustness-matrix/${id}`),
  getStrategyRobustnessMatrixCells: (id: string) =>
    request<StrategyRobustnessMatrixCellsResponse>(
      `/research/strategy-robustness-matrix/${id}/cells`,
    ),
  getStrategyPerformance: (
    mode: StrategyPerformanceMode,
    strategyId?: string,
    symbol?: string,
    timeframe?: string,
    startTime?: string,
    endTime?: string,
    limit?: number,
  ) =>
    request<StrategyPerformanceSummaryResponse>(
      "/analytics/strategy/performance",
      undefined,
      {
        mode,
        strategy_id: strategyId,
        symbol,
        timeframe,
        start_time: startTime,
        end_time: endTime,
        limit,
      },
    ),
  getStrategyPerformanceRankings: (
    mode: StrategyPerformanceMode,
    symbol?: string,
    timeframe?: string,
    limit = 20,
  ) =>
    request<StrategyPerformanceRankingsResponse>(
      "/analytics/strategy/rankings",
      undefined,
      { mode, symbol, timeframe, limit },
    ),
  getStrategyDecisionBreakdown: (
    strategyId: string,
    symbol?: string,
    timeframe?: string,
    startTime?: string,
    endTime?: string,
  ) =>
    request<StrategyDecisionBreakdownResponse>(
      `/analytics/strategy/${strategyId}/decision-breakdown`,
      undefined,
      { symbol, timeframe, start_time: startTime, end_time: endTime },
    ),
  getStrategyPaperPnlBreakdown: (
    strategyId: string,
    symbol?: string,
    timeframe?: string,
    startTime?: string,
    endTime?: string,
  ) =>
    request<StrategyPnlBreakdownResponse>(
      `/analytics/strategy/${strategyId}/paper-pnl-breakdown`,
      undefined,
      { symbol, timeframe, start_time: startTime, end_time: endTime },
    ),
  getStrategyBacktestBreakdown: (
    strategyId: string,
    symbol?: string,
    timeframe?: string,
    startTime?: string,
    endTime?: string,
  ) =>
    request<StrategyPnlBreakdownResponse>(
      `/analytics/strategy/${strategyId}/backtest-breakdown`,
      undefined,
      { symbol, timeframe, start_time: startTime, end_time: endTime },
    ),
  getTestnetPromotionFunnel: (
    strategyId?: string,
    symbol?: string,
    timeframe?: string,
    startTime?: string,
    endTime?: string,
  ) =>
    request<TestnetPromotionFunnelSummaryResponse>(
      "/analytics/testnet/promotion-funnel",
      undefined,
      {
        strategy_id: strategyId,
        symbol,
        timeframe,
        start_time: startTime,
        end_time: endTime,
      },
    ),
  getTestnetPromotionOutcomes: (
    strategyId?: string,
    symbol?: string,
    timeframe?: string,
    startTime?: string,
    endTime?: string,
  ) =>
    request<TestnetPromotionFunnelOutcomesResponse>(
      "/analytics/testnet/promotion-funnel/outcomes",
      undefined,
      {
        strategy_id: strategyId,
        symbol,
        timeframe,
        start_time: startTime,
        end_time: endTime,
      },
    ),
  getTestnetPromotionRows: (
    strategyId?: string,
    symbol?: string,
    timeframe?: string,
    startTime?: string,
    endTime?: string,
    limit = 50,
  ) =>
    request<TestnetPromotionFunnelRowsResponse>(
      "/analytics/testnet/promotion-funnel/rows",
      undefined,
      {
        strategy_id: strategyId,
        symbol,
        timeframe,
        start_time: startTime,
        end_time: endTime,
        limit,
      },
    ),
  generateOperatorReport: (payload: OperatorReportRequest) =>
    request<OperatorReportResponse>("/reports/operator/daily", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getOperatorReports: (limit = 20) =>
    request<OperatorReportsListResponse>("/reports/operator", undefined, { limit }),
  getOperatorReport: (reportId: string) =>
    request<OperatorReportResponse>(`/reports/operator/${reportId}`),
  checkExecutionReadiness: (payload: ExecutionReadinessRequest) =>
    request<ExecutionReadinessResponse>("/readiness/check", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getExecutionReadinessSnapshots: (limit = 20) =>
    request<ExecutionReadinessSnapshotsResponse>(
      "/readiness/snapshots",
      undefined,
      { limit },
    ),
  getRecentEvents: (params?: {
    limit?: number;
    event_type?: string;
    source?: string;
    correlation_id?: string;
  }) =>
    request<{ events: SystemEventRecord[]; request_id: string; correlation_id: string; timestamp: string }>(
      "/events/recent",
      undefined,
      params,
    ),
};
