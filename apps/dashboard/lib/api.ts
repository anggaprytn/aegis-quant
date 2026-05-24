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
  CandlesResponse,
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
  FeedStatusResponse,
  HealthResponse,
  MarketSymbolsResponse,
  MarketTickResponse,
  OrderResponse,
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
  StrategyListResponse,
  StrategyConfigAuditResponse,
  StrategyConfigUpdateRequest,
  StrategyConfigValidationResponse,
  StrategyConfigVersionsResponse,
  StrategyDryRunResponse,
  StrategyStatusResponse,
  StrategyToggleResponse,
  SystemEventRecord,
  TestnetShadowRunResponse,
  TestnetShadowRunsResponse,
} from "@/lib/types";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") ??
  "http://localhost:3000";
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
