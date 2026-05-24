import type {
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
  FeedStatusResponse,
  HealthResponse,
  MarketSymbolsResponse,
  MarketTickResponse,
  OrderResponse,
  OrdersResponse,
  PaperAccountResponse,
  PaperEquityResponse,
  PaperPipelineRequest,
  PaperPipelineResult,
  PaperPnlResponse,
  PaperPositionsResponse,
  PaperTradeJournalResponse,
  RecentSignalsResponse,
  RiskActionResponse,
  RiskDecisionResponse,
  RiskDecisionsResponse,
  RiskStatusResponse,
  StatusResponse,
  StrategyListResponse,
  StrategyStatusResponse,
  StrategyToggleResponse,
  SystemEventRecord,
} from "@/lib/types";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") ??
  "http://localhost:3000";

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

async function request<T>(
  path: string,
  init?: RequestInit,
  query?: Record<string, string | number | undefined>,
): Promise<T> {
  const response = await fetch(withQuery(path, query), {
    ...init,
    headers: {
      "content-type": "application/json",
      ...(init?.headers ?? {}),
    },
    cache: "no-store",
  });

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
  getSystemHealth: () => request<HealthResponse>("/system/health"),
  getSystemStatus: () => request<StatusResponse>("/system/status"),
  getRiskStatus: () => request<RiskStatusResponse>("/risk/status"),
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
  getMarketFeedStatus: () => request<FeedStatusResponse>("/market/feed-status"),
  getStrategyList: () => request<StrategyListResponse>("/strategy/list"),
  getStrategyStatus: (id: string) =>
    request<StrategyStatusResponse>(`/strategy/${id}/status`),
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
  getPaperPositions: (limit = 50) =>
    request<PaperPositionsResponse>("/paper/positions", undefined, { limit }),
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
