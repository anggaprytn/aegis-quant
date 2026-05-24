"use client";

import {
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

import { api, getErrorMessage } from "@/lib/api";
import type {
  AuthUser,
  BacktestRequest,
  BacktestResult,
  BacktestRunAcceptedResponse,
  CandleBackfillRequest,
  CandleBackfillResult,
  MarketFeedStatusRecord,
  OrderRecord,
  PaperPositionRecord,
  RiskConfig,
  RiskDecisionRecord,
  StrategyConfigUpdateRequest,
  StrategyStatusView,
  SystemEventRecord,
} from "@/lib/types";
import {
  cn,
  formatDateTime,
  formatNumber,
  formatRelativeAge,
  shortenId,
  toTitleCase,
} from "@/lib/utils";

type SectionId =
  | "command-center"
  | "market-data"
  | "strategies"
  | "risk"
  | "orders"
  | "backtests"
  | "events"
  | "settings";

const SECTIONS: Array<{ id: SectionId; label: string }> = [
  { id: "command-center", label: "Command Center" },
  { id: "market-data", label: "Market Data" },
  { id: "strategies", label: "Strategies" },
  { id: "risk", label: "Risk" },
  { id: "orders", label: "Orders" },
  { id: "backtests", label: "Backtests" },
  { id: "events", label: "Logs / Events" },
  { id: "settings", label: "Settings" },
];

const DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"];

const DEFAULT_BACKTEST_FORM: BacktestRequest = {
  strategy_id: "momentum_v1",
  symbol: "BTCUSDT",
  timeframe: "1m",
  start_time: "2026-05-01T00:00:00Z",
  end_time: "2026-05-02T00:00:00Z",
  initial_capital: "1000000",
  fee_bps: "10",
  slippage_bps: "5",
  holding_candles: 3,
};

const DEFAULT_BACKFILL_FORM: CandleBackfillRequest = {
  exchange: "binance",
  symbol: "BTCUSDT",
  interval: "1m",
  start_time: "2026-05-01T00:00:00Z",
  end_time: "2026-05-02T00:00:00Z",
  limit_per_request: 1000,
};

function strategyConfigFormFromStatus(
  strategy?: StrategyStatusView,
): StrategyConfigUpdateRequest {
  return {
    strategy_id: strategy?.strategy_id ?? "momentum_v1",
    enabled: strategy?.enabled ?? true,
    mode: strategy?.mode ?? "paper",
    symbols: strategy?.symbols ?? ["BTCUSDT"],
    timeframe: strategy?.timeframe ?? "1m",
    suggested_notional: strategy?.suggested_notional ?? "100000",
    max_signal_age_ms: strategy?.max_signal_age_ms ?? 5000,
    cooldown_seconds: strategy?.cooldown_seconds ?? 900,
    lookback_candles: strategy?.lookback_candles ?? 3,
    confidence_floor: strategy?.confidence_floor ?? null,
    stop_loss_pct: strategy?.stop_loss_pct ?? null,
    take_profit_pct: strategy?.take_profit_pct ?? null,
    holding_candles: strategy?.holding_candles ?? 3,
    notes: strategy?.notes ?? "",
  };
}

function riskConfigFormFromView(config?: RiskConfig): RiskConfig {
  return {
    max_open_positions: config?.max_open_positions ?? 2,
    max_daily_loss_pct: config?.max_daily_loss_pct ?? "2",
    max_weekly_loss_pct: config?.max_weekly_loss_pct ?? "5",
    max_position_notional: config?.max_position_notional ?? "150000",
    max_slippage_pct: config?.max_slippage_pct ?? "1",
    max_consecutive_losses: config?.max_consecutive_losses ?? 3,
    cooldown_seconds: config?.cooldown_seconds ?? 900,
    max_signal_age_ms: config?.max_signal_age_ms ?? 5000,
    stale_feed_threshold_seconds: config?.stale_feed_threshold_seconds ?? 10,
  };
}

type TelemetrySnapshot = {
  reachable: boolean;
  killSwitchActive?: string;
  openPositions?: string;
  paperEquity?: string;
  maxFeedAgeSeconds?: string;
  raw: string;
};

function readMetricValue(metricsText: string, metricName: string) {
  const line = metricsText
    .split("\n")
    .find((entry) => entry.startsWith(`${metricName} `) && !entry.startsWith("#"));
  if (!line) {
    return undefined;
  }

  const parts = line.trim().split(/\s+/);
  return parts[parts.length - 1];
}

function sumMetricValues(metricsText: string, metricName: string) {
  const lines = metricsText
    .split("\n")
    .filter((entry) => entry.startsWith(`${metricName}{`));
  if (!lines.length) {
    return undefined;
  }

  let total = 0;
  for (const line of lines) {
    const parts = line.trim().split(/\s+/);
    const value = Number(parts[parts.length - 1]);
    if (Number.isFinite(value)) {
      total += value;
    }
  }

  return String(total);
}

function readMaxFeedAgeSeconds(metricsText: string) {
  const lines = metricsText
    .split("\n")
    .filter((entry) => entry.startsWith("aegis_market_feed_last_event_age_seconds{"));
  if (!lines.length) {
    return undefined;
  }

  let maxValue: number | undefined;
  for (const line of lines) {
    const parts = line.trim().split(/\s+/);
    const value = Number(parts[parts.length - 1]);
    if (Number.isFinite(value)) {
      maxValue = maxValue === undefined ? value : Math.max(maxValue, value);
    }
  }

  return maxValue === undefined ? undefined : String(maxValue);
}

export function DashboardApp() {
  const queryClient = useQueryClient();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const authQuery = useQuery({
    queryKey: ["auth-me"],
    queryFn: api.me,
    retry: false,
  });

  useEffect(() => {
    if (authQuery.error) {
      api.setAccessToken(null);
    }
  }, [authQuery.error]);

  const loginMutation = useMutation({
    mutationFn: api.login,
    onSuccess: (response) => {
      api.setAccessToken(response.access_token);
      queryClient.setQueryData(["auth-me"], { user: response.user });
      setPassword("");
    },
  });

  const logoutMutation = useMutation({
    mutationFn: api.logout,
    onSettled: async () => {
      api.setAccessToken(null);
      await queryClient.invalidateQueries({ queryKey: ["auth-me"] });
    },
  });

  if (authQuery.isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-transparent text-slate-100">
        <div className="rounded-2xl border border-border bg-panel/90 px-6 py-4 shadow-panel">
          Authenticating...
        </div>
      </div>
    );
  }

  if (!authQuery.data?.user) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-transparent px-4 text-slate-100">
        <form
          className="w-full max-w-md rounded-3xl border border-border bg-panel/95 p-6 shadow-panel"
          onSubmit={(event) => {
            event.preventDefault();
            loginMutation.mutate({ email, password });
          }}
        >
          <div className="text-xs uppercase tracking-[0.24em] text-muted">Aegis Quant</div>
          <h1 className="mt-3 text-2xl font-semibold">Operator Login</h1>
          <p className="mt-2 text-sm text-slate-300">
            Dashboard access requires an authenticated local operator session.
          </p>
          <div className="mt-6 space-y-4">
            <label className="block text-sm">
              <span className="mb-2 block text-slate-300">Email</span>
              <input
                className="w-full rounded-xl border border-border bg-surface/70 px-3 py-2 outline-none"
                type="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                required
              />
            </label>
            <label className="block text-sm">
              <span className="mb-2 block text-slate-300">Password</span>
              <input
                className="w-full rounded-xl border border-border bg-surface/70 px-3 py-2 outline-none"
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                required
                minLength={12}
              />
            </label>
          </div>
          {loginMutation.error ? (
            <div className="mt-4 rounded-xl border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200">
              {getErrorMessage(loginMutation.error)}
            </div>
          ) : null}
          <button
            className="mt-6 w-full rounded-xl border border-accent bg-accent/15 px-4 py-2 font-medium text-white transition hover:bg-accent/25 disabled:opacity-50"
            type="submit"
            disabled={loginMutation.isPending}
          >
            {loginMutation.isPending ? "Signing in..." : "Sign In"}
          </button>
        </form>
      </div>
    );
  }

  return (
    <AuthenticatedDashboard
      user={authQuery.data.user}
      onLogout={() => logoutMutation.mutate()}
      isLoggingOut={logoutMutation.isPending}
    />
  );
}

function AuthenticatedDashboard({
  user,
  onLogout,
  isLoggingOut,
}: {
  user: AuthUser;
  onLogout: () => void;
  isLoggingOut: boolean;
}) {
  const queryClient = useQueryClient();
  const [section, setSection] = useState<SectionId>("command-center");
  const [selectedSymbol, setSelectedSymbol] = useState("BTCUSDT");
  const [selectedStrategyId, setSelectedStrategyId] = useState("momentum_v1");
  const [pipelineStrategyId, setPipelineStrategyId] = useState("momentum_v1");
  const [pipelineSymbol, setPipelineSymbol] = useState("BTCUSDT");
  const [pipelineTimeframe, setPipelineTimeframe] = useState("1m");
  const [paperPositionStatus, setPaperPositionStatus] = useState("OPEN");
  const [closeTarget, setCloseTarget] = useState<PaperPositionRecord | null>(null);
  const [closeConfirmation, setCloseConfirmation] = useState("");
  const [closeReason, setCloseReason] = useState("manual_operator_exit");
  const [killSwitchReason, setKillSwitchReason] = useState("");
  const [resumeReason, setResumeReason] = useState("");
  const [resumeConfirmation, setResumeConfirmation] = useState("");
  const [eventTypeFilter, setEventTypeFilter] = useState("");
  const [eventSourceFilter, setEventSourceFilter] = useState("");
  const [eventCorrelationFilter, setEventCorrelationFilter] = useState("");
  const [selectedOrderId, setSelectedOrderId] = useState<string | null>(null);
  const [selectedRiskDecisionId, setSelectedRiskDecisionId] = useState<string | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [backtestForm, setBacktestForm] =
    useState<BacktestRequest>(DEFAULT_BACKTEST_FORM);
  const [lastBacktestResult, setLastBacktestResult] =
    useState<BacktestRunAcceptedResponse | null>(null);
  const [backfillForm, setBackfillForm] =
    useState<CandleBackfillRequest>(DEFAULT_BACKFILL_FORM);
  const [selectedBackfillRunId, setSelectedBackfillRunId] = useState<string | null>(null);
  const [lastBackfillResult, setLastBackfillResult] =
    useState<CandleBackfillResult | null>(null);
  const [strategyConfigForm, setStrategyConfigForm] =
    useState<StrategyConfigUpdateRequest>(strategyConfigFormFromStatus());
  const [riskConfigForm, setRiskConfigForm] = useState<RiskConfig>(riskConfigFormFromView());

  const healthQuery = useQuery({
    queryKey: ["system-health"],
    queryFn: api.getSystemHealth,
    refetchInterval: 10_000,
  });
  const statusQuery = useQuery({
    queryKey: ["system-status"],
    queryFn: api.getSystemStatus,
    refetchInterval: 10_000,
  });
  const riskQuery = useQuery({
    queryKey: ["risk-status"],
    queryFn: api.getRiskStatus,
    refetchInterval: 5_000,
  });
  const symbolsQuery = useQuery({
    queryKey: ["market-symbols"],
    queryFn: api.getMarketSymbols,
    refetchInterval: 30_000,
  });
  const feedQuery = useQuery({
    queryKey: ["feed-status"],
    queryFn: api.getMarketFeedStatus,
    refetchInterval: 5_000,
  });
  const metricsQuery = useQuery({
    queryKey: ["metrics-text"],
    queryFn: api.getMetricsText,
    refetchInterval: 15_000,
  });
  const strategiesQuery = useQuery({
    queryKey: ["strategies"],
    queryFn: api.getStrategyList,
    refetchInterval: 10_000,
  });
  const signalsQuery = useQuery({
    queryKey: ["signals", selectedSymbol],
    queryFn: () => api.getRecentSignals(selectedSymbol, 20),
    refetchInterval: 10_000,
  });
  const ordersQuery = useQuery({
    queryKey: ["orders"],
    queryFn: api.getOrders,
    refetchInterval: 10_000,
  });
  const paperAccountQuery = useQuery({
    queryKey: ["paper-account"],
    queryFn: api.getPaperAccount,
    refetchInterval: 10_000,
  });
  const paperPositionsQuery = useQuery({
    queryKey: ["paper-positions", paperPositionStatus],
    queryFn: () => api.getPaperPositions(50, paperPositionStatus),
    refetchInterval: 10_000,
  });
  const paperPnlQuery = useQuery({
    queryKey: ["paper-pnl"],
    queryFn: api.getPaperPnl,
    refetchInterval: 10_000,
  });
  const paperEquityQuery = useQuery({
    queryKey: ["paper-equity"],
    queryFn: () => api.getPaperEquity(50),
    refetchInterval: 15_000,
  });
  const paperJournalQuery = useQuery({
    queryKey: ["paper-journal"],
    queryFn: () => api.getPaperTradeJournal(50),
    refetchInterval: 15_000,
  });
  const riskDecisionsQuery = useQuery({
    queryKey: ["risk-decisions", selectedSymbol],
    queryFn: () => api.getRiskDecisions(selectedSymbol, 50),
    refetchInterval: 10_000,
  });
  const latestRiskDecisionsQuery = useQuery({
    queryKey: ["risk-decisions-latest"],
    queryFn: () => api.getRiskDecisions(undefined, 20),
    refetchInterval: 10_000,
  });
  const riskConfigQuery = useQuery({
    queryKey: ["risk-config"],
    queryFn: api.getRiskConfig,
    refetchInterval: 10_000,
  });
  const riskConfigVersionsQuery = useQuery({
    queryKey: ["risk-config-versions"],
    queryFn: api.getRiskConfigVersions,
    refetchInterval: 10_000,
  });
  const riskConfigAuditQuery = useQuery({
    queryKey: ["risk-config-audit"],
    queryFn: api.getRiskConfigAudit,
    refetchInterval: 10_000,
  });
  const backtestRunsQuery = useQuery({
    queryKey: ["backtest-runs"],
    queryFn: () => api.getBacktestRuns(20),
    refetchInterval: 15_000,
  });
  const eventsQuery = useQuery({
    queryKey: ["events", eventTypeFilter, eventSourceFilter, eventCorrelationFilter],
    queryFn: () =>
      api.getRecentEvents({
        limit: 100,
        event_type: eventTypeFilter || undefined,
        source: eventSourceFilter || undefined,
        correlation_id: eventCorrelationFilter || undefined,
      }),
    refetchInterval: 10_000,
  });

  const tickQueries = useQueries({
    queries: DEFAULT_SYMBOLS.map((symbol) => ({
      queryKey: ["latest-tick", symbol],
      queryFn: () => api.getLatestTick(symbol),
      refetchInterval: 5_000,
    })),
  });

  const candlesQuery = useQuery({
    queryKey: ["candles", selectedSymbol],
    queryFn: () => api.getMarketCandles(selectedSymbol, "1m", 25),
    refetchInterval: 10_000,
  });
  const backfillRunsQuery = useQuery({
    queryKey: ["backfill-runs"],
    queryFn: () => api.getMarketBackfillRuns(20),
    refetchInterval: 15_000,
  });
  const selectedBackfillRunQuery = useQuery({
    queryKey: ["backfill-run", selectedBackfillRunId],
    queryFn: () => api.getMarketBackfillRun(selectedBackfillRunId ?? ""),
    enabled: Boolean(selectedBackfillRunId),
  });

  const selectedStrategyStatusQuery = useQuery({
    queryKey: ["strategy-status", selectedStrategyId],
    queryFn: () => api.getStrategyConfig(selectedStrategyId),
    enabled: Boolean(selectedStrategyId),
  });
  const strategyConfigVersionsQuery = useQuery({
    queryKey: ["strategy-config-versions", selectedStrategyId],
    queryFn: () => api.getStrategyConfigVersions(selectedStrategyId),
    enabled: Boolean(selectedStrategyId),
  });
  const strategyConfigAuditQuery = useQuery({
    queryKey: ["strategy-config-audit", selectedStrategyId],
    queryFn: () => api.getStrategyConfigAudit(selectedStrategyId),
    enabled: Boolean(selectedStrategyId),
  });

  const selectedOrderQuery = useQuery({
    queryKey: ["order", selectedOrderId],
    queryFn: () => api.getOrder(selectedOrderId ?? ""),
    enabled: Boolean(selectedOrderId),
  });

  const selectedRiskDecisionQuery = useQuery({
    queryKey: ["risk-decision", selectedRiskDecisionId],
    queryFn: () => api.getRiskDecision(selectedRiskDecisionId ?? ""),
    enabled: Boolean(selectedRiskDecisionId),
  });

  const selectedRunQuery = useQuery({
    queryKey: ["backtest-run", selectedRunId],
    queryFn: () => api.getBacktestRun(selectedRunId ?? ""),
    enabled: Boolean(selectedRunId),
  });

  const selectedRunTradesQuery = useQuery({
    queryKey: ["backtest-trades", selectedRunId],
    queryFn: () => api.getBacktestTrades(selectedRunId ?? ""),
    enabled: Boolean(selectedRunId),
  });

  const selectedRunEquityQuery = useQuery({
    queryKey: ["backtest-equity", selectedRunId],
    queryFn: () => api.getBacktestEquity(selectedRunId ?? ""),
    enabled: Boolean(selectedRunId),
  });

  useEffect(() => {
    const symbols = symbolsQuery.data?.symbols;
    if (symbols?.length) {
      if (!symbols.includes(selectedSymbol)) {
        setSelectedSymbol(symbols[0]);
      }
      if (!symbols.includes(pipelineSymbol)) {
        setPipelineSymbol(symbols[0]);
      }
      if (!symbols.includes(backfillForm.symbol)) {
        setBackfillForm((current) => ({ ...current, symbol: symbols[0] }));
      }
    }
  }, [backfillForm.symbol, pipelineSymbol, selectedSymbol, symbolsQuery.data?.symbols]);

  useEffect(() => {
    const strategies = strategiesQuery.data?.strategies;
    if (strategies?.length) {
      if (!strategies.some((item) => item.strategy_id === selectedStrategyId)) {
        setSelectedStrategyId(strategies[0].strategy_id);
      }
      if (!strategies.some((item) => item.strategy_id === pipelineStrategyId)) {
        setPipelineStrategyId(strategies[0].strategy_id);
      }
    }
  }, [
    pipelineStrategyId,
    selectedStrategyId,
    strategiesQuery.data?.strategies,
  ]);

  useEffect(() => {
    if (selectedStrategyStatusQuery.data?.strategy) {
      setStrategyConfigForm(
        strategyConfigFormFromStatus(selectedStrategyStatusQuery.data.strategy),
      );
    }
  }, [selectedStrategyStatusQuery.data?.strategy]);

  useEffect(() => {
    if (riskConfigQuery.data?.config) {
      setRiskConfigForm(riskConfigFormFromView(riskConfigQuery.data.config));
    }
  }, [riskConfigQuery.data?.config]);

  useEffect(() => {
    if (!selectedOrderId && ordersQuery.data?.orders[0]) {
      setSelectedOrderId(ordersQuery.data.orders[0].order_id);
    }
  }, [ordersQuery.data?.orders, selectedOrderId]);

  useEffect(() => {
    if (!selectedRiskDecisionId && riskDecisionsQuery.data?.decisions[0]) {
      setSelectedRiskDecisionId(riskDecisionsQuery.data.decisions[0].id);
    }
  }, [riskDecisionsQuery.data?.decisions, selectedRiskDecisionId]);

  useEffect(() => {
    if (!selectedRunId && backtestRunsQuery.data?.runs[0]) {
      setSelectedRunId(backtestRunsQuery.data.runs[0].run_id);
    }
  }, [backtestRunsQuery.data?.runs, selectedRunId]);

  useEffect(() => {
    if (!selectedBackfillRunId && backfillRunsQuery.data?.runs[0]) {
      setSelectedBackfillRunId(backfillRunsQuery.data.runs[0].run_id);
    }
  }, [backfillRunsQuery.data?.runs, selectedBackfillRunId]);

  const refreshOperationalData = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["risk-status"] }),
      queryClient.invalidateQueries({ queryKey: ["risk-config"] }),
      queryClient.invalidateQueries({ queryKey: ["risk-config-versions"] }),
      queryClient.invalidateQueries({ queryKey: ["risk-config-audit"] }),
      queryClient.invalidateQueries({ queryKey: ["risk-decisions"] }),
      queryClient.invalidateQueries({ queryKey: ["orders"] }),
      queryClient.invalidateQueries({ queryKey: ["signals"] }),
      queryClient.invalidateQueries({ queryKey: ["backtest-runs"] }),
      queryClient.invalidateQueries({ queryKey: ["events"] }),
      queryClient.invalidateQueries({ queryKey: ["feed-status"] }),
      queryClient.invalidateQueries({ queryKey: ["backfill-runs"] }),
      queryClient.invalidateQueries({ queryKey: ["backfill-run"] }),
      queryClient.invalidateQueries({ queryKey: ["latest-tick"] }),
      queryClient.invalidateQueries({ queryKey: ["strategy-status"] }),
      queryClient.invalidateQueries({ queryKey: ["strategies"] }),
    ]);
  };

  const killSwitchMutation = useMutation({
    mutationFn: () => api.activateKillSwitch(killSwitchReason || undefined),
    onSuccess: async () => {
      setKillSwitchReason("");
      await refreshOperationalData();
    },
  });

  const resumeMutation = useMutation({
    mutationFn: () =>
      api.resumeTrading(resumeConfirmation, resumeReason || undefined),
    onSuccess: async () => {
      setResumeReason("");
      setResumeConfirmation("");
      await refreshOperationalData();
    },
  });

  const pipelineMutation = useMutation({
    mutationFn: () =>
      api.runPaperPipeline({
        strategy_id: pipelineStrategyId,
        symbol: pipelineSymbol,
        timeframe: pipelineTimeframe,
      }),
    onSuccess: refreshOperationalData,
  });

  const evaluateMutation = useMutation({
    mutationFn: ({ strategyId, symbol }: { strategyId: string; symbol?: string }) =>
      api.evaluateStrategy(strategyId, symbol),
    onSuccess: refreshOperationalData,
  });

  const toggleStrategyMutation = useMutation({
    mutationFn: ({
      strategyId,
      enabled,
    }: {
      strategyId: string;
      enabled: boolean;
    }) =>
      enabled ? api.enableStrategy(strategyId) : api.disableStrategy(strategyId),
    onSuccess: refreshOperationalData,
  });

  const validateStrategyConfigMutation = useMutation({
    mutationFn: () => api.validateStrategyConfig(selectedStrategyId, strategyConfigForm),
  });

  const updateStrategyConfigMutation = useMutation({
    mutationFn: () => api.updateStrategyConfig(selectedStrategyId, strategyConfigForm),
    onSuccess: async () => {
      await refreshOperationalData();
      await queryClient.invalidateQueries({ queryKey: ["strategy-status", selectedStrategyId] });
      await queryClient.invalidateQueries({
        queryKey: ["strategy-config-versions", selectedStrategyId],
      });
      await queryClient.invalidateQueries({
        queryKey: ["strategy-config-audit", selectedStrategyId],
      });
    },
  });

  const strategyDryRunMutation = useMutation({
    mutationFn: () =>
      api.dryRunStrategy(selectedStrategyId, {
        symbol: strategyConfigForm.symbols[0],
        timeframe: strategyConfigForm.timeframe,
        config_override: strategyConfigForm,
      }),
  });

  const validateRiskConfigMutation = useMutation({
    mutationFn: () => api.validateRiskConfig(riskConfigForm),
  });

  const updateRiskConfigMutation = useMutation({
    mutationFn: () => api.updateRiskConfig(riskConfigForm),
    onSuccess: async () => {
      await refreshOperationalData();
      await queryClient.invalidateQueries({ queryKey: ["risk-config"] });
      await queryClient.invalidateQueries({ queryKey: ["risk-config-versions"] });
      await queryClient.invalidateQueries({ queryKey: ["risk-config-audit"] });
    },
  });

  const runBacktestMutation = useMutation({
    mutationFn: () => api.runBacktest(backtestForm),
    onSuccess: async (result) => {
      setLastBacktestResult(result);
      setSelectedRunId(result.run_id);
      await refreshOperationalData();
    },
  });

  const backfillMutation = useMutation({
    mutationFn: () => api.backfillMarketCandles(backfillForm),
    onSuccess: async (result) => {
      setLastBackfillResult(result);
      setSelectedBackfillRunId(result.run_id);
      await refreshOperationalData();
      await queryClient.invalidateQueries({ queryKey: ["candles"] });
    },
  });
  const paperMarkMutation = useMutation({
    mutationFn: api.markPaperToMarket,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["paper-account"] });
      queryClient.invalidateQueries({ queryKey: ["paper-positions"] });
      queryClient.invalidateQueries({ queryKey: ["paper-pnl"] });
      queryClient.invalidateQueries({ queryKey: ["paper-equity"] });
      queryClient.invalidateQueries({ queryKey: ["paper-journal"] });
    },
  });
  const paperCloseMutation = useMutation({
    mutationFn: async () => {
      if (!closeTarget) {
        throw new Error("No paper position selected for simulated close.");
      }
      return api.closePaperPosition(closeTarget.id, {
        confirmation_text: closeConfirmation,
        reason: closeReason,
        close_mode: "MARKET_SIMULATED",
      });
    },
    onSuccess: () => {
      setCloseConfirmation("");
      setCloseTarget(null);
      queryClient.invalidateQueries({ queryKey: ["paper-account"] });
      queryClient.invalidateQueries({ queryKey: ["paper-positions"] });
      queryClient.invalidateQueries({ queryKey: ["paper-pnl"] });
      queryClient.invalidateQueries({ queryKey: ["paper-equity"] });
      queryClient.invalidateQueries({ queryKey: ["paper-journal"] });
      queryClient.invalidateQueries({ queryKey: ["metrics-text"] });
    },
  });

  const strategies = strategiesQuery.data?.strategies ?? [];
  const orders = ordersQuery.data?.orders ?? [];
  const paperAccount = paperAccountQuery.data?.account;
  const paperPositions = paperPositionsQuery.data?.positions ?? [];
  const paperPnl = paperPnlQuery.data?.pnl;
  const riskDecisions = riskDecisionsQuery.data?.decisions ?? [];
  const latestRiskDecisions = latestRiskDecisionsQuery.data?.decisions ?? [];
  const events = eventsQuery.data?.events ?? [];
  const recentSignals = signalsQuery.data?.signals ?? [];
  const backtestRuns = backtestRunsQuery.data?.runs ?? [];
  const feeds = feedQuery.data?.feeds ?? [];
  const dataSymbols = symbolsQuery.data?.symbols ?? DEFAULT_SYMBOLS;
  const telemetrySnapshot = useMemo<TelemetrySnapshot>(
    () => ({
      reachable: Boolean(metricsQuery.data),
      killSwitchActive: metricsQuery.data
        ? readMetricValue(metricsQuery.data, "aegis_kill_switch_active")
        : undefined,
      openPositions: metricsQuery.data
        ? sumMetricValues(metricsQuery.data, "aegis_paper_positions_open")
        : undefined,
      paperEquity: metricsQuery.data
        ? readMetricValue(metricsQuery.data, "aegis_paper_equity")
        : undefined,
      maxFeedAgeSeconds: metricsQuery.data
        ? readMaxFeedAgeSeconds(metricsQuery.data)
        : undefined,
      raw: metricsQuery.data ?? "",
    }),
    [metricsQuery.data],
  );

  const latestTicks = DEFAULT_SYMBOLS.map((symbol, index) => ({
    symbol,
    data: tickQueries[index]?.data?.tick,
    error: tickQueries[index]?.error,
    isLoading: tickQueries[index]?.isLoading,
  }));

  const headerFeedState = summarizeFeedState(feeds);
  const headerDataAge = computeDataAge(feeds, latestTicks.map((item) => item.data?.trade_time));
  const riskEvents = useMemo(
    () =>
      events.filter(
        (event) =>
          event.event_type.startsWith("risk.") ||
          event.event_type.startsWith("system.kill_switch"),
      ),
    [events],
  );
  const latestRiskRejection = useMemo(
    () =>
      latestRiskDecisions.find((decision) => decision.decision === "REJECTED") ?? null,
    [latestRiskDecisions],
  );

  return (
    <div className="min-h-screen bg-transparent text-slate-100">
      <div className="mx-auto flex min-h-screen max-w-[1700px] gap-4 px-3 py-3 lg:px-4">
        <aside className="hidden w-64 shrink-0 rounded-2xl border border-border bg-panel/90 p-4 shadow-panel lg:block">
          <div className="mb-6">
            <div className="text-xs uppercase tracking-[0.24em] text-muted">
              Aegis Quant
            </div>
            <div className="mt-2 text-xl font-semibold">Operational Cockpit</div>
          </div>
          <nav className="space-y-2">
            {SECTIONS.map((item) => (
              <button
                key={item.id}
                className={cn(
                  "flex w-full items-center justify-between rounded-xl border px-3 py-2 text-left text-sm transition",
                  section === item.id
                    ? "border-accent bg-accent/10 text-white"
                    : "border-border bg-surface/50 text-slate-300 hover:border-slate-500 hover:text-white",
                )}
                onClick={() => setSection(item.id)}
              >
                <span>{item.label}</span>
              </button>
            ))}
          </nav>
        </aside>

        <main className="flex min-w-0 flex-1 flex-col gap-4">
          <header className="sticky top-3 z-20 rounded-2xl border border-border bg-panel/95 p-3 shadow-panel backdrop-blur">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3 rounded-xl border border-border bg-surface/50 px-3 py-2">
              <div>
                <div className="text-sm font-medium text-white">{user.email}</div>
                <div className="text-xs uppercase tracking-[0.2em] text-muted">
                  Role {user.role}
                </div>
              </div>
              <button
                className="rounded-xl border border-border bg-surface/70 px-3 py-2 text-sm text-slate-200 transition hover:border-slate-400 hover:text-white disabled:opacity-50"
                onClick={onLogout}
                disabled={isLoggingOut}
              >
                {isLoggingOut ? "Signing out..." : "Logout"}
              </button>
            </div>
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
              <HeaderStat
                label="Mode"
                value={statusQuery.data?.market_mode?.toUpperCase() ?? "UNKNOWN"}
                tone="neutral"
              />
              <HeaderStat
                label="Kill Switch"
                value={riskQuery.data?.kill_switch.enabled ? "ON" : "OFF"}
                tone={riskQuery.data?.kill_switch.enabled ? "danger" : "ok"}
              />
              <HeaderStat
                label="Feed Status"
                value={headerFeedState.label}
                tone={headerFeedState.tone}
              />
              <HeaderStat
                label="Data Age"
                value={headerDataAge}
                tone={headerFeedState.tone}
              />
              <HeaderStat label="Daily PnL" value="N/A" tone="neutral" />
              <HeaderStat
                label="API Health"
                value={healthQuery.data?.status?.toUpperCase() ?? "UNKNOWN"}
                tone={healthQuery.data?.status === "ok" ? "ok" : "danger"}
              />
            </div>
          </header>

          <div className="rounded-2xl border border-border bg-panel/90 p-3 shadow-panel lg:hidden">
            <div className="flex gap-2 overflow-x-auto">
              {SECTIONS.map((item) => (
                <button
                  key={item.id}
                  className={cn(
                    "whitespace-nowrap rounded-lg border px-3 py-2 text-sm",
                    section === item.id
                      ? "border-accent bg-accent/10"
                      : "border-border bg-surface/60",
                  )}
                  onClick={() => setSection(item.id)}
                >
                  {item.label}
                </button>
              ))}
            </div>
          </div>

          {section === "command-center" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-4" title="System Health">
                <KeyValue
                  items={[
                    ["Status", healthQuery.data?.status ?? "N/A"],
                    ["Service", healthQuery.data?.service ?? "N/A"],
                    ["Environment", healthQuery.data?.environment ?? "N/A"],
                    ["Timestamp", formatDateTime(healthQuery.data?.timestamp)],
                  ]}
                  loading={healthQuery.isLoading}
                  error={getErrorMessage(healthQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="System Status">
                <KeyValue
                  items={[
                    ["Mode", statusQuery.data?.market_mode ?? "N/A"],
                    ["Started", formatDateTime(statusQuery.data?.started_at)],
                    [
                      "Database",
                      statusQuery.data?.dependencies.database.status ?? "N/A",
                    ],
                    [
                      "Event Bus",
                      statusQuery.data?.dependencies.event_bus.status ?? "N/A",
                    ],
                    [
                      "Execution",
                      statusQuery.data?.dependencies.exchange_execution.status ?? "N/A",
                    ],
                  ]}
                  loading={statusQuery.isLoading}
                  error={getErrorMessage(statusQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="Kill Switch Status">
                <KeyValue
                  items={[
                    [
                      "State",
                      riskQuery.data?.kill_switch.enabled ? "ACTIVE" : "DISABLED",
                    ],
                    ["Reason", riskQuery.data?.kill_switch.reason ?? "N/A"],
                    [
                      "Updated",
                      formatDateTime(riskQuery.data?.kill_switch.updated_at),
                    ],
                    [
                      "Updated By",
                      riskQuery.data?.kill_switch.updated_by.actor ?? "N/A",
                    ],
                  ]}
                  loading={riskQuery.isLoading}
                  error={getErrorMessage(riskQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-4" title="Feed Status">
                <FeedTable feeds={feeds} loading={feedQuery.isLoading} error={getErrorMessage(feedQuery.error)} />
              </Panel>

              <Panel className="xl:col-span-8" title="Telemetry">
                <KeyValue
                  items={[
                    ["Metrics Reachable", telemetrySnapshot.reachable ? "YES" : "NO"],
                    ["Kill Switch Active", telemetrySnapshot.killSwitchActive ?? "N/A"],
                    ["Open Positions", telemetrySnapshot.openPositions ?? "N/A"],
                    ["Paper Equity", telemetrySnapshot.paperEquity ?? "N/A"],
                    ["Max Feed Age Seconds", telemetrySnapshot.maxFeedAgeSeconds ?? "N/A"],
                  ]}
                  loading={metricsQuery.isLoading}
                  error={getErrorMessage(metricsQuery.error)}
                />
                <pre className="mt-3 max-h-64 overflow-auto rounded-xl border border-border bg-surface/80 p-3 text-xs text-copy/80">
                  {telemetrySnapshot.raw || "# metrics unavailable"}
                </pre>
              </Panel>

              {latestTicks.map((tickCard) => (
                <Panel
                  key={tickCard.symbol}
                  className="xl:col-span-4"
                  title={`Latest ${tickCard.symbol} Tick`}
                >
                  <KeyValue
                    items={[
                      ["Price", formatNumber(tickCard.data?.price)],
                      ["Quantity", formatNumber(tickCard.data?.quantity)],
                      ["Trade Time", formatDateTime(tickCard.data?.trade_time)],
                      ["Age", formatRelativeAge(tickCard.data?.trade_time)],
                    ]}
                    loading={tickCard.isLoading}
                    error={getErrorMessage(tickCard.error)}
                  />
                </Panel>
              ))}

              <Panel className="xl:col-span-6" title="Recent Signals">
                <SignalsTable signals={recentSignals.slice(0, 8)} />
              </Panel>

              <Panel className="xl:col-span-6" title="Recent Orders">
                <OrdersTable
                  orders={orders.slice(0, 8)}
                  onSelect={setSelectedOrderId}
                  selectedId={selectedOrderId}
                />
                <InlineStatus error={getErrorMessage(ordersQuery.error)} />
              </Panel>

              <Panel className="xl:col-span-6" title="Paper Account">
                <KeyValue
                  items={[
                    ["Equity", formatNumber(paperAccount?.current_equity)],
                    ["Realized PnL", formatNumber(paperAccount?.realized_pnl)],
                    ["Unrealized PnL", formatNumber(paperAccount?.unrealized_pnl)],
                    ["Daily PnL", formatNumber(paperPnl?.daily_pnl)],
                    ["Open Positions", String(paperPnl?.open_positions_count ?? 0)],
                    ["Drawdown %", formatNumber(paperPnl?.drawdown_pct)],
                  ]}
                  loading={paperAccountQuery.isLoading || paperPnlQuery.isLoading}
                  error={getErrorMessage(paperAccountQuery.error) ?? getErrorMessage(paperPnlQuery.error)}
                />
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Mark To Market"
                    onClick={() => paperMarkMutation.mutate()}
                    busy={paperMarkMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(paperMarkMutation.error)}
                    success={paperMarkMutation.data ? "Paper equity refreshed" : undefined}
                  />
                </div>
              </Panel>

              <Panel className="xl:col-span-12" title="Open Paper Positions">
                <PaperPositionsTable
                  positions={paperPositions.filter((position) => position.status === "open")}
                  onClose={setCloseTarget}
                />
                <InlineStatus error={getErrorMessage(paperPositionsQuery.error)} />
              </Panel>

              <Panel className="xl:col-span-6" title="Recent Backtest Runs">
                <BacktestRunsTable runs={backtestRuns.slice(0, 8)} onSelect={setSelectedRunId} />
              </Panel>

              <Panel className="xl:col-span-6" title="Latest Risk Rejection">
                <RiskRejectionSummary
                  decision={latestRiskRejection}
                  loading={latestRiskDecisionsQuery.isLoading}
                  error={getErrorMessage(latestRiskDecisionsQuery.error)}
                  onOpen={() => {
                    if (latestRiskRejection) {
                      setSelectedRiskDecisionId(latestRiskRejection.id);
                      setSection("risk");
                    }
                  }}
                />
              </Panel>

              <Panel className="xl:col-span-6" title="Recent System Events">
                <EventsTable
                  events={events.slice(0, 8)}
                  loading={eventsQuery.isLoading}
                  error={getErrorMessage(eventsQuery.error)}
                />
              </Panel>

              <Panel className="xl:col-span-6" title="Paper Pipeline Run">
                <div className="grid gap-3 md:grid-cols-3">
                  <Field
                    label="Strategy"
                    as="select"
                    value={pipelineStrategyId}
                    onChange={setPipelineStrategyId}
                    options={strategies.map((strategy) => strategy.strategy_id)}
                  />
                  <Field
                    label="Symbol"
                    as="select"
                    value={pipelineSymbol}
                    onChange={setPipelineSymbol}
                    options={dataSymbols}
                  />
                  <Field
                    label="Timeframe"
                    value={pipelineTimeframe}
                    onChange={setPipelineTimeframe}
                  />
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Run Paper Pipeline"
                    onClick={() => pipelineMutation.mutate()}
                    busy={pipelineMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(pipelineMutation.error)}
                    success={
                      pipelineMutation.data
                        ? `${pipelineMutation.data.pipeline_decision}: ${pipelineMutation.data.reasons.join(", ") || "no reason"}`
                        : undefined
                    }
                  />
                </div>
              </Panel>

              <Panel className="xl:col-span-6" title="Dangerous Controls">
                <div className="space-y-4">
                  <div className="rounded-xl border border-danger/40 bg-danger/10 p-3">
                    <div className="text-sm font-semibold text-red-200">
                      Activate Kill Switch
                    </div>
                    <div className="mt-1 text-xs text-red-100/80">
                      Immediately stops paper order execution through the normal pipeline.
                    </div>
                    <textarea
                      className="mt-3 h-20 w-full rounded-lg border border-danger/40 bg-surface px-3 py-2 text-sm outline-none"
                      placeholder="Reason"
                      value={killSwitchReason}
                      onChange={(event) => setKillSwitchReason(event.target.value)}
                    />
                    <div className="mt-3 flex items-center gap-3">
                      <ActionButton
                        label="Activate Kill Switch"
                        onClick={() => killSwitchMutation.mutate()}
                        tone="danger"
                        busy={killSwitchMutation.isPending}
                      />
                      <InlineStatus
                        error={getErrorMessage(killSwitchMutation.error)}
                        success={killSwitchMutation.data?.message}
                      />
                    </div>
                  </div>

                  <div className="rounded-xl border border-warning/40 bg-warning/10 p-3">
                    <div className="text-sm font-semibold text-amber-100">
                      Resume Trading
                    </div>
                    <div className="mt-1 text-xs text-amber-50/80">
                      Requires typed confirmation exactly equal to RESUME TRADING.
                    </div>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <Field
                        label="Confirmation"
                        value={resumeConfirmation}
                        onChange={setResumeConfirmation}
                        placeholder="RESUME TRADING"
                      />
                      <Field
                        label="Reason"
                        value={resumeReason}
                        onChange={setResumeReason}
                        placeholder="Operator reason"
                      />
                    </div>
                    <div className="mt-3 flex items-center gap-3">
                      <ActionButton
                        label="Resume Paper Trading"
                        onClick={() => resumeMutation.mutate()}
                        tone="warning"
                        busy={resumeMutation.isPending}
                        disabled={resumeConfirmation !== "RESUME TRADING"}
                      />
                      <InlineStatus
                        error={getErrorMessage(resumeMutation.error)}
                        success={resumeMutation.data?.message}
                      />
                    </div>
                  </div>
                </div>
              </Panel>
            </section>
          )}

          {section === "market-data" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-3" title="Symbols">
                <SimpleList items={dataSymbols} />
              </Panel>
              <Panel className="xl:col-span-4" title="Feed Status">
                <FeedTable feeds={feeds} loading={feedQuery.isLoading} error={getErrorMessage(feedQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-5" title="Latest Ticks">
                <TicksTable ticks={latestTicks} />
              </Panel>
              <Panel className="xl:col-span-12" title="Recent 1m Candles">
                <div className="mb-3 flex max-w-xs">
                  <Field
                    label="Symbol"
                    as="select"
                    value={selectedSymbol}
                    onChange={setSelectedSymbol}
                    options={dataSymbols}
                  />
                </div>
                <CandlesTable candles={candlesQuery.data?.candles ?? []} />
                <InlineStatus error={getErrorMessage(candlesQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-5" title="Historical Backfill">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Exchange"
                    value={backfillForm.exchange ?? "binance"}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, exchange: value }))
                    }
                    disabled
                  />
                  <Field
                    label="Symbol"
                    as="select"
                    value={backfillForm.symbol}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, symbol: value }))
                    }
                    options={dataSymbols}
                  />
                  <Field
                    label="Interval"
                    value={backfillForm.interval}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, interval: value }))
                    }
                  />
                  <Field
                    label="Limit"
                    value={String(backfillForm.limit_per_request ?? 1000)}
                    onChange={(value) =>
                      setBackfillForm((current) => ({
                        ...current,
                        limit_per_request: Number(value) || 1000,
                      }))
                    }
                  />
                  <Field
                    label="Start"
                    value={backfillForm.start_time}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, start_time: value }))
                    }
                  />
                  <Field
                    label="End"
                    value={backfillForm.end_time}
                    onChange={(value) =>
                      setBackfillForm((current) => ({ ...current, end_time: value }))
                    }
                  />
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Run Candle Backfill"
                    onClick={() => backfillMutation.mutate()}
                    busy={backfillMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(backfillMutation.error)}
                    success={lastBackfillResult ? `Completed ${lastBackfillResult.inserted_candles} inserts` : undefined}
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-7" title="Recent Backfill Runs">
                <BackfillRunsTable
                  runs={backfillRunsQuery.data?.runs ?? []}
                  selectedRunId={selectedBackfillRunId}
                  onSelect={setSelectedBackfillRunId}
                />
                <InlineStatus error={getErrorMessage(backfillRunsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-12" title="Selected Backfill Run">
                <KeyValue
                  items={[
                    ["Run ID", selectedBackfillRunQuery.data?.run.run_id ?? "N/A"],
                    ["Status", selectedBackfillRunQuery.data?.run.status ?? "N/A"],
                    ["Exchange", selectedBackfillRunQuery.data?.run.exchange ?? "N/A"],
                    ["Symbol", selectedBackfillRunQuery.data?.run.symbol ?? "N/A"],
                    ["Interval", selectedBackfillRunQuery.data?.run.interval ?? "N/A"],
                    [
                      "Counts",
                      selectedBackfillRunQuery.data
                        ? `${selectedBackfillRunQuery.data.run.inserted_candles} inserted / ${selectedBackfillRunQuery.data.run.updated_candles} updated / ${selectedBackfillRunQuery.data.run.skipped_candles} skipped`
                        : "N/A",
                    ],
                    [
                      "Failure Reason",
                      selectedBackfillRunQuery.data?.run.failed_reason ?? "N/A",
                    ],
                  ]}
                  loading={selectedBackfillRunQuery.isLoading}
                  error={getErrorMessage(selectedBackfillRunQuery.error)}
                />
              </Panel>
            </section>
          )}

          {section === "strategies" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-7" title="Strategy List">
                <StrategiesTable
                  strategies={strategies}
                  onSelect={setSelectedStrategyId}
                  selectedStrategyId={selectedStrategyId}
                  onToggle={(strategy, enabled) =>
                    toggleStrategyMutation.mutate({
                      strategyId: strategy.strategy_id,
                      enabled,
                    })
                  }
                  onEvaluate={(strategy) =>
                    evaluateMutation.mutate({
                      strategyId: strategy.strategy_id,
                      symbol: strategy.symbols[0],
                    })
                  }
                  busyStrategyId={
                    toggleStrategyMutation.variables?.strategyId ??
                    evaluateMutation.variables?.strategyId
                  }
                />
                <InlineStatus
                  error={
                    toggleStrategyMutation.error
                      ? getErrorMessage(toggleStrategyMutation.error)
                      : evaluateMutation.error
                        ? getErrorMessage(evaluateMutation.error)
                        : undefined
                  }
                />
              </Panel>
              <Panel className="xl:col-span-5" title="Selected Strategy Status">
                <div className="space-y-3">
                  <KeyValue
                    items={[
                      [
                        "Strategy",
                        selectedStrategyStatusQuery.data?.strategy.strategy_id ?? "N/A",
                      ],
                      [
                        "Enabled",
                        String(selectedStrategyStatusQuery.data?.strategy.enabled ?? false),
                      ],
                      ["Mode", selectedStrategyStatusQuery.data?.strategy.mode ?? "N/A"],
                      [
                        "Version",
                        String(
                          selectedStrategyStatusQuery.data?.strategy.config_version ?? "N/A",
                        ),
                      ],
                      [
                        "Last Evaluated",
                        formatDateTime(
                          selectedStrategyStatusQuery.data?.strategy.last_evaluated_at,
                        ),
                      ],
                    ]}
                    loading={selectedStrategyStatusQuery.isLoading}
                    error={getErrorMessage(selectedStrategyStatusQuery.error)}
                  />
                  <div className="grid gap-3 md:grid-cols-2">
                    <Field
                      label="Mode"
                      value={strategyConfigForm.mode}
                      as="select"
                      options={["paper", "research", "shadow"]}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({ ...current, mode: value }))
                      }
                    />
                    <Field
                      label="Enabled"
                      value={String(strategyConfigForm.enabled)}
                      as="select"
                      options={["true", "false"]}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          enabled: value === "true",
                        }))
                      }
                    />
                    <Field
                      label="Symbols"
                      value={strategyConfigForm.symbols.join(",")}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          symbols: value.split(",").map((item) => item.trim()).filter(Boolean),
                        }))
                      }
                    />
                    <Field
                      label="Timeframe"
                      value={strategyConfigForm.timeframe}
                      as="select"
                      options={["1m"]}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({ ...current, timeframe: value }))
                      }
                    />
                    <Field
                      label="Suggested Notional"
                      value={strategyConfigForm.suggested_notional}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          suggested_notional: value,
                        }))
                      }
                    />
                    <Field
                      label="Lookback Candles"
                      value={String(strategyConfigForm.lookback_candles)}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          lookback_candles: Number(value) || 0,
                        }))
                      }
                    />
                    <Field
                      label="Max Signal Age"
                      value={String(strategyConfigForm.max_signal_age_ms)}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          max_signal_age_ms: Number(value) || 0,
                        }))
                      }
                    />
                    <Field
                      label="Cooldown Seconds"
                      value={String(strategyConfigForm.cooldown_seconds)}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          cooldown_seconds: Number(value) || 0,
                        }))
                      }
                    />
                    <Field
                      label="Confidence Floor"
                      value={strategyConfigForm.confidence_floor ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          confidence_floor: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Stop Loss %"
                      value={strategyConfigForm.stop_loss_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          stop_loss_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Take Profit %"
                      value={strategyConfigForm.take_profit_pct ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          take_profit_pct: value || null,
                        }))
                      }
                    />
                    <Field
                      label="Holding Candles"
                      value={String(strategyConfigForm.holding_candles ?? "")}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          holding_candles: value ? Number(value) : null,
                        }))
                      }
                    />
                    <Field
                      label="Notes"
                      value={strategyConfigForm.notes ?? ""}
                      onChange={(value) =>
                        setStrategyConfigForm((current) => ({
                          ...current,
                          notes: value,
                        }))
                      }
                    />
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <ActionButton
                      label="Validate"
                      onClick={() => validateStrategyConfigMutation.mutate()}
                      busy={validateStrategyConfigMutation.isPending}
                    />
                    <ActionButton
                      label="Update"
                      onClick={() => updateStrategyConfigMutation.mutate()}
                      busy={updateStrategyConfigMutation.isPending}
                    />
                    <ActionButton
                      label="Dry Run"
                      onClick={() => strategyDryRunMutation.mutate()}
                      busy={strategyDryRunMutation.isPending}
                    />
                  </div>
                  <InlineStatus
                    error={
                      getErrorMessage(validateStrategyConfigMutation.error) ??
                      getErrorMessage(updateStrategyConfigMutation.error) ??
                      getErrorMessage(strategyDryRunMutation.error)
                    }
                    success={
                      validateStrategyConfigMutation.data
                        ? `validation: ${validateStrategyConfigMutation.data.validation.valid ? "valid" : "rejected"}`
                        : strategyDryRunMutation.data
                          ? `dry-run: ${strategyDryRunMutation.data.result.reason}`
                          : updateStrategyConfigMutation.data
                            ? "config updated"
                            : undefined
                    }
                  />
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    {(validateStrategyConfigMutation.data?.validation.issues ?? []).map((issue) => (
                      <div key={`${issue.field}-${issue.code}`}>
                        {issue.severity} {issue.field}: {issue.message}
                      </div>
                    ))}
                  </div>
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Config Versions</div>
                    {(strategyConfigVersionsQuery.data?.versions ?? []).slice(0, 5).map((entry) => (
                      <div key={`${entry.strategy_id}-${entry.version}`}>
                        v{entry.version} {entry.config.mode} enabled={String(entry.config.enabled)}
                      </div>
                    ))}
                  </div>
                  <div className="rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                    <div className="font-medium text-slate-100">Recent Config Audit</div>
                    {(strategyConfigAuditQuery.data?.audit ?? []).slice(0, 5).map((entry) => (
                      <div key={entry.audit_id}>
                        {formatDateTime(entry.created_at)} v{entry.version ?? "-"} issues=
                        {entry.validation_issues.length}
                      </div>
                    ))}
                  </div>
                </div>
              </Panel>
              <Panel className="xl:col-span-12" title="Recent Signals">
                <SignalsTable signals={recentSignals} />
              </Panel>
            </section>
          )}

          {section === "risk" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-4" title="Risk Status">
                <KeyValue
                  items={[
                    ["Status", riskQuery.data?.status ?? "N/A"],
                    ["Market Mode", riskQuery.data?.market_mode ?? "N/A"],
                    [
                      "Paper Allowed",
                      String(riskQuery.data?.paper_trading_allowed ?? false),
                    ],
                    [
                      "Live Allowed",
                      String(riskQuery.data?.live_trading_allowed ?? false),
                    ],
                    [
                      "Kill Switch",
                      riskQuery.data?.kill_switch.enabled ? "ACTIVE" : "DISABLED",
                    ],
                  ]}
                  loading={riskQuery.isLoading}
                  error={getErrorMessage(riskQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-8" title="Risk Config">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Max Open Positions"
                    value={String(riskConfigForm.max_open_positions)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_open_positions: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Max Daily Loss %"
                    value={riskConfigForm.max_daily_loss_pct}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_daily_loss_pct: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Weekly Loss %"
                    value={riskConfigForm.max_weekly_loss_pct}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_weekly_loss_pct: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Position Notional"
                    value={riskConfigForm.max_position_notional}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_position_notional: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Slippage %"
                    value={riskConfigForm.max_slippage_pct}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_slippage_pct: value,
                      }))
                    }
                  />
                  <Field
                    label="Max Consecutive Losses"
                    value={String(riskConfigForm.max_consecutive_losses)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_consecutive_losses: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Cooldown Seconds"
                    value={String(riskConfigForm.cooldown_seconds)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        cooldown_seconds: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Max Signal Age ms"
                    value={String(riskConfigForm.max_signal_age_ms)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        max_signal_age_ms: Number(value),
                      }))
                    }
                  />
                  <Field
                    label="Stale Feed Threshold s"
                    value={String(riskConfigForm.stale_feed_threshold_seconds)}
                    onChange={(value) =>
                      setRiskConfigForm((current) => ({
                        ...current,
                        stale_feed_threshold_seconds: Number(value),
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <ActionButton
                    label="Validate"
                    onClick={() => validateRiskConfigMutation.mutate()}
                    busy={validateRiskConfigMutation.isPending}
                  />
                  <ActionButton
                    label="Update"
                    onClick={() => updateRiskConfigMutation.mutate()}
                    busy={updateRiskConfigMutation.isPending}
                  />
                </div>
                <InlineStatus
                  error={
                    getErrorMessage(validateRiskConfigMutation.error) ??
                    getErrorMessage(updateRiskConfigMutation.error)
                  }
                  success={
                    validateRiskConfigMutation.data
                      ? `validation: ${validateRiskConfigMutation.data.validation.valid ? "valid" : "rejected"}`
                      : updateRiskConfigMutation.data
                        ? "risk config updated"
                        : undefined
                  }
                />
                <div className="mt-3 rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                  <div className="font-medium text-slate-100">Validation Issues</div>
                  {(validateRiskConfigMutation.data?.validation.issues ?? []).map((issue) => (
                    <div key={`${issue.field}-${issue.code}`}>
                      {issue.severity} {issue.field}: {issue.message}
                    </div>
                  ))}
                </div>
                <div className="mt-3 rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                  <div className="font-medium text-slate-100">Config Versions</div>
                  {(riskConfigVersionsQuery.data?.versions ?? []).slice(0, 5).map((entry) => (
                    <div key={`${entry.config_id}-${entry.version}`}>
                      v{entry.version} max_open_positions={entry.config.max_open_positions} max_notional=
                      {entry.config.max_position_notional}
                    </div>
                  ))}
                </div>
                <div className="mt-3 rounded-xl border border-border bg-surface/40 p-3 text-xs text-slate-300">
                  <div className="font-medium text-slate-100">Recent Config Audit</div>
                  {(riskConfigAuditQuery.data?.audit ?? []).slice(0, 5).map((entry) => (
                    <div key={entry.audit_id}>
                      {formatDateTime(entry.created_at)} v{entry.version ?? "-"} issues=
                      {entry.validation_issues.length}
                    </div>
                  ))}
                </div>
              </Panel>
              <Panel className="xl:col-span-8" title="Recent Risk Events">
                <EventsTable
                  events={riskEvents}
                  loading={eventsQuery.isLoading}
                  error={getErrorMessage(eventsQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-8" title="Risk Decisions">
                <div className="mb-3 max-w-xs">
                  <Field
                    label="Symbol"
                    as="select"
                    value={selectedSymbol}
                    onChange={setSelectedSymbol}
                    options={dataSymbols}
                  />
                </div>
                <RiskDecisionsTable
                  decisions={riskDecisions}
                  onSelect={setSelectedRiskDecisionId}
                  selectedId={selectedRiskDecisionId}
                  loading={riskDecisionsQuery.isLoading}
                  error={getErrorMessage(riskDecisionsQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-4" title="Risk Decision Detail">
                <KeyValue
                  items={[
                    ["Decision ID", selectedRiskDecisionQuery.data?.decision.id ?? "N/A"],
                    [
                      "Signal ID",
                      selectedRiskDecisionQuery.data?.decision.signal_id ?? "N/A",
                    ],
                    [
                      "Strategy",
                      selectedRiskDecisionQuery.data?.decision.strategy_id ?? "N/A",
                    ],
                    ["Symbol", selectedRiskDecisionQuery.data?.decision.symbol ?? "N/A"],
                    ["Decision", selectedRiskDecisionQuery.data?.decision.decision ?? "N/A"],
                    [
                      "Approved Notional",
                      selectedRiskDecisionQuery.data?.decision.approved_notional ?? "N/A",
                    ],
                    [
                      "Risk Score",
                      selectedRiskDecisionQuery.data?.decision.risk_score ?? "N/A",
                    ],
                    [
                      "Reasons",
                      selectedRiskDecisionQuery.data?.decision.reasons.join(", ") || "N/A",
                    ],
                    [
                      "Correlation",
                      selectedRiskDecisionQuery.data?.decision.correlation_id ?? "N/A",
                    ],
                    [
                      "Created",
                      formatDateTime(selectedRiskDecisionQuery.data?.decision.created_at),
                    ],
                  ]}
                  loading={selectedRiskDecisionQuery.isLoading}
                  error={getErrorMessage(selectedRiskDecisionQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-6" title="Activate Kill Switch">
                <Field
                  label="Reason"
                  value={killSwitchReason}
                  onChange={setKillSwitchReason}
                  placeholder="Operator reason"
                />
                <div className="mt-3">
                  <ActionButton
                    label="Activate Kill Switch"
                    onClick={() => killSwitchMutation.mutate()}
                    tone="danger"
                    busy={killSwitchMutation.isPending}
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-6" title="Resume Paper Trading">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Confirmation"
                    value={resumeConfirmation}
                    onChange={setResumeConfirmation}
                    placeholder="RESUME TRADING"
                  />
                  <Field
                    label="Reason"
                    value={resumeReason}
                    onChange={setResumeReason}
                    placeholder="Operator reason"
                  />
                </div>
                <div className="mt-3">
                  <ActionButton
                    label="Resume"
                    onClick={() => resumeMutation.mutate()}
                    tone="warning"
                    busy={resumeMutation.isPending}
                    disabled={resumeConfirmation !== "RESUME TRADING"}
                  />
                </div>
              </Panel>
            </section>
          )}

          {section === "orders" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-8" title="Paper Orders">
                <OrdersTable orders={orders} onSelect={setSelectedOrderId} selectedId={selectedOrderId} />
              </Panel>
              <Panel className="xl:col-span-4" title="Order Detail">
                <KeyValue
                  items={[
                    ["Order ID", selectedOrderQuery.data?.order.order_id ?? "N/A"],
                    [
                      "Client Order ID",
                      selectedOrderQuery.data?.order.client_order_id ?? "N/A",
                    ],
                    [
                      "Exchange Order ID",
                      selectedOrderQuery.data?.order.exchange_order_id ?? "N/A",
                    ],
                    [
                      "Signal ID",
                      selectedOrderQuery.data?.order.signal_id ?? "N/A",
                    ],
                    [
                      "Strategy ID",
                      selectedOrderQuery.data?.order.strategy_id ?? "N/A",
                    ],
                    [
                      "Correlation ID",
                      selectedOrderQuery.data?.order.correlation_id ?? "N/A",
                    ],
                    [
                      "Execution State",
                      selectedOrderQuery.data?.order.execution_state ?? "N/A",
                    ],
                    ["Status", selectedOrderQuery.data?.order.status ?? "N/A"],
                    ["Side", selectedOrderQuery.data?.order.side ?? "N/A"],
                    ["Symbol", selectedOrderQuery.data?.order.symbol ?? "N/A"],
                    ["Mode", selectedOrderQuery.data?.order.mode ?? "N/A"],
                    [
                      "Idempotency Key",
                      selectedOrderQuery.data?.order.idempotency_key ?? "N/A",
                    ],
                    [
                      "Requested Notional",
                      selectedOrderQuery.data?.order.requested_notional ?? "N/A",
                    ],
                    ["Quantity", selectedOrderQuery.data?.order.quantity ?? "N/A"],
                    ["Filled Qty", selectedOrderQuery.data?.order.filled_qty ?? "N/A"],
                    [
                      "Avg Fill Price",
                      selectedOrderQuery.data?.order.avg_fill_price ?? "N/A",
                    ],
                    [
                      "Risk Decision ID",
                      selectedOrderQuery.data?.order.risk_decision_id ?? "N/A",
                    ],
                    [
                      "Status Reason",
                      selectedOrderQuery.data?.order.status_reason ?? "N/A",
                    ],
                    [
                      "Created",
                      formatDateTime(selectedOrderQuery.data?.order.created_at),
                    ],
                    [
                      "Updated",
                      formatDateTime(selectedOrderQuery.data?.order.updated_at),
                    ],
                  ]}
                  loading={selectedOrderQuery.isLoading}
                  error={getErrorMessage(selectedOrderQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-8" title="Paper Positions">
                <div className="mb-3 flex items-center gap-3">
                  <select
                    className="rounded-lg border border-border bg-surface px-3 py-2 text-sm text-slate-100"
                    value={paperPositionStatus}
                    onChange={(event) => setPaperPositionStatus(event.target.value)}
                  >
                    {["OPEN", "CLOSED", "ALL"].map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                  <span className="text-xs uppercase tracking-[0.2em] text-muted">
                    Simulated paper positions only
                  </span>
                </div>
                {closeTarget ? (
                  <div className="mb-4 rounded-xl border border-amber-400/40 bg-amber-500/10 p-4">
                    <div className="text-sm font-semibold text-amber-100">
                      Close {closeTarget.symbol} paper position
                    </div>
                    <div className="mt-1 text-xs text-amber-50/80">
                      Type <code>CLOSE {closeTarget.symbol}</code> to submit a simulated market close.
                    </div>
                    <div className="mt-3 grid gap-3 md:grid-cols-[2fr,1fr,auto,auto]">
                      <input
                        className="rounded-lg border border-border bg-surface px-3 py-2 text-sm text-slate-100"
                        value={closeConfirmation}
                        onChange={(event) => setCloseConfirmation(event.target.value)}
                        placeholder={`CLOSE ${closeTarget.symbol}`}
                      />
                      <select
                        className="rounded-lg border border-border bg-surface px-3 py-2 text-sm text-slate-100"
                        value={closeReason}
                        onChange={(event) => setCloseReason(event.target.value)}
                      >
                        {["manual_operator_exit", "risk_operator_exit", "emergency_exit"].map((option) => (
                          <option key={option} value={option}>
                            {option}
                          </option>
                        ))}
                      </select>
                      <ActionButton
                        label="Close Paper Position"
                        onClick={() => paperCloseMutation.mutate()}
                        busy={paperCloseMutation.isPending}
                      />
                      <button
                        className="rounded-lg border border-border px-3 py-2 text-sm text-slate-100"
                        onClick={() => {
                          setCloseTarget(null);
                          setCloseConfirmation("");
                        }}
                        type="button"
                      >
                        Cancel
                      </button>
                    </div>
                    <InlineStatus
                      error={getErrorMessage(paperCloseMutation.error)}
                      success={
                        paperCloseMutation.data
                          ? `Simulated close booked with realized PnL ${paperCloseMutation.data.realized_pnl}`
                          : undefined
                      }
                    />
                  </div>
                ) : null}
                <PaperPositionsTable positions={paperPositions} onClose={setCloseTarget} />
                <InlineStatus error={getErrorMessage(paperPositionsQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-4" title="Paper Journal">
                <SimpleList
                  items={(paperJournalQuery.data?.journal ?? []).slice(0, 10).map((entry) => {
                    const symbol = entry.symbol ? ` ${entry.symbol}` : "";
                    const pnl = entry.pnl ? ` pnl=${entry.pnl}` : "";
                    return `${entry.event_type}${symbol}${pnl}`;
                  })}
                />
                <InlineStatus error={getErrorMessage(paperJournalQuery.error)} />
              </Panel>
            </section>
          )}

          {section === "backtests" && (
            <section className="grid gap-4 xl:grid-cols-12">
              <Panel className="xl:col-span-7" title="Backtest Run Form">
                <div className="grid gap-3 md:grid-cols-3">
                  {(
                    [
                      ["strategy_id", "Strategy ID"],
                      ["symbol", "Symbol"],
                      ["timeframe", "Timeframe"],
                      ["start_time", "Start Time"],
                      ["end_time", "End Time"],
                      ["initial_capital", "Initial Capital"],
                      ["fee_bps", "Fee Bps"],
                      ["slippage_bps", "Slippage Bps"],
                    ] as const
                  ).map(([key, label]) => (
                    <Field
                      key={key}
                      label={label}
                      value={String(backtestForm[key])}
                      onChange={(value) =>
                        setBacktestForm((current) => ({ ...current, [key]: value }))
                      }
                    />
                  ))}
                  <Field
                    label="Holding Candles"
                    value={String(backtestForm.holding_candles ?? 3)}
                    onChange={(value) =>
                      setBacktestForm((current) => ({
                        ...current,
                        holding_candles: Number(value),
                      }))
                    }
                  />
                </div>
                <div className="mt-3 flex items-center gap-3">
                  <ActionButton
                    label="Run Backtest"
                    onClick={() => runBacktestMutation.mutate()}
                    busy={runBacktestMutation.isPending}
                  />
                  <InlineStatus
                    error={getErrorMessage(runBacktestMutation.error)}
                    success={
                      lastBacktestResult
                        ? `Run ${shortenId(lastBacktestResult.run_id)} completed with pnl ${lastBacktestResult.pnl}`
                        : undefined
                    }
                  />
                </div>
              </Panel>
              <Panel className="xl:col-span-5" title="Run Result Summary">
                <KeyValue
                  items={[
                    ["Run ID", lastBacktestResult?.run_id ?? selectedRunQuery.data?.run.run_id ?? "N/A"],
                    ["Status", lastBacktestResult?.status ?? selectedRunQuery.data?.run.status ?? "N/A"],
                    ["PnL", lastBacktestResult?.pnl ?? selectedRunQuery.data?.run.pnl ?? "N/A"],
                    ["PnL %", lastBacktestResult?.pnl_pct ?? selectedRunQuery.data?.run.pnl_pct ?? "N/A"],
                    [
                      "Max Drawdown %",
                      lastBacktestResult?.max_drawdown_pct ??
                        selectedRunQuery.data?.run.max_drawdown_pct ??
                        "N/A",
                    ],
                    [
                      "Win Rate",
                      lastBacktestResult?.win_rate ??
                        selectedRunQuery.data?.run.win_rate ??
                        "N/A",
                    ],
                  ]}
                  loading={selectedRunQuery.isLoading}
                  error={getErrorMessage(selectedRunQuery.error)}
                />
              </Panel>
              <Panel className="xl:col-span-4" title="Recent Backtest Runs">
                <BacktestRunsTable runs={backtestRuns} onSelect={setSelectedRunId} selectedId={selectedRunId} />
              </Panel>
              <Panel className="xl:col-span-4" title="Selected Run Trades">
                <BacktestTradesTable trades={selectedRunTradesQuery.data?.trades ?? []} />
                <InlineStatus error={getErrorMessage(selectedRunTradesQuery.error)} />
              </Panel>
              <Panel className="xl:col-span-4" title="Selected Run Equity">
                <BacktestEquityTable equity={selectedRunEquityQuery.data?.equity ?? []} />
                <InlineStatus error={getErrorMessage(selectedRunEquityQuery.error)} />
              </Panel>
            </section>
          )}

          {section === "events" && (
            <section className="grid gap-4">
              <Panel title="System Events">
                <div className="mb-3 grid gap-3 md:grid-cols-3">
                  <Field
                    label="Event Type"
                    value={eventTypeFilter}
                    onChange={setEventTypeFilter}
                    placeholder="risk."
                  />
                  <Field
                    label="Source"
                    value={eventSourceFilter}
                    onChange={setEventSourceFilter}
                    placeholder="aegis-quant-api"
                  />
                  <Field
                    label="Correlation ID"
                    value={eventCorrelationFilter}
                    onChange={setEventCorrelationFilter}
                    placeholder="2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0"
                  />
                </div>
                <EventsTable
                  events={events}
                  loading={eventsQuery.isLoading}
                  error={getErrorMessage(eventsQuery.error)}
                />
              </Panel>
            </section>
          )}

          {section === "settings" && (
            <section className="grid gap-4">
              <Panel title="Settings Placeholder">
                <div className="text-sm text-slate-300">
                  No mutable settings are exposed in the MVP dashboard. Keep operational controls paper-only.
                </div>
              </Panel>
            </section>
          )}
        </main>
      </div>
    </div>
  );
}

function HeaderStat({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "ok" | "danger" | "warning" | "neutral";
}) {
  return (
    <div className="rounded-xl border border-border bg-surface/70 p-3">
      <div className="text-[11px] uppercase tracking-[0.24em] text-muted">{label}</div>
      <div
        className={cn(
          "mt-2 text-base font-semibold",
          tone === "ok" && "text-emerald-300",
          tone === "danger" && "text-red-300",
          tone === "warning" && "text-amber-200",
          tone === "neutral" && "text-slate-100",
        )}
      >
        {value}
      </div>
    </div>
  );
}

function Panel({
  title,
  children,
  className,
}: {
  title: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("rounded-2xl border border-border bg-panel/90 p-4 shadow-panel", className)}>
      <div className="mb-3 text-sm font-semibold uppercase tracking-[0.2em] text-slate-200">
        {title}
      </div>
      {children}
    </div>
  );
}

function KeyValue({
  items,
  loading,
  error,
}: {
  items: Array<[string, string]>;
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading..." />;
  }

  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }

  return (
    <div className="space-y-2">
      {items.map(([label, value]) => (
        <div
          key={label}
          className="flex items-start justify-between gap-4 rounded-lg border border-border bg-surface/60 px-3 py-2"
        >
          <div className="text-xs uppercase tracking-[0.14em] text-muted">{label}</div>
          <div className="text-right text-sm text-slate-100">{value}</div>
        </div>
      ))}
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  placeholder,
  as,
  options,
  disabled,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  as?: "input" | "select";
  options?: string[];
  disabled?: boolean;
}) {
  const commonClassName =
    "mt-1 w-full rounded-lg border border-border bg-surface px-3 py-2 text-sm text-slate-100 outline-none transition focus:border-accent";

  return (
    <label className="block text-sm">
      <span className="text-xs uppercase tracking-[0.18em] text-muted">{label}</span>
      {as === "select" ? (
        <select
          className={commonClassName}
          value={value}
          disabled={disabled}
          onChange={(event) => onChange(event.target.value)}
        >
          {(options ?? []).map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      ) : (
        <input
          className={commonClassName}
          value={value}
          disabled={disabled}
          placeholder={placeholder}
          onChange={(event) => onChange(event.target.value)}
        />
      )}
    </label>
  );
}

function ActionButton({
  label,
  onClick,
  busy,
  tone = "ok",
  disabled,
}: {
  label: string;
  onClick: () => void;
  busy?: boolean;
  tone?: "ok" | "danger" | "warning";
  disabled?: boolean;
}) {
  return (
    <button
      className={cn(
        "rounded-lg border px-4 py-2 text-sm font-medium transition",
        tone === "ok" && "border-accent/40 bg-accent/10 text-emerald-200",
        tone === "danger" && "border-danger/40 bg-danger/10 text-red-200",
        tone === "warning" && "border-warning/40 bg-warning/10 text-amber-100",
        "disabled:cursor-not-allowed disabled:opacity-50",
      )}
      onClick={onClick}
      disabled={busy || disabled}
    >
      {busy ? "Working..." : label}
    </button>
  );
}

function InlineStatus({
  error,
  success,
}: {
  error?: string;
  success?: string;
}) {
  if (error && error !== "Unknown error") {
    return <div className="text-sm text-red-300">{error}</div>;
  }

  if (success) {
    return <div className="text-sm text-emerald-300">{success}</div>;
  }

  return null;
}

function EmptyState({ label, tone = "neutral" }: { label: string; tone?: "neutral" | "danger" }) {
  return (
    <div
      className={cn(
        "rounded-xl border border-dashed px-3 py-6 text-center text-sm",
        tone === "neutral" && "border-border text-slate-300",
        tone === "danger" && "border-danger/40 text-red-300",
      )}
    >
      {label}
    </div>
  );
}

function SimpleList({ items }: { items: string[] }) {
  if (!items.length) {
    return <EmptyState label="No symbols." />;
  }

  return (
    <div className="space-y-2">
      {items.map((item) => (
        <div key={item} className="rounded-lg border border-border bg-surface/60 px-3 py-2 text-sm">
          {item}
        </div>
      ))}
    </div>
  );
}

function FeedTable({
  feeds,
  loading,
  error,
}: {
  feeds: MarketFeedStatusRecord[];
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading feed status..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!feeds.length) {
    return <EmptyState label="No feed status rows." />;
  }

  return (
    <Table
      headers={["Symbol", "Status", "Freshness", "Last Event", "Reconnects"]}
      rows={feeds.map((feed) => [
        feed.symbol,
        badge(feed.status),
        badge(feed.freshness_status),
        formatRelativeAge(feed.last_event_at),
        String(feed.reconnect_count),
      ])}
    />
  );
}

function TicksTable({
  ticks,
}: {
  ticks: Array<{
    symbol: string;
    data?: { price: string; quantity: string; trade_time: string };
    error: unknown;
    isLoading: boolean;
  }>;
}) {
  return (
    <Table
      headers={["Symbol", "Price", "Qty", "Age"]}
      rows={ticks.map((tick) => [
        tick.symbol,
        tick.isLoading ? "Loading..." : formatNumber(tick.data?.price),
        tick.isLoading ? "Loading..." : formatNumber(tick.data?.quantity),
        tick.error ? getErrorMessage(tick.error) : formatRelativeAge(tick.data?.trade_time),
      ])}
    />
  );
}

function CandlesTable({
  candles,
}: {
  candles: Array<{
    open_time: string;
    close_time: string;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
    trade_count: number;
    is_closed: boolean;
  }>;
}) {
  if (!candles.length) {
    return <EmptyState label="No candles found." />;
  }

  return (
    <Table
      headers={["Open Time", "Close", "High", "Low", "Volume", "Trades", "State"]}
      rows={candles.map((candle) => [
        formatDateTime(candle.open_time),
        formatNumber(candle.close),
        formatNumber(candle.high),
        formatNumber(candle.low),
        formatNumber(candle.volume),
        String(candle.trade_count),
        candle.is_closed ? "closed" : "open",
      ])}
    />
  );
}

function BackfillRunsTable({
  runs,
  selectedRunId,
  onSelect,
}: {
  runs: CandleBackfillResult[];
  selectedRunId: string | null;
  onSelect: (runId: string) => void;
}) {
  if (!runs.length) {
    return <EmptyState label="No backfill runs found." />;
  }

  return (
    <div className="space-y-2">
      {runs.map((run) => (
        <button
          key={run.run_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left",
            run.run_id === selectedRunId
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60",
          )}
          onClick={() => onSelect(run.run_id)}
        >
          <div className="flex flex-col gap-2 xl:flex-row xl:items-center xl:justify-between">
            <div>
              <div className="font-medium">
                {run.symbol} {run.interval}
              </div>
              <div className="mt-1 text-xs text-muted">
                {formatDateTime(run.created_at)} | {run.status}
              </div>
            </div>
            <div className="text-xs text-muted">
              {run.inserted_candles} inserted / {run.updated_candles} updated /{" "}
              {run.skipped_candles} skipped
            </div>
          </div>
          {run.failed_reason ? (
            <div className="mt-2 text-xs text-rose-200">{run.failed_reason}</div>
          ) : null}
        </button>
      ))}
    </div>
  );
}

function StrategiesTable({
  strategies,
  selectedStrategyId,
  onSelect,
  onToggle,
  onEvaluate,
  busyStrategyId,
}: {
  strategies: StrategyStatusView[];
  selectedStrategyId: string;
  onSelect: (strategyId: string) => void;
  onToggle: (strategy: StrategyStatusView, enabled: boolean) => void;
  onEvaluate: (strategy: StrategyStatusView) => void;
  busyStrategyId?: string;
}) {
  if (!strategies.length) {
    return <EmptyState label="No strategies found." />;
  }

  return (
    <div className="space-y-2">
      {strategies.map((strategy) => {
        const enabled = strategy.enabled;
        return (
          <div
            key={strategy.strategy_id}
            className={cn(
              "rounded-xl border p-3",
              strategy.strategy_id === selectedStrategyId
                ? "border-accent bg-accent/5"
                : "border-border bg-surface/60",
            )}
          >
            <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
              <button className="text-left" onClick={() => onSelect(strategy.strategy_id)}>
                <div className="font-medium">{strategy.strategy_id}</div>
                <div className="mt-1 text-xs text-muted">
                  {strategy.symbols.join(", ")} | {strategy.timeframe} | last eval{" "}
                  {formatRelativeAge(strategy.last_evaluated_at)}
                </div>
              </button>
              <div className="flex flex-wrap items-center gap-2">
                <span className="rounded-full border border-border px-2 py-1 text-xs">
                  {enabled ? "enabled" : "disabled"}
                </span>
                <ActionButton
                  label={enabled ? "Disable" : "Enable"}
                  onClick={() => onToggle(strategy, !enabled)}
                  tone={enabled ? "warning" : "ok"}
                  busy={busyStrategyId === strategy.strategy_id}
                />
                <ActionButton
                  label="Evaluate"
                  onClick={() => onEvaluate(strategy)}
                  busy={busyStrategyId === strategy.strategy_id}
                />
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function SignalsTable({
  signals,
}: {
  signals: Array<{
    id: string;
    strategy_id: string;
    symbol: string;
    side: string;
    confidence: string;
    reason: string;
    created_at: string;
  }>;
}) {
  if (!signals.length) {
    return <EmptyState label="No signals found." />;
  }

  return (
    <Table
      headers={["Signal ID", "Strategy", "Symbol", "Side", "Confidence", "Reason", "Created"]}
      rows={signals.map((signal) => [
        mono(shortenId(signal.id)),
        signal.strategy_id,
        signal.symbol,
        signal.side,
        signal.confidence,
        signal.reason,
        formatRelativeAge(signal.created_at),
      ])}
    />
  );
}

function OrdersTable({
  orders,
  onSelect,
  selectedId,
}: {
  orders: OrderRecord[];
  onSelect: (orderId: string) => void;
  selectedId?: string | null;
}) {
  if (!orders.length) {
    return <EmptyState label="No paper orders." />;
  }

  return (
    <div className="space-y-2">
      {orders.map((order) => (
        <button
          key={order.order_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left transition",
            selectedId === order.order_id
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60 hover:border-slate-500",
          )}
          onClick={() => onSelect(order.order_id)}
        >
          <div className="grid gap-2 md:grid-cols-6">
            <div className="font-mono text-xs">{shortenId(order.order_id)}</div>
            <div>{order.symbol}</div>
            <div>{order.side}</div>
            <div>{order.execution_state}</div>
            <div>{order.status}</div>
            <div className="font-mono text-xs">{shortenId(order.idempotency_key)}</div>
          </div>
        </button>
      ))}
    </div>
  );
}

function PaperPositionsTable({
  positions,
  onClose,
}: {
  positions: PaperPositionRecord[];
  onClose?: (position: PaperPositionRecord) => void;
}) {
  if (!positions.length) {
    return <EmptyState label="No paper positions." />;
  }

  return (
    <Table
      headers={[
        "Symbol",
        "Side",
        "Quantity",
        "Entry",
        "Mark",
        "Unrealized",
        "Realized",
        "Status",
        "Strategy",
        "Signal",
        "Action",
      ]}
      rows={positions.map((position) => [
        position.symbol,
        position.side,
        position.quantity,
        position.entry_price,
        position.mark_price ?? position.price_status,
        position.unrealized_pnl,
        position.realized_pnl,
        position.status,
        position.strategy_id ?? "N/A",
        position.signal_id ? shortenId(position.signal_id) : "N/A",
        position.status === "open" && onClose ? (
          <button
            className="rounded-md border border-amber-400/40 px-2 py-1 text-xs uppercase tracking-[0.2em] text-amber-100"
            onClick={() => onClose(position)}
            type="button"
          >
            Sim Close
          </button>
        ) : (
          "Closed"
        ),
      ])}
    />
  );
}

function BacktestRunsTable({
  runs,
  onSelect,
  selectedId,
}: {
  runs: BacktestResult[];
  onSelect: (runId: string) => void;
  selectedId?: string | null;
}) {
  if (!runs.length) {
    return <EmptyState label="No backtest runs." />;
  }

  return (
    <div className="space-y-2">
      {runs.map((run) => (
        <button
          key={run.run_id}
          className={cn(
            "w-full rounded-xl border p-3 text-left transition",
            selectedId === run.run_id
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60 hover:border-slate-500",
          )}
          onClick={() => onSelect(run.run_id)}
        >
          <div className="grid gap-2 md:grid-cols-5">
            <div className="font-mono text-xs">{shortenId(run.run_id)}</div>
            <div>{run.strategy_id}</div>
            <div>{run.symbol}</div>
            <div>{run.status}</div>
            <div>PnL {run.pnl}</div>
          </div>
        </button>
      ))}
    </div>
  );
}

function BacktestTradesTable({
  trades,
}: {
  trades: Array<{
    id: string;
    side: string;
    entry_time: string;
    exit_time: string | null;
    notional: string;
    realized_pnl: string;
    reason: string;
  }>;
}) {
  if (!trades.length) {
    return <EmptyState label="No backtest trades." />;
  }

  return (
    <Table
      headers={["Trade", "Side", "Entry", "Exit", "Notional", "PnL", "Reason"]}
      rows={trades.map((trade) => [
        mono(shortenId(trade.id)),
        trade.side,
        formatDateTime(trade.entry_time),
        formatDateTime(trade.exit_time),
        trade.notional,
        trade.realized_pnl,
        trade.reason,
      ])}
    />
  );
}

function BacktestEquityTable({
  equity,
}: {
  equity: Array<{
    id: string;
    timestamp: string;
    equity: string;
    drawdown_pct: string;
  }>;
}) {
  if (!equity.length) {
    return <EmptyState label="No equity points." />;
  }

  return (
    <Table
      headers={["Point", "Timestamp", "Equity", "Drawdown %"]}
      rows={equity.map((point) => [
        mono(shortenId(point.id)),
        formatDateTime(point.timestamp),
        point.equity,
        point.drawdown_pct,
      ])}
    />
  );
}

function RiskDecisionsTable({
  decisions,
  onSelect,
  selectedId,
  loading,
  error,
}: {
  decisions: RiskDecisionRecord[];
  onSelect: (decisionId: string) => void;
  selectedId?: string | null;
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading risk decisions..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!decisions.length) {
    return <EmptyState label="No risk decisions found for this filter." />;
  }

  return (
    <div className="space-y-2">
      {decisions.map((decision) => (
        <button
          key={decision.id}
          className={cn(
            "w-full rounded-xl border p-3 text-left transition",
            selectedId === decision.id
              ? "border-accent bg-accent/5"
              : "border-border bg-surface/60 hover:border-slate-500",
          )}
          onClick={() => onSelect(decision.id)}
        >
          <div className="grid gap-2 md:grid-cols-6">
            <div className="font-mono text-xs">{shortenId(decision.id)}</div>
            <div>{decision.symbol ?? "N/A"}</div>
            <div>{decision.strategy_id ?? "N/A"}</div>
            <div>{decision.decision}</div>
            <div>{decision.risk_score ?? "N/A"}</div>
            <div>{formatRelativeAge(decision.created_at)}</div>
          </div>
          <div className="mt-2 text-xs text-slate-300">
            {decision.reasons.join(", ") || "No reasons recorded."}
          </div>
        </button>
      ))}
    </div>
  );
}

function RiskRejectionSummary({
  decision,
  loading,
  error,
  onOpen,
}: {
  decision: RiskDecisionRecord | null;
  loading?: boolean;
  error?: string;
  onOpen: () => void;
}) {
  if (loading) {
    return <EmptyState label="Loading latest rejection..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!decision) {
    return <EmptyState label="No recent risk rejection." />;
  }

  return (
    <div className="space-y-3">
      <KeyValue
        items={[
          ["Decision ID", decision.id],
          ["Symbol", decision.symbol ?? "N/A"],
          ["Strategy", decision.strategy_id ?? "N/A"],
          ["Risk Score", decision.risk_score ?? "N/A"],
          ["Created", formatDateTime(decision.created_at)],
          ["Reasons", decision.reasons.join(", ") || "N/A"],
        ]}
      />
      <ActionButton label="Open Risk Screen" onClick={onOpen} tone="warning" />
    </div>
  );
}

function EventsTable({
  events,
  loading,
  error,
}: {
  events: SystemEventRecord[];
  loading?: boolean;
  error?: string;
}) {
  if (loading) {
    return <EmptyState label="Loading events..." />;
  }
  if (error && error !== "Unknown error") {
    return <EmptyState label={error} tone="danger" />;
  }
  if (!events.length) {
    return <EmptyState label="No events available." />;
  }

  return (
    <Table
      headers={["Type", "Source", "Occurred", "Correlation", "Payload"]}
      rows={events.map((event) => [
        event.event_type,
        event.source,
        formatRelativeAge(event.occurred_at),
        mono(shortenId(event.correlation_id)),
        trimJson(event.payload),
      ])}
    />
  );
}

function Table({
  headers,
  rows,
}: {
  headers: string[];
  rows: Array<Array<React.ReactNode>>;
}) {
  return (
    <div className="overflow-auto rounded-xl border border-border">
      <table className="min-w-full divide-y divide-border text-sm">
        <thead className="bg-surface/90">
          <tr>
            {headers.map((header) => (
              <th
                key={header}
                className="px-3 py-2 text-left text-[11px] uppercase tracking-[0.2em] text-muted"
              >
                {header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border bg-surface/40">
          {rows.map((row, index) => (
            <tr key={`row-${index}`}>
              {row.map((cell, cellIndex) => (
                <td key={`cell-${index}-${cellIndex}`} className="px-3 py-2 align-top text-slate-100">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function summarizeFeedState(feeds: MarketFeedStatusRecord[]) {
  if (!feeds.length) {
    return { label: "UNKNOWN", tone: "danger" as const };
  }

  if (feeds.some((feed) => feed.status === "error" || feed.status === "disconnected")) {
    return { label: "DEGRADED", tone: "danger" as const };
  }
  if (feeds.some((feed) => feed.freshness_status === "stale" || feed.status === "stale")) {
    return { label: "STALE", tone: "warning" as const };
  }
  return { label: "OK", tone: "ok" as const };
}

function computeDataAge(feeds: MarketFeedStatusRecord[], tickTimes: Array<string | undefined>) {
  const timestamps = [
    ...feeds.map((feed) => feed.last_event_at).filter(Boolean),
    ...tickTimes.filter(Boolean),
  ] as string[];
  if (!timestamps.length) {
    return "unknown";
  }
  return formatRelativeAge(
    timestamps.sort((left, right) => new Date(right).getTime() - new Date(left).getTime())[0],
  );
}

function trimJson(payload: unknown) {
  const text = JSON.stringify(payload ?? {});
  if (text.length <= 72) {
    return text;
  }
  return `${text.slice(0, 72)}...`;
}

function badge(value: string) {
  return toTitleCase(value);
}

function mono(value: string) {
  return <span className="font-mono text-xs">{value}</span>;
}
