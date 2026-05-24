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
  BacktestRequest,
  BacktestResult,
  BacktestRunAcceptedResponse,
  MarketFeedStatusRecord,
  OrderRecord,
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

export function DashboardApp() {
  const queryClient = useQueryClient();
  const [section, setSection] = useState<SectionId>("command-center");
  const [selectedSymbol, setSelectedSymbol] = useState("BTCUSDT");
  const [selectedStrategyId, setSelectedStrategyId] = useState("momentum_v1");
  const [pipelineStrategyId, setPipelineStrategyId] = useState("momentum_v1");
  const [pipelineSymbol, setPipelineSymbol] = useState("BTCUSDT");
  const [pipelineTimeframe, setPipelineTimeframe] = useState("1m");
  const [killSwitchReason, setKillSwitchReason] = useState("");
  const [resumeReason, setResumeReason] = useState("");
  const [resumeConfirmation, setResumeConfirmation] = useState("");
  const [eventTypeFilter, setEventTypeFilter] = useState("");
  const [eventSourceFilter, setEventSourceFilter] = useState("");
  const [eventSymbolFilter, setEventSymbolFilter] = useState("");
  const [selectedOrderId, setSelectedOrderId] = useState<string | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [backtestForm, setBacktestForm] =
    useState<BacktestRequest>(DEFAULT_BACKTEST_FORM);
  const [lastBacktestResult, setLastBacktestResult] =
    useState<BacktestRunAcceptedResponse | null>(null);

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
  const backtestRunsQuery = useQuery({
    queryKey: ["backtest-runs"],
    queryFn: () => api.getBacktestRuns(20),
    refetchInterval: 15_000,
  });
  const eventsQuery = useQuery({
    queryKey: ["events"],
    queryFn: () => api.getRecentEvents(100),
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

  const selectedStrategyStatusQuery = useQuery({
    queryKey: ["strategy-status", selectedStrategyId],
    queryFn: () => api.getStrategyStatus(selectedStrategyId),
    enabled: Boolean(selectedStrategyId),
  });

  const selectedOrderQuery = useQuery({
    queryKey: ["order", selectedOrderId],
    queryFn: () => api.getOrder(selectedOrderId ?? ""),
    enabled: Boolean(selectedOrderId),
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
    }
  }, [pipelineSymbol, selectedSymbol, symbolsQuery.data?.symbols]);

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
    if (!selectedRunId && backtestRunsQuery.data?.runs[0]) {
      setSelectedRunId(backtestRunsQuery.data.runs[0].run_id);
    }
  }, [backtestRunsQuery.data?.runs, selectedRunId]);

  const refreshOperationalData = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["risk-status"] }),
      queryClient.invalidateQueries({ queryKey: ["orders"] }),
      queryClient.invalidateQueries({ queryKey: ["signals"] }),
      queryClient.invalidateQueries({ queryKey: ["backtest-runs"] }),
      queryClient.invalidateQueries({ queryKey: ["events"] }),
      queryClient.invalidateQueries({ queryKey: ["feed-status"] }),
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

  const runBacktestMutation = useMutation({
    mutationFn: () => api.runBacktest(backtestForm),
    onSuccess: async (result) => {
      setLastBacktestResult(result);
      setSelectedRunId(result.run_id);
      await refreshOperationalData();
    },
  });

  const strategies = strategiesQuery.data?.strategies ?? [];
  const orders = ordersQuery.data?.orders ?? [];
  const events = eventsQuery.data?.events ?? [];
  const recentSignals = signalsQuery.data?.signals ?? [];
  const backtestRuns = backtestRunsQuery.data?.runs ?? [];
  const feeds = feedQuery.data?.feeds ?? [];
  const dataSymbols = symbolsQuery.data?.symbols ?? DEFAULT_SYMBOLS;

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
  const filteredEvents = useMemo(
    () =>
      events.filter((event) => {
        if (eventTypeFilter && !event.event_type.includes(eventTypeFilter)) {
          return false;
        }
        if (eventSourceFilter && !event.source.includes(eventSourceFilter)) {
          return false;
        }
        if (eventSymbolFilter) {
          const payload = JSON.stringify(event.payload ?? {}).toUpperCase();
          if (!payload.includes(eventSymbolFilter.toUpperCase())) {
            return false;
          }
        }
        return true;
      }),
    [eventSourceFilter, eventSymbolFilter, eventTypeFilter, events],
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
                <OrdersTable orders={orders.slice(0, 8)} onSelect={setSelectedOrderId} />
              </Panel>

              <Panel className="xl:col-span-6" title="Recent Backtest Runs">
                <BacktestRunsTable runs={backtestRuns.slice(0, 8)} onSelect={setSelectedRunId} />
              </Panel>

              <Panel className="xl:col-span-6" title="Recent System Events">
                <EventsTable events={events.slice(0, 8)} />
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
                <KeyValue
                  items={[
                    [
                      "Strategy",
                      selectedStrategyStatusQuery.data?.strategy.strategy_id ?? "N/A",
                    ],
                    [
                      "Status",
                      selectedStrategyStatusQuery.data?.strategy.status ?? "N/A",
                    ],
                    [
                      "Mode",
                      selectedStrategyStatusQuery.data?.strategy.mode ?? "N/A",
                    ],
                    [
                      "Timeframe",
                      selectedStrategyStatusQuery.data?.strategy.timeframe ?? "N/A",
                    ],
                    [
                      "Last Evaluated",
                      formatDateTime(
                        selectedStrategyStatusQuery.data?.strategy.last_evaluated_at,
                      ),
                    ],
                    [
                      "Last Reason",
                      selectedStrategyStatusQuery.data?.strategy.last_evaluation_reason ??
                        "N/A",
                    ],
                  ]}
                  loading={selectedStrategyStatusQuery.isLoading}
                  error={getErrorMessage(selectedStrategyStatusQuery.error)}
                />
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
              <Panel className="xl:col-span-8" title="Recent Risk Events">
                <EventsTable events={riskEvents} />
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
                      "Execution State",
                      selectedOrderQuery.data?.order.execution_state ?? "N/A",
                    ],
                    ["Status", selectedOrderQuery.data?.order.status ?? "N/A"],
                    [
                      "Idempotency Key",
                      selectedOrderQuery.data?.order.idempotency_key ?? "N/A",
                    ],
                    [
                      "Signal ID",
                      deriveSignalIdFromCorrelation(
                        selectedOrderQuery.data?.order.correlation_id,
                        recentSignals,
                      ),
                    ],
                    [
                      "Risk Decision ID",
                      selectedOrderQuery.data?.order.risk_decision_id ?? "N/A",
                    ],
                    [
                      "Status Reason",
                      selectedOrderQuery.data?.order.status_reason ?? "N/A",
                    ],
                  ]}
                  loading={selectedOrderQuery.isLoading}
                  error={getErrorMessage(selectedOrderQuery.error)}
                />
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
                    label="Symbol"
                    value={eventSymbolFilter}
                    onChange={setEventSymbolFilter}
                    placeholder="BTCUSDT"
                  />
                </div>
                <EventsTable events={filteredEvents} />
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
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  as?: "input" | "select";
  options?: string[];
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
        const enabled = strategy.status === "enabled";
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
                  {strategy.status}
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

function EventsTable({ events }: { events: SystemEventRecord[] }) {
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

function deriveSignalIdFromCorrelation(correlationId?: string, signals?: Array<{ id: string; correlation_id: string }>) {
  const signal = signals?.find((item) => item.correlation_id === correlationId);
  return signal?.id ?? "Unavailable";
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
