use aegis_core::{
    PaperAccount, PaperAccountStatus, PaperEquitySnapshot, PaperFill, PaperPnlSummary,
    PaperPosition, PaperPriceStatus, PaperTradeJournalEntry, PnlCalculationMode, PositionSide,
    PositionStatus,
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaperAccountingConfig {
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub pnl_calculation_mode: PnlCalculationMode,
}

impl Default for PaperAccountingConfig {
    fn default() -> Self {
        Self {
            fee_bps: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            pnl_calculation_mode: PnlCalculationMode::WeightedAverage,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PaperMarkPriceInput {
    pub symbol: String,
    pub mark_price: Option<Decimal>,
    pub priced_at: Option<DateTime<Utc>>,
    pub price_status: PaperPriceStatus,
}

#[derive(Debug, Clone)]
pub struct PaperMarkToMarketResult {
    pub positions: Vec<PaperPosition>,
    pub summary: PaperPnlSummary,
    pub snapshot: PaperEquitySnapshot,
    pub missing_symbols: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FillApplication {
    pub account: PaperAccount,
    pub position: PaperPosition,
    pub fill: PaperFill,
    pub journal_entries: Vec<PaperTradeJournalEntry>,
    pub summary: PaperPnlSummary,
}

pub fn create_default_paper_account_if_missing(
    existing: Option<PaperAccount>,
    name: &str,
    base_currency: &str,
    initial_equity: Decimal,
    now: DateTime<Utc>,
) -> Result<PaperAccount> {
    if let Some(account) = existing {
        return Ok(account);
    }

    Ok(PaperAccount {
        id: Uuid::new_v4(),
        name: name.trim().to_string(),
        base_currency: base_currency.trim().to_string(),
        initial_equity,
        current_equity: initial_equity,
        realized_pnl: Decimal::ZERO,
        unrealized_pnl: Decimal::ZERO,
        status: PaperAccountStatus::Active,
        created_at: now,
        updated_at: now,
    })
}

pub fn apply_paper_order_fill(
    account: &PaperAccount,
    existing_position: Option<&PaperPosition>,
    fill: &PaperFill,
    config: PaperAccountingConfig,
) -> Result<FillApplication> {
    match fill.side {
        PositionSide::Long => open_position_from_buy(account, existing_position, fill, config),
    }
}

pub fn open_position_from_buy(
    account: &PaperAccount,
    existing_position: Option<&PaperPosition>,
    fill: &PaperFill,
    _config: PaperAccountingConfig,
) -> Result<FillApplication> {
    let now = fill.filled_at;
    let quantity = fill.quantity;
    let notional = fill.notional;
    let trade_cost = fill.fee + fill.slippage_cost;
    if quantity <= Decimal::ZERO {
        return Err(anyhow!("paper fill quantity must be greater than zero"));
    }

    let position = if let Some(current) = existing_position {
        let new_quantity = current.quantity + quantity;
        if new_quantity <= Decimal::ZERO {
            return Err(anyhow!("paper position quantity must remain positive"));
        }

        let gross_cost = (current.entry_price * current.quantity) + notional + trade_cost;
        let entry_price = gross_cost / new_quantity;

        PaperPosition {
            id: current.id,
            account_id: current.account_id,
            symbol: current.symbol.clone(),
            side: current.side,
            quantity: new_quantity,
            entry_price,
            mark_price: current.mark_price,
            price_status: current.price_status,
            notional: entry_price * new_quantity,
            realized_pnl: current.realized_pnl,
            unrealized_pnl: compute_unrealized_pnl(
                current.side,
                entry_price,
                current.mark_price,
                new_quantity,
            ),
            status: PositionStatus::Open,
            opened_at: current.opened_at,
            closed_at: None,
            strategy_id: fill.strategy_id.clone(),
            signal_id: fill.signal_id,
            risk_decision_id: fill.risk_decision_id,
            order_id: Some(fill.order_id),
            updated_at: now,
        }
    } else {
        PaperPosition {
            id: fill.position_id.unwrap_or_else(Uuid::new_v4),
            account_id: account.id,
            symbol: fill.symbol.clone(),
            side: fill.side,
            quantity,
            entry_price: (notional + trade_cost) / quantity,
            mark_price: None,
            price_status: PaperPriceStatus::Missing,
            notional: notional + trade_cost,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: now,
            closed_at: None,
            strategy_id: fill.strategy_id.clone(),
            signal_id: fill.signal_id,
            risk_decision_id: fill.risk_decision_id,
            order_id: Some(fill.order_id),
            updated_at: now,
        }
    };

    let summary = rebuild_account_summary(
        account.initial_equity,
        account.realized_pnl - trade_cost,
        position.unrealized_pnl,
    );
    let account = PaperAccount {
        current_equity: summary.equity,
        realized_pnl: summary.realized_pnl,
        unrealized_pnl: summary.unrealized_pnl,
        updated_at: now,
        ..account.clone()
    };

    let journal_entries = vec![
        PaperTradeJournalEntry {
            id: Uuid::new_v4(),
            account_id: account.id,
            position_id: Some(position.id),
            order_id: Some(fill.order_id),
            event_type: "paper.fill.created".to_string(),
            symbol: Some(fill.symbol.clone()),
            pnl: None,
            payload: serde_json::json!({
                "fill_id": fill.id,
                "quantity": fill.quantity,
                "price": fill.price,
                "notional": fill.notional,
                "fee": fill.fee,
                "slippage_cost": fill.slippage_cost,
            }),
            created_at: now,
            correlation_id: fill.correlation_id,
        },
        PaperTradeJournalEntry {
            id: Uuid::new_v4(),
            account_id: account.id,
            position_id: Some(position.id),
            order_id: Some(fill.order_id),
            event_type: if existing_position.is_some() {
                "paper.position.updated".to_string()
            } else {
                "paper.position.opened".to_string()
            },
            symbol: Some(fill.symbol.clone()),
            pnl: Some(position.realized_pnl),
            payload: serde_json::json!({
                "position_id": position.id,
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "status": position.status.as_str(),
            }),
            created_at: now,
            correlation_id: fill.correlation_id,
        },
        PaperTradeJournalEntry {
            id: Uuid::new_v4(),
            account_id: account.id,
            position_id: Some(position.id),
            order_id: Some(fill.order_id),
            event_type: "paper.equity.updated".to_string(),
            symbol: Some(fill.symbol.clone()),
            pnl: Some(summary.equity),
            payload: serde_json::json!({
                "equity": summary.equity,
                "realized_pnl": summary.realized_pnl,
                "unrealized_pnl": summary.unrealized_pnl,
            }),
            created_at: now,
            correlation_id: fill.correlation_id,
        },
    ];

    Ok(FillApplication {
        account,
        position,
        fill: fill.clone(),
        journal_entries,
        summary,
    })
}

pub fn close_position(
    account: &PaperAccount,
    position: &PaperPosition,
    exit_price: Decimal,
    quantity: Decimal,
    closed_at: DateTime<Utc>,
    fees_and_slippage: Decimal,
) -> Result<(PaperAccount, PaperPosition, PaperPnlSummary)> {
    if quantity <= Decimal::ZERO || quantity > position.quantity {
        return Err(anyhow!(
            "close quantity must be positive and no larger than open quantity"
        ));
    }

    let realized_pnl = compute_realized_pnl(
        position.side,
        position.entry_price,
        exit_price,
        quantity,
        fees_and_slippage,
    );
    let remaining_quantity = position.quantity - quantity;
    let updated_position = PaperPosition {
        quantity: remaining_quantity,
        mark_price: Some(exit_price),
        price_status: PaperPriceStatus::Live,
        realized_pnl: position.realized_pnl + realized_pnl,
        unrealized_pnl: if remaining_quantity.is_zero() {
            Decimal::ZERO
        } else {
            compute_unrealized_pnl(
                position.side,
                position.entry_price,
                Some(exit_price),
                remaining_quantity,
            )
        },
        status: if remaining_quantity.is_zero() {
            PositionStatus::Closed
        } else {
            PositionStatus::Open
        },
        closed_at: if remaining_quantity.is_zero() {
            Some(closed_at)
        } else {
            None
        },
        updated_at: closed_at,
        ..position.clone()
    };

    let summary = rebuild_account_summary(
        account.initial_equity,
        account.realized_pnl + realized_pnl,
        updated_position.unrealized_pnl,
    );
    let updated_account = PaperAccount {
        current_equity: summary.equity,
        realized_pnl: summary.realized_pnl,
        unrealized_pnl: summary.unrealized_pnl,
        updated_at: closed_at,
        ..account.clone()
    };

    Ok((updated_account, updated_position, summary))
}

pub fn mark_positions_to_market(
    account: &PaperAccount,
    positions: &[PaperPosition],
    prices: &[PaperMarkPriceInput],
    snapshot_at: DateTime<Utc>,
) -> PaperMarkToMarketResult {
    let mut updated_positions = Vec::with_capacity(positions.len());
    let mut unrealized_pnl = Decimal::ZERO;
    let mut missing_symbols = Vec::new();

    for position in positions {
        let price = prices.iter().find(|item| item.symbol == position.symbol);
        let updated = if let Some(price) = price {
            let unrealized = compute_unrealized_pnl(
                position.side,
                position.entry_price,
                price.mark_price,
                position.quantity,
            );
            if price.mark_price.is_none() {
                missing_symbols.push(position.symbol.clone());
            }
            PaperPosition {
                mark_price: price.mark_price,
                price_status: price.price_status,
                unrealized_pnl: unrealized,
                updated_at: snapshot_at,
                ..position.clone()
            }
        } else {
            missing_symbols.push(position.symbol.clone());
            PaperPosition {
                mark_price: None,
                price_status: PaperPriceStatus::Missing,
                unrealized_pnl: Decimal::ZERO,
                updated_at: snapshot_at,
                ..position.clone()
            }
        };
        unrealized_pnl += updated.unrealized_pnl;
        updated_positions.push(updated);
    }

    let summary =
        rebuild_account_summary(account.initial_equity, account.realized_pnl, unrealized_pnl);
    let drawdown_pct = compute_drawdown(summary.equity, summary.peak_equity);
    let snapshot = PaperEquitySnapshot {
        id: Uuid::new_v4(),
        account_id: account.id,
        equity: summary.equity,
        realized_pnl: summary.realized_pnl,
        unrealized_pnl: summary.unrealized_pnl,
        drawdown_pct,
        snapshot_at,
    };

    PaperMarkToMarketResult {
        positions: updated_positions,
        summary,
        snapshot,
        missing_symbols,
    }
}

pub fn compute_unrealized_pnl(
    side: PositionSide,
    entry_price: Decimal,
    mark_price: Option<Decimal>,
    quantity: Decimal,
) -> Decimal {
    let Some(mark_price) = mark_price else {
        return Decimal::ZERO;
    };

    match side {
        PositionSide::Long => (mark_price - entry_price) * quantity,
    }
}

pub fn compute_realized_pnl(
    side: PositionSide,
    entry_price: Decimal,
    exit_price: Decimal,
    quantity: Decimal,
    fees_and_slippage: Decimal,
) -> Decimal {
    let gross = match side {
        PositionSide::Long => (exit_price - entry_price) * quantity,
    };
    gross - fees_and_slippage
}

pub fn compute_daily_pnl(snapshots: &[PaperEquitySnapshot], day: NaiveDate) -> Option<Decimal> {
    let mut day_points = snapshots
        .iter()
        .filter(|snapshot| snapshot.snapshot_at.date_naive() == day)
        .collect::<Vec<_>>();
    day_points.sort_by_key(|snapshot| snapshot.snapshot_at);

    let first = day_points.first()?;
    let last = day_points.last()?;
    Some(last.equity - first.equity)
}

pub fn compute_drawdown(current_equity: Decimal, peak_equity: Decimal) -> Decimal {
    if peak_equity <= Decimal::ZERO || current_equity >= peak_equity {
        return Decimal::ZERO;
    }
    (peak_equity - current_equity) / peak_equity
}

fn rebuild_account_summary(
    initial_equity: Decimal,
    realized_pnl: Decimal,
    unrealized_pnl: Decimal,
) -> PaperPnlSummary {
    let equity = initial_equity + realized_pnl + unrealized_pnl;
    let peak_equity = if equity > initial_equity {
        equity
    } else {
        initial_equity
    };
    PaperPnlSummary {
        account_id: Uuid::nil(),
        realized_pnl,
        unrealized_pnl,
        equity,
        daily_pnl: Decimal::ZERO,
        drawdown_pct: compute_drawdown(equity, peak_equity),
        price_status: if unrealized_pnl.is_zero() {
            PaperPriceStatus::Missing
        } else {
            PaperPriceStatus::Live
        },
        calculated_at: Utc::now(),
        peak_equity,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_paper_order_fill, compute_unrealized_pnl, create_default_paper_account_if_missing,
        mark_positions_to_market, PaperAccountingConfig, PaperMarkPriceInput,
    };
    use aegis_core::{
        PaperAccount, PaperAccountStatus, PaperFill, PaperPosition, PaperPriceStatus, PositionSide,
        PositionStatus,
    };
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_account() -> PaperAccount {
        let now = Utc.with_ymd_and_hms(2026, 5, 24, 0, 0, 0).unwrap();
        PaperAccount {
            id: Uuid::new_v4(),
            name: "Default Paper".to_string(),
            base_currency: "USDT".to_string(),
            initial_equity: Decimal::new(1_000_000, 0),
            current_equity: Decimal::new(1_000_000, 0),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            status: PaperAccountStatus::Active,
            created_at: now,
            updated_at: now,
        }
    }

    fn sample_fill() -> PaperFill {
        let now = Utc.with_ymd_and_hms(2026, 5, 24, 0, 1, 0).unwrap();
        PaperFill {
            id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            order_id: Uuid::new_v4(),
            position_id: None,
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            price: Decimal::new(100_000, 0),
            quantity: Decimal::new(1, 0),
            notional: Decimal::new(100_000, 0),
            fee: Decimal::ZERO,
            slippage_cost: Decimal::ZERO,
            filled_at: now,
            strategy_id: Some("momentum_v1".to_string()),
            signal_id: Some(Uuid::new_v4()),
            risk_decision_id: Some(Uuid::new_v4()),
            correlation_id: Uuid::new_v4(),
        }
    }

    #[test]
    fn buy_opens_paper_position() {
        let account = sample_account();
        let fill = sample_fill();

        let application =
            apply_paper_order_fill(&account, None, &fill, PaperAccountingConfig::default())
                .expect("fill application should succeed");

        assert_eq!(application.position.symbol, "BTCUSDT");
        assert_eq!(application.position.status, PositionStatus::Open);
        assert_eq!(application.position.quantity, Decimal::ONE);
    }

    #[test]
    fn unrealized_pnl_positive_when_mark_above_entry() {
        let pnl = compute_unrealized_pnl(
            PositionSide::Long,
            Decimal::new(100_000, 0),
            Some(Decimal::new(105_000, 0)),
            Decimal::ONE,
        );

        assert_eq!(pnl, Decimal::new(5_000, 0));
    }

    #[test]
    fn unrealized_pnl_negative_when_mark_below_entry() {
        let pnl = compute_unrealized_pnl(
            PositionSide::Long,
            Decimal::new(100_000, 0),
            Some(Decimal::new(95_000, 0)),
            Decimal::ONE,
        );

        assert_eq!(pnl, Decimal::new(-5_000, 0));
    }

    #[test]
    fn fee_reduces_account_equity() {
        let account = sample_account();
        let mut fill = sample_fill();
        fill.fee = Decimal::new(100, 0);

        let application =
            apply_paper_order_fill(&account, None, &fill, PaperAccountingConfig::default())
                .expect("fill application should succeed");

        assert_eq!(application.account.current_equity, Decimal::new(999_900, 0));
    }

    #[test]
    fn mark_to_market_updates_equity_snapshot() {
        let account = sample_account();
        let now = Utc.with_ymd_and_hms(2026, 5, 24, 0, 1, 0).unwrap();
        let position = PaperPosition {
            id: Uuid::new_v4(),
            account_id: account.id,
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            quantity: Decimal::ONE,
            entry_price: Decimal::new(100_000, 0),
            mark_price: None,
            price_status: PaperPriceStatus::Missing,
            notional: Decimal::new(100_000, 0),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: now,
            closed_at: None,
            strategy_id: None,
            signal_id: None,
            risk_decision_id: None,
            order_id: None,
            updated_at: now,
        };

        let result = mark_positions_to_market(
            &account,
            &[position],
            &[PaperMarkPriceInput {
                symbol: "BTCUSDT".to_string(),
                mark_price: Some(Decimal::new(105_000, 0)),
                priced_at: Some(now),
                price_status: PaperPriceStatus::Live,
            }],
            now,
        );

        assert_eq!(result.snapshot.equity, Decimal::new(1_005_000, 0));
        assert_eq!(result.snapshot.unrealized_pnl, Decimal::new(5_000, 0));
    }

    #[test]
    fn create_default_account_returns_existing_account() {
        let account = sample_account();
        let result = create_default_paper_account_if_missing(
            Some(account.clone()),
            "ignored",
            "USDT",
            Decimal::new(1_000_000, 0),
            account.created_at,
        )
        .expect("existing account should return");

        assert_eq!(result.id, account.id);
    }
}
