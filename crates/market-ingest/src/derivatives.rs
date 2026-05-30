use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db::{DerivativesFundingRateInput, DerivativesOpenInterestInput, DerivativesPositioningInput};
use reqwest::{Client, StatusCode, Url};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{str::FromStr, time::Duration as StdDuration};
use tokio::time::sleep;

const FUNDING_LIMIT: u16 = 1000;
const DERIVATIVES_DATA_LIMIT: u16 = 500;

#[derive(Debug, Clone)]
pub struct BinanceUsdMFuturesPublicClient {
    base_url: String,
    http: Client,
}

impl BinanceUsdMFuturesPublicClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: Client::new(),
        }
    }

    pub fn production() -> Self {
        Self::new("https://fapi.binance.com")
    }

    pub async fn fetch_funding_rate_history(
        &self,
        symbol: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<DerivativesFundingRateInput>> {
        if end_time <= start_time {
            anyhow::bail!("end_time must be after start_time");
        }
        let symbol = normalized_symbol(symbol);
        let mut cursor = start_time;
        let mut rows = Vec::new();

        while cursor <= end_time {
            let raw = self
                .get_json(
                    "/fapi/v1/fundingRate",
                    &[
                        ("symbol", symbol.as_str().to_string()),
                        ("startTime", cursor.timestamp_millis().to_string()),
                        ("endTime", end_time.timestamp_millis().to_string()),
                        ("limit", FUNDING_LIMIT.to_string()),
                    ],
                )
                .await?;
            let page = serde_json::from_value::<Vec<BinanceFundingRateRow>>(raw.clone())
                .context("failed to parse Binance funding response")?;
            if page.is_empty() {
                break;
            }
            let fetched_at = Utc::now();
            let mut max_time = cursor;
            for item in page {
                let funding_time = millis_to_utc(item.funding_time)?;
                if funding_time < start_time || funding_time > end_time {
                    continue;
                }
                if funding_time > max_time {
                    max_time = funding_time;
                }
                rows.push(item.into_input(fetched_at)?);
            }
            let next_cursor = max_time + Duration::milliseconds(1);
            if next_cursor <= cursor {
                break;
            }
            cursor = next_cursor;
            sleep(StdDuration::from_millis(80)).await;
        }

        rows.sort_by_key(|row| row.funding_time);
        rows.dedup_by(|left, right| {
            left.exchange == right.exchange
                && left.symbol == right.symbol
                && left.funding_time == right.funding_time
        });
        Ok(rows)
    }

    pub async fn fetch_current_open_interest(
        &self,
        symbol: &str,
    ) -> Result<DerivativesOpenInterestInput> {
        let symbol = normalized_symbol(symbol);
        let raw = self
            .get_json(
                "/fapi/v1/openInterest",
                &[("symbol", symbol.as_str().to_string())],
            )
            .await?;
        let row = serde_json::from_value::<BinanceCurrentOpenInterestRow>(raw.clone())
            .context("failed to parse Binance current open-interest response")?;
        row.into_input("current", Utc::now())
    }

    pub async fn fetch_open_interest_history(
        &self,
        symbol: &str,
        period: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<DerivativesOpenInterestInput>> {
        let symbol = normalized_symbol(symbol);
        let raw = self
            .get_json(
                "/futures/data/openInterestHist",
                &[
                    ("symbol", symbol.as_str().to_string()),
                    ("period", period.to_string()),
                    ("startTime", start_time.timestamp_millis().to_string()),
                    ("endTime", end_time.timestamp_millis().to_string()),
                    ("limit", DERIVATIVES_DATA_LIMIT.to_string()),
                ],
            )
            .await?;
        let page = serde_json::from_value::<Vec<BinanceOpenInterestHistoryRow>>(raw)
            .context("failed to parse Binance open-interest history response")?;
        let fetched_at = Utc::now();
        page.into_iter()
            .map(|row| row.into_input(period, fetched_at))
            .collect()
    }

    pub async fn fetch_global_long_short_ratio(
        &self,
        symbol: &str,
        period: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<DerivativesPositioningInput>> {
        self.fetch_positioning(
            "/futures/data/globalLongShortAccountRatio",
            symbol,
            "global-long-short",
            period,
            start_time,
            end_time,
        )
        .await
    }

    pub async fn fetch_taker_buy_sell_volume(
        &self,
        symbol: &str,
        period: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<DerivativesPositioningInput>> {
        self.fetch_positioning(
            "/futures/data/takerlongshortRatio",
            symbol,
            "taker-buy-sell",
            period,
            start_time,
            end_time,
        )
        .await
    }

    async fn fetch_positioning(
        &self,
        endpoint: &str,
        symbol: &str,
        metric: &str,
        period: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<DerivativesPositioningInput>> {
        let symbol = normalized_symbol(symbol);
        let raw = self
            .get_json(
                endpoint,
                &[
                    ("symbol", symbol.as_str().to_string()),
                    ("period", period.to_string()),
                    ("startTime", start_time.timestamp_millis().to_string()),
                    ("endTime", end_time.timestamp_millis().to_string()),
                    ("limit", DERIVATIVES_DATA_LIMIT.to_string()),
                ],
            )
            .await?;
        let fetched_at = Utc::now();
        match metric {
            "global-long-short" => serde_json::from_value::<Vec<BinanceLongShortRatioRow>>(raw)
                .context("failed to parse Binance long-short response")?
                .into_iter()
                .map(|row| row.into_input(period, fetched_at))
                .collect(),
            "taker-buy-sell" => serde_json::from_value::<Vec<BinanceTakerBuySellRow>>(raw)
                .context("failed to parse Binance taker buy/sell response")?
                .into_iter()
                .map(|row| row.into_input(&symbol, period, fetched_at))
                .collect(),
            other => Err(anyhow!("unsupported positioning metric {other}")),
        }
    }

    async fn get_json(&self, endpoint: &str, query: &[(&str, String)]) -> Result<Value> {
        let url = Url::parse(self.base_url.trim_end_matches('/'))?.join(endpoint)?;
        let response = self.http.get(url).query(query).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(public_endpoint_error(endpoint, status, body));
        }
        Ok(response.json::<Value>().await?)
    }
}

fn public_endpoint_error(endpoint: &str, status: StatusCode, body: String) -> anyhow::Error {
    anyhow!("Binance USD-M public endpoint {endpoint} returned HTTP {status}: {body}")
}

fn normalized_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn millis_to_utc(value: i64) -> Result<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value)
        .single()
        .ok_or_else(|| anyhow!("invalid millisecond timestamp {value}"))
}

fn parse_decimal(value: &str, field: &str) -> Result<Decimal> {
    Decimal::from_str(value).with_context(|| format!("invalid decimal field {field}: {value}"))
}

fn parse_optional_decimal(value: Option<&str>, field: &str) -> Result<Option<Decimal>> {
    value
        .map(str::trim)
        .filter(|raw| !raw.is_empty())
        .map(|raw| parse_decimal(raw, field))
        .transpose()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BinanceFundingRateRow {
    pub symbol: String,
    pub funding_time: i64,
    pub funding_rate: String,
    pub mark_price: Option<String>,
}

impl BinanceFundingRateRow {
    pub fn into_input(self, fetched_at: DateTime<Utc>) -> Result<DerivativesFundingRateInput> {
        let funding_time = millis_to_utc(self.funding_time)?;
        let raw_payload = serde_json::to_value(&self)?;
        Ok(DerivativesFundingRateInput {
            exchange: "binance".to_string(),
            symbol: normalized_symbol(&self.symbol),
            funding_time,
            funding_rate: parse_decimal(&self.funding_rate, "fundingRate")?,
            mark_price: parse_optional_decimal(self.mark_price.as_deref(), "markPrice")?,
            fetched_at,
            raw_payload,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BinanceCurrentOpenInterestRow {
    pub symbol: String,
    pub open_interest: String,
    pub time: i64,
}

impl BinanceCurrentOpenInterestRow {
    pub fn into_input(
        self,
        period: &str,
        fetched_at: DateTime<Utc>,
    ) -> Result<DerivativesOpenInterestInput> {
        let timestamp = millis_to_utc(self.time)?;
        let raw_payload = serde_json::to_value(&self)?;
        Ok(DerivativesOpenInterestInput {
            exchange: "binance".to_string(),
            symbol: normalized_symbol(&self.symbol),
            period: period.to_string(),
            timestamp,
            open_interest: parse_decimal(&self.open_interest, "openInterest")?,
            open_interest_value: None,
            fetched_at,
            raw_payload,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinanceOpenInterestHistoryRow {
    pub symbol: String,
    #[serde(rename = "sumOpenInterest")]
    pub sum_open_interest: String,
    #[serde(rename = "sumOpenInterestValue")]
    pub sum_open_interest_value: Option<String>,
    pub timestamp: i64,
}

impl BinanceOpenInterestHistoryRow {
    pub fn into_input(
        self,
        period: &str,
        fetched_at: DateTime<Utc>,
    ) -> Result<DerivativesOpenInterestInput> {
        let timestamp = millis_to_utc(self.timestamp)?;
        let raw_payload = serde_json::to_value(&self)?;
        Ok(DerivativesOpenInterestInput {
            exchange: "binance".to_string(),
            symbol: normalized_symbol(&self.symbol),
            period: period.to_string(),
            timestamp,
            open_interest: parse_decimal(&self.sum_open_interest, "sumOpenInterest")?,
            open_interest_value: parse_optional_decimal(
                self.sum_open_interest_value.as_deref(),
                "sumOpenInterestValue",
            )?,
            fetched_at,
            raw_payload,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinanceLongShortRatioRow {
    pub symbol: String,
    #[serde(rename = "longShortRatio")]
    pub long_short_ratio: String,
    #[serde(rename = "longAccount")]
    pub long_account: String,
    #[serde(rename = "shortAccount")]
    pub short_account: String,
    pub timestamp: i64,
}

impl BinanceLongShortRatioRow {
    pub fn into_input(
        self,
        period: &str,
        fetched_at: DateTime<Utc>,
    ) -> Result<DerivativesPositioningInput> {
        let timestamp = millis_to_utc(self.timestamp)?;
        let raw_payload = serde_json::to_value(&self)?;
        Ok(DerivativesPositioningInput {
            exchange: "binance".to_string(),
            symbol: normalized_symbol(&self.symbol),
            metric: "global-long-short".to_string(),
            period: period.to_string(),
            timestamp,
            long_short_ratio: Some(parse_decimal(&self.long_short_ratio, "longShortRatio")?),
            long_account: Some(parse_decimal(&self.long_account, "longAccount")?),
            short_account: Some(parse_decimal(&self.short_account, "shortAccount")?),
            buy_sell_ratio: None,
            buy_vol: None,
            sell_vol: None,
            fetched_at,
            raw_payload,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinanceTakerBuySellRow {
    #[serde(rename = "buySellRatio")]
    pub buy_sell_ratio: String,
    #[serde(rename = "buyVol")]
    pub buy_vol: String,
    #[serde(rename = "sellVol")]
    pub sell_vol: String,
    pub timestamp: i64,
}

impl BinanceTakerBuySellRow {
    pub fn into_input(
        self,
        symbol: &str,
        period: &str,
        fetched_at: DateTime<Utc>,
    ) -> Result<DerivativesPositioningInput> {
        let timestamp = millis_to_utc(self.timestamp)?;
        let raw_payload = serde_json::to_value(&self)?;
        Ok(DerivativesPositioningInput {
            exchange: "binance".to_string(),
            symbol: normalized_symbol(symbol),
            metric: "taker-buy-sell".to_string(),
            period: period.to_string(),
            timestamp,
            long_short_ratio: None,
            long_account: None,
            short_account: None,
            buy_sell_ratio: Some(parse_decimal(&self.buy_sell_ratio, "buySellRatio")?),
            buy_vol: Some(parse_decimal(&self.buy_vol, "buyVol")?),
            sell_vol: Some(parse_decimal(&self.sell_vol, "sellVol")?),
            fetched_at,
            raw_payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BinanceFundingRateRow, BinanceLongShortRatioRow, BinanceOpenInterestHistoryRow,
        BinanceTakerBuySellRow,
    };
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;

    #[test]
    fn parses_funding_response_decimals() {
        let row = BinanceFundingRateRow {
            symbol: "BTCUSDT".to_string(),
            funding_time: 1_700_000_000_000,
            funding_rate: "-0.00010000".to_string(),
            mark_price: Some("34287.54619963".to_string()),
        };
        let input = row
            .into_input(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap())
            .expect("funding input");
        assert_eq!(input.funding_rate, Decimal::new(-10000, 8));
        assert_eq!(input.mark_price, Some(Decimal::new(3428754619963, 8)));
    }

    #[test]
    fn parses_oi_response_decimals() {
        let row = BinanceOpenInterestHistoryRow {
            symbol: "ETHUSDT".to_string(),
            sum_open_interest: "20403.63700000".to_string(),
            sum_open_interest_value: Some("150570784.07809979".to_string()),
            timestamp: 1_700_000_000_000,
        };
        let input = row
            .into_input("4h", Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap())
            .expect("oi input");
        assert_eq!(input.open_interest, Decimal::new(2040363700000, 8));
        assert_eq!(
            input.open_interest_value,
            Some(Decimal::new(15057078407809979, 8))
        );
    }

    #[test]
    fn parses_long_short_response_decimals() {
        let row = BinanceLongShortRatioRow {
            symbol: "SOLUSDT".to_string(),
            long_short_ratio: "1.9559".to_string(),
            long_account: "0.6617".to_string(),
            short_account: "0.3382".to_string(),
            timestamp: 1_700_000_000_000,
        };
        let input = row
            .into_input("4h", Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap())
            .expect("long-short input");
        assert_eq!(input.long_short_ratio, Some(Decimal::new(19559, 4)));
    }

    #[test]
    fn parses_taker_response_decimals() {
        let row = BinanceTakerBuySellRow {
            buy_sell_ratio: "1.3104".to_string(),
            buy_vol: "343.9290".to_string(),
            sell_vol: "248.5030".to_string(),
            timestamp: 1_700_000_000_000,
        };
        let input = row
            .into_input(
                "BNBUSDT",
                "4h",
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            )
            .expect("taker input");
        assert_eq!(input.buy_sell_ratio, Some(Decimal::new(13104, 4)));
        assert_eq!(input.buy_vol, Some(Decimal::new(3439290, 4)));
    }
}
