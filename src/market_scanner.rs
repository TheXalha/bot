use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use futures_util::{stream, StreamExt};
use reqwest::{Client, StatusCode};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use tokio::time::sleep;

use crate::logger::{self, LogLevel};

// Reward sayfaları cursor bitene kadar tamamen çekilir. Bu sabit yalnızca
// bozuk/sonsuz cursor döngüsüne karşı emniyet sınırıdır (500.000 market).
const REWARD_PAGE_SIZE: usize = 500;
const MAX_REWARD_PAGE_SAFETY: usize = 1_000;

// POST /books istekleri küçük body ve kontrollü paralellik ile yürütülür.
// 8 eşzamanlı istek, yayımlanan /books limitinin çok altındadır.
const BOOK_BATCH_SIZE: usize = 100;
const BOOK_FETCH_CONCURRENCY: usize = 8;
// /books: 500 istek / 10 sn. 25 ms aralık = en fazla 400 başlangıç / 10 sn.
const BOOK_REQUEST_MIN_INTERVAL_MILLISECONDS: u64 = 25;

// Geçmiş endpoint'i için de sınırlı paralellik ve muhafazakâr pacing kullanılır.
const HISTORY_FETCH_CONCURRENCY: usize = 8;
const HISTORY_REQUEST_MIN_INTERVAL_MILLISECONDS: u64 = 20;

// 429, 425, timeout ve 5xx cevaplarında resmi öneriye uygun üstel backoff.
const HTTP_RETRY_ATTEMPTS: usize = 5;
const HTTP_RETRY_BASE_MILLISECONDS: u64 = 250;
const HTTP_RETRY_MAX_MILLISECONDS: u64 = 4_000;

#[derive(Debug, Clone)]
struct RequestPacer {
    next_allowed: Arc<Mutex<Instant>>,
    min_interval: Duration,
}

impl RequestPacer {
    fn new(min_interval: Duration) -> Self {
        Self {
            next_allowed: Arc::new(Mutex::new(Instant::now())),
            min_interval,
        }
    }

    async fn wait(&self) {
        // std::sync::Mutex yalnızca bir sonraki slotu ayırmak için çok kısa
        // süre tutulur; await başlamadan önce guard bırakılır.
        let delay = {
            let mut next_allowed = self
                .next_allowed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            let now = Instant::now();
            let scheduled = (*next_allowed).max(now);
            *next_allowed = scheduled + self.min_interval;
            scheduled.saturating_duration_since(now)
        };

        if !delay.is_zero() {
            sleep(delay).await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScannerConfig {
    /// Scanner tarafındaki bütün CLOB HTTP isteklerinin ortak taban adresi.
    pub clob_host: String,

    /// Olayın sonuçlanmasına kalan minimum gün.
    pub min_days_to_end: f64,

    /// Olayın sonuçlanmasına kalan maksimum gün.
    pub max_days_to_end: f64,

    /// Son 24 saatte izin verilen maksimum mutlak fiyat değişimi.
    /// 0.12 = 12 cent.
    pub max_abs_price_change_24h: f64,

    /// Gerçek order book'taki maksimum alış-satış farkı.
    /// 0.06 = 6 cent.
    pub max_live_spread: f64,

    /// Reward Max Spread için minimum değer.
    /// 0.02 = 2 cent.
    pub min_reward_max_spread: f64,

    /// Reward sınırından kaç tick içeride emir önerileceği.
    /// 0 tam sınırı hedefler; resmi formülde sınırdaki skor sıfırdır.
    /// Varsayılan 1, emri bir tick içeride tutar.
    pub quote_inside_ticks: u32,

    /// Otomatik strateji sadece BUY emirleri kullanacağı için true.
    /// Bağımsız tarayıcıda SELL fırsatlarını da görmek için false yapılabilir.
    pub buy_quotes_only: bool,

    /// İncelenen dönem içindeki maksimum fiyat aralığı.
    pub max_history_range: f64,

    /// Yakın tarihli iki ölçüm arasında izin verilen maksimum ani sıçrama.
    pub max_history_jump: f64,

    /// Son 24 saatlik minimum hacim.
    pub min_volume_24h: f64,

    /// Ödüle sayılmak için gereken minimum share miktarının üst sınırı.
    pub max_reward_min_size: f64,

    /// Bizim emir fiyatımızdan kesin olarak daha iyi seviyelerde duran
    /// minimum efektif share miktarı.
    pub min_protective_depth: f64,

    /// Tek bir fiyat seviyesinin koruyucu derinliğe yapabileceği maksimum katkı.
    /// Böylece tek bir büyük ve kolayca iptal edilebilir duvar bütün korumayı
    /// tek başına sağlıyor gibi değerlendirilmez.
    pub protective_depth_level_cap: f64,

    /// Kaç günlük fiyat geçmişinin inceleneceği.
    pub history_days: i64,

    /// Minimum tarihsel fiyat noktası.
    pub min_history_points: usize,

    /// Geçmiş fiyat noktalarının dakika hassasiyeti.
    pub history_fidelity_minutes: u32,

    /// Tek fiyat geçmişi isteğinde gönderilecek token sayısı.
    /// Küçük paketler timeout riskini azaltır.
    pub history_batch_size: usize,

    /// Başarısız fiyat geçmişi paketinin kaç kez deneneceği.
    pub history_retry_attempts: usize,

    /// Her fiyat geçmişi isteğine özel timeout.
    pub history_request_timeout_seconds: u64,

    /// İki tarihsel nokta arasında bundan daha büyük boşluk varsa,
    /// aradaki fark ani sıçrama hesabına katılmaz.
    pub max_history_gap_minutes: i64,

    pub request_timeout_seconds: u64,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            clob_host: "https://clob.polymarket.com".to_string(),
            min_days_to_end: 3.0,
            max_days_to_end: 730.0,
            max_abs_price_change_24h: 0.12,
            max_live_spread: 0.06,
            min_reward_max_spread: 0.02,
            quote_inside_ticks: 1,
            buy_quotes_only: true,
            max_history_range: 0.30,
            max_history_jump: 0.03,
            min_volume_24h: 25.0,
            max_reward_min_size: 100.0,
            min_protective_depth: 15.0,
            protective_depth_level_cap: 50.0,
            history_days: 3,
            min_history_points: 16,
            history_fidelity_minutes: 15,
            history_batch_size: 5,
            history_retry_attempts: 3,
            history_request_timeout_seconds: 60,
            max_history_gap_minutes: 45,
            request_timeout_seconds: 60,
        }
    }
}

impl ScannerConfig {
    /// Bağımsız scanner ile ana botun aynı ortak quote ayarlarını kullanmasını sağlar.
    /// Değer yoksa ScannerConfig varsayılanı korunur; geçersiz değer hata döndürür.
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();

        if let Ok(value) = std::env::var("CLOB_API_URL") {
            let value = value.trim();
            if !value.is_empty() {
                config.clob_host = value.to_owned();
            }
        }

        if let Ok(value) = std::env::var("QUOTE_INSIDE_TICKS") {
            let value = value.trim();
            if !value.is_empty() {
                config.quote_inside_ticks = value
                    .parse::<u32>()
                    .context("QUOTE_INSIDE_TICKS geçerli bir u32 değil")?;
            }
        }

        if let Ok(value) = std::env::var("MIN_PROTECTIVE_DEPTH") {
            let value = value.trim();
            if !value.is_empty() {
                config.min_protective_depth = value
                    .parse::<f64>()
                    .context("MIN_PROTECTIVE_DEPTH geçerli bir f64 değil")?;
            }
        }

        if let Ok(value) = std::env::var("PROTECTIVE_DEPTH_LEVEL_CAP") {
            let value = value.trim();
            if !value.is_empty() {
                config.protective_depth_level_cap = value
                    .parse::<f64>()
                    .context("PROTECTIVE_DEPTH_LEVEL_CAP geçerli bir f64 değil")?;
            }
        }

        if let Ok(value) = std::env::var("MAX_LIVE_SPREAD") {
            let value = value.trim();
            if !value.is_empty() {
                config.max_live_spread = value
                    .parse::<f64>()
                    .context("MAX_LIVE_SPREAD geçerli bir f64 değil")?;
            }
        }

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if !is_http_url(&self.clob_host) {
            bail!(
                "CLOB_API_URL http:// veya https:// ile başlamalı: {}",
                self.clob_host
            );
        }

        for (name, value) in [
            ("MIN_DAYS_TO_END", self.min_days_to_end),
            ("MAX_DAYS_TO_END", self.max_days_to_end),
            ("MAX_ABS_PRICE_CHANGE_24H", self.max_abs_price_change_24h),
            ("MAX_LIVE_SPREAD", self.max_live_spread),
            ("MIN_REWARD_MAX_SPREAD", self.min_reward_max_spread),
            ("MAX_HISTORY_RANGE", self.max_history_range),
            ("MAX_HISTORY_JUMP", self.max_history_jump),
            ("MIN_VOLUME_24H", self.min_volume_24h),
            ("MAX_REWARD_MIN_SIZE", self.max_reward_min_size),
            ("MIN_PROTECTIVE_DEPTH", self.min_protective_depth),
            (
                "PROTECTIVE_DEPTH_LEVEL_CAP",
                self.protective_depth_level_cap,
            ),
        ] {
            if !value.is_finite() {
                bail!("{name} sonlu bir sayı olmalı");
            }
        }

        if self.min_days_to_end < 0.0
            || self.max_days_to_end <= 0.0
            || self.min_days_to_end > self.max_days_to_end
        {
            bail!(
                "Market bitiş günü aralığı geçersiz: min={}, max={}",
                self.min_days_to_end,
                self.max_days_to_end
            );
        }

        for (name, value) in [
            ("MAX_ABS_PRICE_CHANGE_24H", self.max_abs_price_change_24h),
            ("MAX_LIVE_SPREAD", self.max_live_spread),
            ("MIN_REWARD_MAX_SPREAD", self.min_reward_max_spread),
            ("MAX_HISTORY_RANGE", self.max_history_range),
            ("MAX_HISTORY_JUMP", self.max_history_jump),
        ] {
            if !(0.0..1.0).contains(&value) {
                bail!("{name} 0 ile 1 arasında olmalı");
            }
        }

        if self.min_volume_24h < 0.0
            || self.max_reward_min_size <= 0.0
            || self.min_protective_depth < 0.0
            || self.protective_depth_level_cap <= 0.0
        {
            bail!("Hacim, reward size veya koruyucu derinlik ayarları geçersiz");
        }

        if self.min_history_points == 0
            || self.history_batch_size == 0
            || self.history_retry_attempts == 0
            || self.history_fidelity_minutes == 0
            || self.history_request_timeout_seconds == 0
            || self.request_timeout_seconds == 0
        {
            bail!("Scanner adet, paket ve timeout değerleri sıfır olamaz");
        }

        if self.history_days <= 0 || self.max_history_gap_minutes <= 0 {
            bail!("History gün ve maksimum boşluk değerleri pozitif olmalı");
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RankedMarket {
    pub score: f64,

    pub condition_id: String,
    pub question: String,
    pub event_slug: String,
    pub market_slug: String,

    pub end_at: DateTime<Utc>,
    pub days_to_end: f64,

    pub midpoint: f64,
    pub live_spread: f64,

    pub price_change_24h: f64,
    pub history_range: f64,
    pub history_max_jump: f64,
    pub history_rms_change: f64,

    pub reward_per_day: f64,
    pub reward_min_size: f64,
    pub reward_max_spread: f64,

    pub volume_24h: f64,
    pub qualifying_depth: f64,
    pub market_competitiveness: f64,

    pub suggested_token_id: String,
    pub suggested_outcome: String,
    pub suggested_side: String,
    pub suggested_price: f64,
    pub reference_best_price: f64,
    pub protective_depth: f64,
    pub quote_distance_from_midpoint: f64,
    pub tick_size: f64,
}

#[derive(Debug, Deserialize)]
struct RewardMarketsPage {
    #[serde(default)]
    next_cursor: String,

    #[serde(default)]
    data: Vec<RewardMarket>,
}

#[derive(Debug, Deserialize, Clone)]
struct RewardMarket {
    #[serde(default)]
    condition_id: String,

    #[serde(default)]
    event_slug: String,

    #[serde(default)]
    market_slug: String,

    #[serde(default)]
    question: String,

    #[serde(default, deserialize_with = "deserialize_optional_string")]
    end_date: Option<String>,

    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    market_competitiveness: Option<f64>,

    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    one_day_price_change: Option<f64>,

    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    rewards_max_spread: Option<f64>,

    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    rewards_min_size: Option<f64>,

    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    spread: Option<f64>,

    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    volume_24hr: Option<f64>,

    #[serde(default)]
    tokens: Vec<RewardToken>,

    #[serde(default)]
    rewards_config: Vec<RewardConfig>,
}

#[derive(Debug, Deserialize, Clone)]
struct RewardToken {
    #[serde(default)]
    token_id: String,

    #[serde(default)]
    outcome: String,
}

#[derive(Debug, Deserialize, Clone)]
struct RewardConfig {
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    start_date: Option<String>,

    #[serde(default, deserialize_with = "deserialize_optional_string")]
    end_date: Option<String>,

    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    rate_per_day: Option<f64>,
}

#[derive(Debug)]
struct PreliminaryCandidate {
    market: RewardMarket,
    end_at: DateTime<Utc>,
    days_to_end: f64,
    reward_per_day: f64,
    reward_min_size: f64,
    reward_max_spread: f64,
    primary_token_id: String,
}

#[derive(Debug)]
struct UnscoredMarket {
    candidate: PreliminaryCandidate,

    midpoint: f64,
    live_spread: f64,

    price_change_24h: f64,
    history_range: f64,
    history_max_jump: f64,
    history_rms_change: f64,

    qualifying_depth: f64,
    competition_proxy: f64,
    reward_efficiency: f64,
    volatility_risk: f64,

    quote: QuoteOpportunity,
}

#[derive(Debug, Serialize)]
struct BookRequest {
    token_id: String,
}

#[derive(Debug, Deserialize)]
struct BookSnapshot {
    #[serde(default)]
    asset_id: String,

    #[serde(default)]
    bids: Vec<PriceLevel>,

    #[serde(default)]
    asks: Vec<PriceLevel>,

    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    tick_size: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PriceLevel {
    #[serde(deserialize_with = "deserialize_f64")]
    price: f64,

    #[serde(deserialize_with = "deserialize_f64")]
    size: f64,
}

#[derive(Debug, Serialize)]
struct BatchHistoryRequest {
    markets: Vec<String>,
    start_ts: i64,
    end_ts: i64,
    fidelity: u32,
}

#[derive(Debug, Deserialize)]
struct BatchHistoryResponse {
    #[serde(default)]
    history: HashMap<String, Vec<HistoryPoint>>,
}

#[derive(Debug, Deserialize)]
struct HistoryPoint {
    #[serde(deserialize_with = "deserialize_i64")]
    t: i64,

    #[serde(deserialize_with = "deserialize_f64")]
    p: f64,
}

#[derive(Debug)]
struct HistoryStats {
    point_count: usize,
    range: f64,
    max_jump: f64,
    rms_change: f64,
    change_24h: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
enum QuoteSide {
    Buy,
    Sell,
}

impl QuoteSide {
    fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone)]
struct BookQuoteCandidate {
    side: QuoteSide,
    planned_price: f64,
    reference_best_price: f64,
    protective_depth: f64,
    distance_from_midpoint: f64,
    tick_size: f64,
}

#[derive(Debug)]
struct BookMetrics {
    midpoint: f64,
    spread: f64,
    qualifying_depth: f64,
    quote_candidates: Vec<BookQuoteCandidate>,
}

#[derive(Debug)]
struct QuoteOpportunity {
    token_id: String,
    outcome: String,
    side: QuoteSide,
    planned_price: f64,
    reference_best_price: f64,
    protective_depth: f64,
    distance_from_midpoint: f64,
    tick_size: f64,
}

#[derive(Debug)]
enum PreliminaryReject {
    MissingEndDate,
    InvalidEndDate(String),
    EndDateRange,
    PriceChange,
    Volume,
    ApiSpread,
    NoActiveReward,
    RewardMinSize,
    RewardMaxSpread,
    MissingTokens,
    MissingPrimaryToken,
}

#[derive(Debug, Default)]
struct PreliminaryFilterStats {
    accepted: usize,
    missing_end_date: usize,
    invalid_end_date: usize,
    invalid_end_date_examples: Vec<String>,
    end_date_range: usize,
    price_change: usize,
    volume: usize,
    api_spread: usize,
    no_active_reward: usize,
    reward_min_size: usize,
    reward_max_spread: usize,
    missing_tokens: usize,
    missing_primary_token: usize,
}

impl PreliminaryFilterStats {
    fn record(&mut self, reason: PreliminaryReject) {
        match reason {
            PreliminaryReject::MissingEndDate => self.missing_end_date += 1,
            PreliminaryReject::InvalidEndDate(value) => {
                self.invalid_end_date += 1;

                if self.invalid_end_date_examples.len() < 5
                    && !self.invalid_end_date_examples.contains(&value)
                {
                    self.invalid_end_date_examples.push(value);
                }
            }
            PreliminaryReject::EndDateRange => self.end_date_range += 1,
            PreliminaryReject::PriceChange => self.price_change += 1,
            PreliminaryReject::Volume => self.volume += 1,
            PreliminaryReject::ApiSpread => self.api_spread += 1,
            PreliminaryReject::NoActiveReward => self.no_active_reward += 1,
            PreliminaryReject::RewardMinSize => self.reward_min_size += 1,
            PreliminaryReject::RewardMaxSpread => self.reward_max_spread += 1,
            PreliminaryReject::MissingTokens => self.missing_tokens += 1,
            PreliminaryReject::MissingPrimaryToken => self.missing_primary_token += 1,
        }
    }

    fn print(&self) {
        crate::log_info!("console", "\nÖn filtre özeti:");
        crate::log_info!("console", "  Geçen: {}", self.accepted);
        crate::log_info!("console", "  Bitiş tarihi yok: {}", self.missing_end_date);
        crate::log_info!("console", "  Bitiş tarihi okunamadı: {}", self.invalid_end_date);

        if !self.invalid_end_date_examples.is_empty() {
            crate::log_info!("console", "  Okunamayan tarih örnekleri:");
            for value in &self.invalid_end_date_examples {
                crate::log_info!("console", "    - {value:?}");
            }
        }

        crate::log_info!("console", "  Tarih aralığı dışında: {}", self.end_date_range);
        crate::log_info!("console", "  24s değişim yüksek: {}", self.price_change);
        crate::log_info!("console", "  24s hacim düşük: {}", self.volume);
        crate::log_info!("console", "  API spread çok yüksek: {}", self.api_spread);
        crate::log_info!("console", "  Aktif reward yok: {}", self.no_active_reward);
        crate::log_info!("console", "  Reward min size uygun değil: {}", self.reward_min_size);
        crate::log_info!("console", 
            "  Reward Max Spread 2 cent altı: {}",
            self.reward_max_spread
        );
        crate::log_info!("console", "  Token verisi eksik: {}", self.missing_tokens);
        crate::log_info!("console", 
            "  Birincil token bulunamadı: {}",
            self.missing_primary_token
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum DeepReject {
    MissingBook,
    InvalidBook,
    LiveSpread,
    NoRewardEligibleQuote,
    ProtectiveDepth,
    ExtremeMidpoint,
    MissingHistory,
    InsufficientHistory,
    HistoryRange,
    HistoryJump,
    PriceChange,
}

#[derive(Debug, Default)]
struct DeepFilterStats {
    accepted: usize,
    missing_book: usize,
    invalid_book: usize,
    live_spread: usize,
    no_reward_eligible_quote: usize,
    protective_depth: usize,
    extreme_midpoint: usize,
    missing_history: usize,
    insufficient_history: usize,
    history_range: usize,
    history_jump: usize,
    price_change: usize,
}

impl DeepFilterStats {
    fn record(&mut self, reason: DeepReject) {
        match reason {
            DeepReject::MissingBook => self.missing_book += 1,
            DeepReject::InvalidBook => self.invalid_book += 1,
            DeepReject::LiveSpread => self.live_spread += 1,
            DeepReject::NoRewardEligibleQuote => self.no_reward_eligible_quote += 1,
            DeepReject::ProtectiveDepth => self.protective_depth += 1,
            DeepReject::ExtremeMidpoint => self.extreme_midpoint += 1,
            DeepReject::MissingHistory => self.missing_history += 1,
            DeepReject::InsufficientHistory => self.insufficient_history += 1,
            DeepReject::HistoryRange => self.history_range += 1,
            DeepReject::HistoryJump => self.history_jump += 1,
            DeepReject::PriceChange => self.price_change += 1,
        }
    }

    fn print(&self) {
        crate::log_info!("console", "\nDerin filtre özeti:");
        crate::log_info!("console", "  Geçen: {}", self.accepted);
        crate::log_info!("console", "  Order book bulunamadı: {}", self.missing_book);
        crate::log_info!("console", "  Order book geçersiz/boş: {}", self.invalid_book);
        crate::log_info!("console", "  Canlı spread yüksek: {}", self.live_spread);
        crate::log_info!("console", 
            "  Reward sınırında korumalı uygun fiyat yok: {}",
            self.no_reward_eligible_quote
        );
        crate::log_info!("console", 
            "  Önümüzdeki efektif koruyucu derinlik düşük: {}",
            self.protective_depth
        );
        crate::log_info!("console", "  Midpoint 0.10-0.90 dışında: {}", self.extreme_midpoint);
        crate::log_info!("console", "  Fiyat geçmişi bulunamadı: {}", self.missing_history);
        crate::log_info!("console", "  Fiyat geçmişi yetersiz: {}", self.insufficient_history);
        crate::log_info!("console", "  Dönem aralığı çok yüksek: {}", self.history_range);
        crate::log_info!("console", "  Ani fiyat sıçraması yüksek: {}", self.history_jump);
        crate::log_info!("console", "  24s değişim yüksek: {}", self.price_change);
    }
}

pub async fn scan_best_markets(config: &ScannerConfig) -> Result<Vec<RankedMarket>> {
    config.validate()?;

    logger::log_event(
        LogLevel::Info,
        "market_scanner",
        "scan_started",
        "Market taraması başlatıldı",
        json!({
            "clob_host": config.clob_host,
            "min_days_to_end": config.min_days_to_end,
            "max_days_to_end": config.max_days_to_end,
            "max_abs_price_change_24h": config.max_abs_price_change_24h,
            "max_live_spread": config.max_live_spread,
            "min_reward_max_spread": config.min_reward_max_spread,
            "min_volume_24h": config.min_volume_24h,
            "min_protective_depth": config.min_protective_depth,
            "history_days": config.history_days,
            "history_fidelity_minutes": config.history_fidelity_minutes,
        }),
        file!(),
        line!(),
    );

    let http = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_seconds))
        .user_agent("PolyBot-MarketScanner/0.3")
        .build()
        .context("HTTP client oluşturulamadı")?;

    crate::log_info!("console", "Ödüllü piyasalar indiriliyor...");

    let reward_markets = fetch_reward_markets(&http, config).await?;

    crate::log_info!("console", "{} ödüllü piyasa indirildi.", reward_markets.len());

    let now = Utc::now();
    let mut preliminary_stats = PreliminaryFilterStats::default();
    let mut preliminary = Vec::new();

    for market in reward_markets {
        match build_preliminary_candidate(market, &now, config) {
            Ok(candidate) => {
                preliminary_stats.accepted += 1;
                preliminary.push(candidate);
            }
            Err(reason) => preliminary_stats.record(reason),
        }
    }

    preliminary_stats.print();

    preliminary.sort_by(|left, right| preliminary_score(right).total_cmp(&preliminary_score(left)));

    preliminary.shrink_to_fit();

    crate::log_info!("console", 
        "\nÖn filtreden geçen {} piyasanın tamamı derin analiz için seçildi.",
        preliminary.len()
    );

    if preliminary.is_empty() {
        logger::log_event(
            LogLevel::Info,
            "market_scanner",
            "scan_finished_empty_preliminary",
            "Ön filtreden geçen market bulunamadı",
            json!({
                "preliminary_accepted": preliminary_stats.accepted,
                "missing_end_date": preliminary_stats.missing_end_date,
                "invalid_end_date": preliminary_stats.invalid_end_date,
                "end_date_range": preliminary_stats.end_date_range,
                "price_change": preliminary_stats.price_change,
                "volume": preliminary_stats.volume,
                "api_spread": preliminary_stats.api_spread,
                "no_active_reward": preliminary_stats.no_active_reward,
                "reward_min_size": preliminary_stats.reward_min_size,
                "reward_max_spread": preliminary_stats.reward_max_spread,
                "missing_tokens": preliminary_stats.missing_tokens,
                "missing_primary_token": preliminary_stats.missing_primary_token,
            }),
            file!(),
            line!(),
        );
        return Ok(Vec::new());
    }

    let all_token_ids = preliminary
        .iter()
        .flat_map(|candidate| {
            candidate
                .market
                .tokens
                .iter()
                .map(|token| token.token_id.clone())
        })
        .filter(|token_id| !token_id.is_empty())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    crate::log_info!("console", "Order book verileri alınıyor...");

    let books = fetch_order_books(&http, &all_token_ids, config).await?;

    let primary_token_ids = preliminary
        .iter()
        .map(|candidate| candidate.primary_token_id.clone())
        .collect::<Vec<_>>();

    crate::log_info!("console", 
        "{} günlük fiyat geçmişi, {} dakikalık hassasiyetle alınıyor...",
        config.history_days, config.history_fidelity_minutes
    );

    let histories = fetch_price_histories(&http, &primary_token_ids, config).await?;

    // Bu iki ID listesine artık ihtiyaç yok.
    drop(all_token_ids);
    drop(primary_token_ids);

    let mut deep_stats = DeepFilterStats::default();
    let mut unscored = Vec::new();

    for candidate in preliminary {
        let reward_band = candidate.reward_max_spread;

        let mut total_qualifying_depth = 0.0;
        let mut market_spreads = Vec::new();
        let mut primary_midpoint = None;
        let mut best_quote: Option<QuoteOpportunity> = None;
        let mut book_failure: Option<DeepReject> = None;

        for token in &candidate.market.tokens {
            let Some(book) = books.get(&token.token_id) else {
                book_failure = Some(DeepReject::MissingBook);
                break;
            };

            let Some(metrics) = calculate_book_metrics(
                book,
                reward_band,
                config.quote_inside_ticks,
                config.buy_quotes_only,
                config.protective_depth_level_cap,
            ) else {
                book_failure = Some(DeepReject::InvalidBook);
                break;
            };

            market_spreads.push(metrics.spread);
            total_qualifying_depth += metrics.qualifying_depth;

            if token.token_id == candidate.primary_token_id {
                primary_midpoint = Some(metrics.midpoint);
            }

            for quote_candidate in metrics.quote_candidates {
                let opportunity = QuoteOpportunity {
                    token_id: token.token_id.clone(),
                    outcome: token.outcome.clone(),
                    side: quote_candidate.side,
                    planned_price: quote_candidate.planned_price,
                    reference_best_price: quote_candidate.reference_best_price,
                    protective_depth: quote_candidate.protective_depth,
                    distance_from_midpoint: quote_candidate.distance_from_midpoint,
                    tick_size: quote_candidate.tick_size,
                };

                let replace = best_quote
                    .as_ref()
                    .map(|current| {
                        quote_opportunity_score(&opportunity, reward_band)
                            > quote_opportunity_score(current, reward_band)
                    })
                    .unwrap_or(true);

                if replace {
                    best_quote = Some(opportunity);
                }
            }
        }

        if let Some(reason) = book_failure {
            deep_stats.record(reason);
            continue;
        }

        if market_spreads.is_empty() {
            deep_stats.record(DeepReject::InvalidBook);
            continue;
        }

        let live_spread = market_spreads.into_iter().fold(0.0_f64, f64::max);

        if live_spread > config.max_live_spread {
            deep_stats.record(DeepReject::LiveSpread);
            continue;
        }

        let Some(midpoint) = primary_midpoint else {
            deep_stats.record(DeepReject::InvalidBook);
            continue;
        };

        // Tek taraflı reward stratejisinde uç fiyatlar daha zor ve daha risklidir.
        if !(0.10..=0.90).contains(&midpoint) {
            deep_stats.record(DeepReject::ExtremeMidpoint);
            continue;
        }

        let Some(quote) = best_quote else {
            deep_stats.record(DeepReject::NoRewardEligibleQuote);
            continue;
        };

        if quote.protective_depth < config.min_protective_depth {
            deep_stats.record(DeepReject::ProtectiveDepth);
            continue;
        }

        let Some(history) = histories.get(&candidate.primary_token_id) else {
            deep_stats.record(DeepReject::MissingHistory);
            continue;
        };

        let Some(history_stats) =
            calculate_history_stats(history, config.max_history_gap_minutes * 60)
        else {
            deep_stats.record(DeepReject::MissingHistory);
            continue;
        };

        if history_stats.point_count < config.min_history_points {
            deep_stats.record(DeepReject::InsufficientHistory);
            continue;
        }

        if history_stats.range > config.max_history_range {
            deep_stats.record(DeepReject::HistoryRange);
            continue;
        }

        if history_stats.max_jump > config.max_history_jump {
            deep_stats.record(DeepReject::HistoryJump);
            continue;
        }

        let api_change = candidate
            .market
            .one_day_price_change
            .unwrap_or_default()
            .abs();

        let calculated_change = history_stats.change_24h.unwrap_or_default().abs();
        let price_change_24h = api_change.max(calculated_change);

        if price_change_24h > config.max_abs_price_change_24h {
            deep_stats.record(DeepReject::PriceChange);
            continue;
        }

        let competitiveness = candidate
            .market
            .market_competitiveness
            .unwrap_or(0.5)
            .clamp(0.0, 1.0);

        // Kesin rakip sayısı değil; reward band'indeki görünen derinlik ve
        // API competitiveness alanından oluşturulan yaklaşık ölçüdür.
        let competition_proxy = total_qualifying_depth * (0.75 + competitiveness * 0.50);

        let reward_efficiency = candidate.reward_per_day / (1.0 + competition_proxy.sqrt());

        // Genel fiyat aralığından çok kısa süreli sıçramaya ağırlık verilir.
        let volatility_risk = history_stats.max_jump * 0.55
            + history_stats.rms_change * 0.25
            + price_change_24h * 0.15
            + history_stats.range * 0.05;

        deep_stats.accepted += 1;

        unscored.push(UnscoredMarket {
            candidate,
            midpoint,
            live_spread,
            price_change_24h,
            history_range: history_stats.range,
            history_max_jump: history_stats.max_jump,
            history_rms_change: history_stats.rms_change,
            qualifying_depth: total_qualifying_depth,
            competition_proxy,
            reward_efficiency,
            volatility_risk,
            quote,
        });
    }

    // Büyük order book ve geçmiş haritaları artık kullanılmıyor.
    // Process devam ederken RAM'i daha erken serbest bırakmak için açıkça düşürüyoruz.
    drop(books);
    drop(histories);

    deep_stats.print();

    if unscored.is_empty() {
        logger::log_event(
            LogLevel::Info,
            "market_scanner",
            "scan_finished_empty_deep_filter",
            "Derin filtreden geçen market bulunamadı",
            json!({
                "accepted": deep_stats.accepted,
                "missing_book": deep_stats.missing_book,
                "invalid_book": deep_stats.invalid_book,
                "live_spread": deep_stats.live_spread,
                "no_reward_eligible_quote": deep_stats.no_reward_eligible_quote,
                "protective_depth": deep_stats.protective_depth,
                "extreme_midpoint": deep_stats.extreme_midpoint,
                "missing_history": deep_stats.missing_history,
                "insufficient_history": deep_stats.insufficient_history,
                "history_range": deep_stats.history_range,
                "history_jump": deep_stats.history_jump,
                "price_change": deep_stats.price_change,
            }),
            file!(),
            line!(),
        );
        return Ok(Vec::new());
    }

    let reward_bounds = bounds(
        unscored
            .iter()
            .map(|market| market.candidate.reward_per_day),
    );

    let efficiency_bounds = bounds(unscored.iter().map(|market| market.reward_efficiency));

    let volatility_bounds = bounds(unscored.iter().map(|market| market.volatility_risk));

    let spread_bounds = bounds(unscored.iter().map(|market| market.live_spread));

    let competition_bounds = bounds(unscored.iter().map(|market| market.competition_proxy));

    let protection_bounds = bounds(unscored.iter().map(|market| market.quote.protective_depth));

    let reward_band_bounds = bounds(
        unscored
            .iter()
            .map(|market| market.candidate.reward_max_spread),
    );

    let mut ranked = unscored
        .into_iter()
        .map(|market| {
            let reward_score = normalize_high(market.candidate.reward_per_day, reward_bounds);

            let efficiency_score = normalize_high(market.reward_efficiency, efficiency_bounds);

            let volatility_score = normalize_low(market.volatility_risk, volatility_bounds);

            let spread_score = normalize_low(market.live_spread, spread_bounds);

            let protection_score = normalize_high(market.quote.protective_depth, protection_bounds);

            let reward_band_score =
                normalize_high(market.candidate.reward_max_spread, reward_band_bounds);

            let competition_score = normalize_low(market.competition_proxy, competition_bounds);

            let horizon_score = ((market.candidate.days_to_end - config.min_days_to_end)
                / (90.0 - config.min_days_to_end).max(1.0))
            .clamp(0.0, 1.0);

            let score = 100.0
                * (efficiency_score * 0.25
                    + reward_score * 0.15
                    + volatility_score * 0.20
                    + spread_score * 0.15
                    + protection_score * 0.15
                    + reward_band_score * 0.07
                    + horizon_score * 0.02
                    + competition_score * 0.01);

            RankedMarket {
                score,
                condition_id: market.candidate.market.condition_id,
                question: market.candidate.market.question,
                event_slug: market.candidate.market.event_slug,
                market_slug: market.candidate.market.market_slug,
                end_at: market.candidate.end_at,
                days_to_end: market.candidate.days_to_end,
                midpoint: market.midpoint,
                live_spread: market.live_spread,
                price_change_24h: market.price_change_24h,
                history_range: market.history_range,
                history_max_jump: market.history_max_jump,
                history_rms_change: market.history_rms_change,
                reward_per_day: market.candidate.reward_per_day,
                reward_min_size: market.candidate.reward_min_size,
                reward_max_spread: market.candidate.reward_max_spread,
                volume_24h: market.candidate.market.volume_24hr.unwrap_or_default(),
                qualifying_depth: market.qualifying_depth,
                market_competitiveness: market
                    .candidate
                    .market
                    .market_competitiveness
                    .unwrap_or_default(),
                suggested_token_id: market.quote.token_id,
                suggested_outcome: market.quote.outcome,
                suggested_side: market.quote.side.as_str().to_string(),
                suggested_price: market.quote.planned_price,
                reference_best_price: market.quote.reference_best_price,
                protective_depth: market.quote.protective_depth,
                quote_distance_from_midpoint: market.quote.distance_from_midpoint,
                tick_size: market.quote.tick_size,
            }
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| right.score.total_cmp(&left.score));

    logger::log_event(
        LogLevel::Info,
        "market_scanner",
        "scan_completed",
        "Market taraması başarıyla tamamlandı",
        json!({
            "ranked_market_count": ranked.len(),
            "top_score": ranked.first().map(|market| market.score),
            "top_market_slug": ranked.first().map(|market| market.market_slug.as_str()),
            "top_token_id": ranked.first().map(|market| market.suggested_token_id.as_str()),
        }),
        file!(),
        line!(),
    );

    Ok(ranked)
}

pub fn print_ranked_markets(markets: &[RankedMarket]) {
    if markets.is_empty() {
        crate::log_info!("console", "\nBelirlenen risk kriterlerine uygun piyasa bulunamadı.");
        return;
    }

    crate::log_info!("console", "\n==========================================");
    crate::log_info!("console", " EN UYGUN ÖDÜLLÜ PİYASALAR");
    crate::log_info!("console", "==========================================");

    for (index, market) in markets.iter().enumerate() {
        crate::log_info!("console", "\n{}. Skor: {:.1}/100", index + 1, market.score);
        crate::log_info!("console", "   {}", market.question);

        crate::log_info!("console", 
            "   Bitiş: {} | Kalan: {:.1} gün",
            market.end_at.format("%Y-%m-%d"),
            market.days_to_end
        );

        crate::log_info!("console", 
            "   Midpoint: {:.3} | Canlı spread: {:.1} cent",
            market.midpoint,
            market.live_spread * 100.0
        );

        crate::log_info!("console", 
            "   24s değişim: {:.1} cent | Dönem aralığı: {:.1} cent",
            market.price_change_24h * 100.0,
            market.history_range * 100.0
        );

        crate::log_info!("console", 
            "   Maksimum kısa sıçrama: {:.1} cent | RMS hareket: {:.2} cent",
            market.history_max_jump * 100.0,
            market.history_rms_change * 100.0
        );

        crate::log_info!("console", 
            "   Reward/gün: ${:.2} | Reward min size: {:.2} share",
            market.reward_per_day, market.reward_min_size
        );

        crate::log_info!("console", 
            "   Reward Max Spread: {:.1} cent | Reward band derinliği: {:.2} share",
            market.reward_max_spread * 100.0,
            market.qualifying_depth
        );

        crate::log_info!("console", 
            "   Reward-edge emir önerisi: {} {} @ {:.3}",
            market.suggested_outcome, market.suggested_side, market.suggested_price
        );

        crate::log_info!("console", 
            "   Mevcut en iyi fiyat: {:.3} | Tick: {:.3}",
            market.reference_best_price, market.tick_size
        );

        crate::log_info!("console", 
            "   Önümüzdeki efektif koruyucu derinlik: {:.2} share | Midpoint uzaklığı: {:.2} cent",
            market.protective_depth,
            market.quote_distance_from_midpoint * 100.0
        );

        crate::log_info!("console", 
            "   24s hacim: ${:.2} | API competitiveness: {:.3}",
            market.volume_24h, market.market_competitiveness
        );

        crate::log_info!("console", "   Token ID: {}", market.suggested_token_id);
        crate::log_info!("console", "   Condition ID: {}", market.condition_id);

        let slug = if !market.event_slug.is_empty() {
            &market.event_slug
        } else {
            &market.market_slug
        };

        crate::log_info!("console", "   URL: https://polymarket.com/event/{}", slug);
    }
}

async fn fetch_reward_markets(http: &Client, config: &ScannerConfig) -> Result<Vec<RewardMarket>> {
    let mut all_markets = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();

    for page_number in 1..=MAX_REWARD_PAGE_SAFETY {
        let mut parameters = vec![
            ("page_size", REWARD_PAGE_SIZE.to_string()),
            ("order_by", "rate_per_day".to_string()),
            ("position", "DESC".to_string()),
        ];

        if let Some(current_cursor) = &cursor {
            parameters.push(("next_cursor", current_cursor.clone()));
        }

        let page = fetch_reward_page_with_retry(http, config, &parameters, page_number).await?;

        let received_count = page.data.len();
        crate::log_info!("console", "  Reward sayfası {page_number}: {received_count} market");
        all_markets.extend(page.data);

        if page.next_cursor == "LTE=" || page.next_cursor.is_empty() || received_count == 0 {
            return Ok(all_markets);
        }

        if !seen_cursors.insert(page.next_cursor.clone()) {
            bail!(
                "Reward pagination aynı cursor'ı tekrar döndürdü: {}",
                page.next_cursor
            );
        }

        cursor = Some(page.next_cursor);
    }

    bail!(
        "Reward pagination güvenlik sınırına ulaştı ({} sayfa, sayfa başına {}). \
         Eksik tarama yapılmadı; cursor davranışı kontrol edilmeli.",
        MAX_REWARD_PAGE_SAFETY,
        REWARD_PAGE_SIZE
    )
}

async fn fetch_reward_page_with_retry(
    http: &Client,
    config: &ScannerConfig,
    parameters: &[(&str, String)],
    page_number: usize,
) -> Result<RewardMarketsPage> {
    for attempt in 1..=HTTP_RETRY_ATTEMPTS {
        let response = match http
            .get(endpoint_url(&config.clob_host, "/rewards/markets/multi"))
            .query(parameters)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if attempt < HTTP_RETRY_ATTEMPTS => {
                let delay = retry_delay(attempt, None);
                crate::log_warn!("warning", 
                    "Reward sayfası {page_number} ağ hatası ({attempt}/{}): {error}; \
                     {} ms sonra tekrar denenecek.",
                    HTTP_RETRY_ATTEMPTS,
                    delay.as_millis()
                );
                sleep(delay).await;
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("Reward market sayfası {page_number} alınamadı"));
            }
        };

        let status = response.status();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs);

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();

            if retryable_status(status) && attempt < HTTP_RETRY_ATTEMPTS {
                let delay = retry_delay(attempt, retry_after);
                crate::log_warn!("warning", 
                    "Reward sayfası {page_number} HTTP {status} ({attempt}/{}): {}; \
                     {} ms sonra tekrar denenecek.",
                    HTTP_RETRY_ATTEMPTS,
                    body.chars().take(240).collect::<String>(),
                    delay.as_millis()
                );
                sleep(delay).await;
                continue;
            }

            bail!(
                "Reward market endpoint'i sayfa {page_number} için HTTP {status} döndürdü: {}",
                body.chars().take(500).collect::<String>()
            );
        }

        match response.json::<RewardMarketsPage>().await {
            Ok(page) => return Ok(page),
            Err(error) if attempt < HTTP_RETRY_ATTEMPTS => {
                let delay = retry_delay(attempt, None);
                crate::log_warn!("warning", 
                    "Reward sayfası {page_number} JSON hatası ({attempt}/{}): {error}; \
                     {} ms sonra tekrar denenecek.",
                    HTTP_RETRY_ATTEMPTS,
                    delay.as_millis()
                );
                sleep(delay).await;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("Reward market sayfası {page_number} çözümlenemedi"));
            }
        }
    }

    unreachable!("HTTP_RETRY_ATTEMPTS en az 1 olmalı")
}

fn build_preliminary_candidate(
    market: RewardMarket,
    now: &DateTime<Utc>,
    config: &ScannerConfig,
) -> std::result::Result<PreliminaryCandidate, PreliminaryReject> {
    let end_date = market
        .end_date
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(PreliminaryReject::MissingEndDate)?;

    let end_at = parse_datetime(end_date)
        .ok_or_else(|| PreliminaryReject::InvalidEndDate(end_date.to_string()))?;

    let seconds_to_end = end_at.timestamp() - now.timestamp();

    if seconds_to_end <= 0 {
        return Err(PreliminaryReject::EndDateRange);
    }

    let days_to_end = seconds_to_end as f64 / 86_400.0;

    if days_to_end < config.min_days_to_end || days_to_end > config.max_days_to_end {
        return Err(PreliminaryReject::EndDateRange);
    }

    let price_change_24h = market.one_day_price_change.unwrap_or_default().abs();

    if price_change_24h > config.max_abs_price_change_24h {
        return Err(PreliminaryReject::PriceChange);
    }

    if market.volume_24hr.unwrap_or_default() < config.min_volume_24h {
        return Err(PreliminaryReject::Volume);
    }

    // Bu yalnızca hızlı ön filtredir; asıl spread gerçek order book'tan hesaplanır.
    if market.spread.unwrap_or_default() > config.max_live_spread * 2.0 {
        return Err(PreliminaryReject::ApiSpread);
    }

    let reward_per_day = active_reward_per_day(&market.rewards_config, now);

    if reward_per_day <= 0.0 {
        return Err(PreliminaryReject::NoActiveReward);
    }

    let reward_min_size = market.rewards_min_size.unwrap_or_default();

    if reward_min_size <= 0.0 || reward_min_size > config.max_reward_min_size {
        return Err(PreliminaryReject::RewardMinSize);
    }

    // Polymarket rewards_max_spread alanı cent cinsindedir.
    let reward_max_spread =
        reward_spread_cents_to_price(market.rewards_max_spread.unwrap_or_default());

    if reward_max_spread < config.min_reward_max_spread {
        return Err(PreliminaryReject::RewardMaxSpread);
    }

    if market.tokens.len() < 2 {
        return Err(PreliminaryReject::MissingTokens);
    }

    let primary_token_id = market
        .tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("yes"))
        .or_else(|| market.tokens.first())
        .map(|token| token.token_id.clone())
        .filter(|token_id| !token_id.is_empty())
        .ok_or(PreliminaryReject::MissingPrimaryToken)?;

    Ok(PreliminaryCandidate {
        market,
        end_at,
        days_to_end,
        reward_per_day,
        reward_min_size,
        reward_max_spread,
        primary_token_id,
    })
}

fn preliminary_score(candidate: &PreliminaryCandidate) -> f64 {
    let spread = candidate.market.spread.unwrap_or(0.10).max(0.001);

    let change = candidate
        .market
        .one_day_price_change
        .unwrap_or_default()
        .abs();

    let competition = candidate
        .market
        .market_competitiveness
        .unwrap_or(0.5)
        .max(0.0);

    let horizon_bonus = (candidate.days_to_end / 14.0).sqrt().clamp(1.0, 4.0);
    let reward_band_bonus = (candidate.reward_max_spread * 100.0).clamp(1.0, 10.0);

    candidate.reward_per_day * horizon_bonus * reward_band_bonus
        / (1.0 + spread * 12.0 + change * 12.0 + competition)
}

async fn fetch_order_books(
    http: &Client,
    token_ids: &[String],
    config: &ScannerConfig,
) -> Result<HashMap<String, BookSnapshot>> {
    let batches = token_ids
        .chunks(BOOK_BATCH_SIZE)
        .enumerate()
        .map(|(index, chunk)| (index, chunk.to_vec()))
        .collect::<Vec<_>>();

    let total_batches = batches.len();
    let pacer = RequestPacer::new(Duration::from_millis(
        BOOK_REQUEST_MIN_INTERVAL_MILLISECONDS,
    ));

    let mut completed = stream::iter(batches)
        .map(|(batch_index, token_ids)| {
            let pacer = pacer.clone();

            async move {
                let result = fetch_order_book_batch_with_retry(
                    http,
                    &token_ids,
                    config,
                    batch_index + 1,
                    &pacer,
                )
                .await;
                (batch_index, token_ids.len(), result)
            }
        })
        .buffer_unordered(BOOK_FETCH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    completed.sort_by_key(|(batch_index, _, _)| *batch_index);

    let mut result = HashMap::new();

    for (batch_index, token_count, batch_result) in completed {
        crate::log_info!("console", 
            "  Order book paketi {}/{} — {} token",
            batch_index + 1,
            total_batches,
            token_count
        );

        let books = batch_result.with_context(|| {
            format!(
                "Order book paketi {}/{} kalıcı olarak başarısız",
                batch_index + 1,
                total_batches
            )
        })?;

        for book in books {
            result.insert(book.asset_id.clone(), book);
        }
    }

    Ok(result)
}

async fn fetch_order_book_batch_with_retry(
    http: &Client,
    token_ids: &[String],
    config: &ScannerConfig,
    batch_number: usize,
    pacer: &RequestPacer,
) -> Result<Vec<BookSnapshot>> {
    let body = token_ids
        .iter()
        .map(|token_id| BookRequest {
            token_id: token_id.clone(),
        })
        .collect::<Vec<_>>();

    for attempt in 1..=HTTP_RETRY_ATTEMPTS {
        pacer.wait().await;

        let response = match http
            .post(endpoint_url(&config.clob_host, "/books"))
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if attempt < HTTP_RETRY_ATTEMPTS => {
                let delay = retry_delay(attempt, None);
                crate::log_warn!("warning", 
                    "Order book paketi {batch_number} ağ hatası ({attempt}/{}): {error}; \
                     {} ms sonra tekrar denenecek.",
                    HTTP_RETRY_ATTEMPTS,
                    delay.as_millis()
                );
                sleep(delay).await;
                continue;
            }
            Err(error) => return Err(error).context("Order book isteği gönderilemedi"),
        };

        let status = response.status();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs);

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();

            if retryable_status(status) && attempt < HTTP_RETRY_ATTEMPTS {
                let delay = retry_delay(attempt, retry_after);
                crate::log_warn!("warning", 
                    "Order book paketi {batch_number} HTTP {status} ({attempt}/{}): {}; \
                     {} ms sonra tekrar denenecek.",
                    HTTP_RETRY_ATTEMPTS,
                    body_text.chars().take(240).collect::<String>(),
                    delay.as_millis()
                );
                sleep(delay).await;
                continue;
            }

            bail!(
                "Order book endpoint'i HTTP {status} döndürdü: {}",
                body_text.chars().take(500).collect::<String>()
            );
        }

        match response.json::<Vec<BookSnapshot>>().await {
            Ok(books) => return Ok(books),
            Err(error) if attempt < HTTP_RETRY_ATTEMPTS => {
                let delay = retry_delay(attempt, None);
                crate::log_warn!("warning", 
                    "Order book paketi {batch_number} JSON hatası ({attempt}/{}): {error}; \
                     {} ms sonra tekrar denenecek.",
                    HTTP_RETRY_ATTEMPTS,
                    delay.as_millis()
                );
                sleep(delay).await;
            }
            Err(error) => return Err(error).context("Order book cevabı çözümlenemedi"),
        }
    }

    unreachable!("HTTP_RETRY_ATTEMPTS en az 1 olmalı")
}

async fn fetch_price_histories(
    http: &Client,
    token_ids: &[String],
    config: &ScannerConfig,
) -> Result<HashMap<String, Vec<HistoryPoint>>> {
    let end_ts = Utc::now().timestamp();
    let start_ts = end_ts - config.history_days * 86_400;
    let batch_size = config.history_batch_size.max(1);

    let batches = token_ids
        .chunks(batch_size)
        .enumerate()
        .map(|(batch_index, chunk)| {
            (
                batch_index,
                BatchHistoryRequest {
                    markets: chunk.to_vec(),
                    start_ts,
                    end_ts,
                    fidelity: config.history_fidelity_minutes,
                },
            )
        })
        .collect::<Vec<_>>();

    let total_batches = batches.len();
    let pacer = RequestPacer::new(Duration::from_millis(
        HISTORY_REQUEST_MIN_INTERVAL_MILLISECONDS,
    ));

    let mut completed = stream::iter(batches)
        .map(|(batch_index, body)| {
            let pacer = pacer.clone();

            async move {
                let token_count = body.markets.len();
                let result = fetch_history_batch_with_retry(
                    http,
                    &body,
                    config,
                    batch_index + 1,
                    &pacer,
                )
                .await;
                (batch_index, token_count, result)
            }
        })
        .buffer_unordered(HISTORY_FETCH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    completed.sort_by_key(|(batch_index, _, _)| *batch_index);

    let mut histories = HashMap::new();
    let mut successful_batches = 0usize;
    let mut failed_batches = 0usize;

    for (batch_index, token_count, result) in completed {
        match result {
            Ok(response) => {
                crate::log_info!("console", 
                    "  Fiyat geçmişi paketi {}/{} — {} token: başarılı",
                    batch_index + 1,
                    total_batches,
                    token_count
                );
                histories.extend(response.history);
                successful_batches += 1;
            }
            Err(error) => {
                failed_batches += 1;
                crate::log_warn!("warning", 
                    "  Fiyat geçmişi paketi {}/{} — {} token: atlandı: {error:#}",
                    batch_index + 1,
                    total_batches,
                    token_count
                );
            }
        }
    }

    crate::log_info!("console", 
        "Fiyat geçmişi paketleri tamamlandı: {} başarılı, {} başarısız.",
        successful_batches, failed_batches
    );

    if successful_batches == 0 {
        return Err(anyhow!(
            "Hiçbir fiyat geçmişi paketi alınamadı. Ağ bağlantısını veya endpoint durumunu kontrol et."
        ));
    }

    Ok(histories)
}

async fn fetch_history_batch_with_retry(
    http: &Client,
    body: &BatchHistoryRequest,
    config: &ScannerConfig,
    batch_number: usize,
    pacer: &RequestPacer,
) -> Result<BatchHistoryResponse> {
    let retry_attempts = config.history_retry_attempts.max(HTTP_RETRY_ATTEMPTS);

    for attempt in 1..=retry_attempts {
        pacer.wait().await;

        let response = match http
            .post(endpoint_url(&config.clob_host, "/batch-prices-history"))
            .timeout(Duration::from_secs(config.history_request_timeout_seconds))
            .json(body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if attempt < retry_attempts => {
                let delay = retry_delay(attempt, None);
                crate::log_warn!("warning", 
                    "Fiyat geçmişi paketi {batch_number} ağ hatası ({attempt}/{retry_attempts}): \
                     {error}; {} ms sonra tekrar denenecek.",
                    delay.as_millis()
                );
                sleep(delay).await;
                continue;
            }
            Err(error) => return Err(error).context("Fiyat geçmişi isteği gönderilemedi"),
        };

        let status = response.status();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs);

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();

            if retryable_status(status) && attempt < retry_attempts {
                let delay = retry_delay(attempt, retry_after);
                crate::log_warn!("warning", 
                    "Fiyat geçmişi paketi {batch_number} HTTP {status} \
                     ({attempt}/{retry_attempts}): {}; {} ms sonra tekrar denenecek.",
                    body_text.chars().take(240).collect::<String>(),
                    delay.as_millis()
                );
                sleep(delay).await;
                continue;
            }

            bail!(
                "Fiyat geçmişi endpoint'i HTTP {status} döndürdü: {}",
                body_text.chars().take(500).collect::<String>()
            );
        }

        match response.json::<BatchHistoryResponse>().await {
            Ok(history) => return Ok(history),
            Err(error) if attempt < retry_attempts => {
                let delay = retry_delay(attempt, None);
                crate::log_warn!("warning", 
                    "Fiyat geçmişi paketi {batch_number} JSON hatası \
                     ({attempt}/{retry_attempts}): {error}; {} ms sonra tekrar denenecek.",
                    delay.as_millis()
                );
                sleep(delay).await;
            }
            Err(error) => {
                return Err(error).context("Fiyat geçmişi cevabı çözümlenemedi");
            }
        }
    }

    unreachable!("retry_attempts en az 1 olmalı")
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status.as_u16(),
        408 | 425 | 429 | 500 | 502 | 503 | 504
    ) || status.is_server_error()
}

fn retry_delay(attempt: usize, retry_after: Option<Duration>) -> Duration {
    if let Some(retry_after) = retry_after {
        return retry_after.min(Duration::from_secs(30));
    }

    let exponent = attempt.saturating_sub(1).min(4) as u32;
    let base = HTTP_RETRY_BASE_MILLISECONDS
        .saturating_mul(1_u64 << exponent)
        .min(HTTP_RETRY_MAX_MILLISECONDS);

    // Deterministik jitter; paralel isteklerin aynı milisaniyede tekrar
    // gönderilmesini azaltır ve yeni bağımlılık gerektirmez.
    let jitter = ((attempt as u64 * 97) % 151).min(HTTP_RETRY_MAX_MILLISECONDS - base);
    Duration::from_millis(base.saturating_add(jitter))
}

fn calculate_book_metrics(
    book: &BookSnapshot,
    reward_band: f64,
    quote_inside_ticks: u32,
    buy_quotes_only: bool,
    protective_depth_level_cap: f64,
) -> Option<BookMetrics> {
    let best_bid = book
        .bids
        .iter()
        .filter(|level| level.price.is_finite() && level.size > 0.0)
        .max_by(|left, right| left.price.total_cmp(&right.price))?;

    let best_ask = book
        .asks
        .iter()
        .filter(|level| level.price.is_finite() && level.size > 0.0)
        .min_by(|left, right| left.price.total_cmp(&right.price))?;

    let spread = best_ask.price - best_bid.price;

    if !spread.is_finite() || spread <= 0.0 || spread > 1.0 {
        return None;
    }

    let midpoint = (best_bid.price + best_ask.price) / 2.0;
    let tick_size = book.tick_size.unwrap_or(0.01).clamp(0.0001, 0.10);

    let mut quote_candidates = Vec::new();

    if let Some(planned_bid) = calculate_reward_edge_buy_price(
        best_bid.price,
        best_ask.price,
        tick_size,
        reward_band,
        quote_inside_ticks,
    ) {
        let bid_distance = midpoint - planned_bid;

        // Yalnızca bizim fiyatımızdan daha iyi BUY seviyeleri koruma sayılır.
        let protective_depth = effective_protective_depth(
            book.bids
                .iter()
                .filter(|level| level.size > 0.0 && level.price > planned_bid + 1e-9)
                .map(|level| level.size),
            protective_depth_level_cap,
        );

        quote_candidates.push(BookQuoteCandidate {
            side: QuoteSide::Buy,
            planned_price: planned_bid,
            reference_best_price: best_bid.price,
            protective_depth,
            distance_from_midpoint: bid_distance,
            tick_size,
        });
    }

    if !buy_quotes_only {
        if let Some(planned_ask) = calculate_reward_edge_sell_price(
            best_bid.price,
            best_ask.price,
            tick_size,
            reward_band,
            quote_inside_ticks,
        ) {
            let ask_distance = planned_ask - midpoint;

            // Yalnızca bizim fiyatımızdan daha iyi SELL seviyeleri koruma sayılır.
            let protective_depth = effective_protective_depth(
                book.asks
                    .iter()
                    .filter(|level| level.size > 0.0 && level.price + 1e-9 < planned_ask)
                    .map(|level| level.size),
                protective_depth_level_cap,
            );

            quote_candidates.push(BookQuoteCandidate {
                side: QuoteSide::Sell,
                planned_price: planned_ask,
                reference_best_price: best_ask.price,
                protective_depth,
                distance_from_midpoint: ask_distance,
                tick_size,
            });
        }
    }

    let qualifying_bid_depth = book
        .bids
        .iter()
        .filter(|level| {
            level.size > 0.0 && midpoint >= level.price && midpoint - level.price < reward_band
        })
        .map(|level| level.size)
        .sum::<f64>();

    let qualifying_ask_depth = book
        .asks
        .iter()
        .filter(|level| {
            level.size > 0.0 && level.price >= midpoint && level.price - midpoint < reward_band
        })
        .map(|level| level.size)
        .sum::<f64>();

    Some(BookMetrics {
        midpoint,
        spread,
        qualifying_depth: qualifying_bid_depth + qualifying_ask_depth,
        quote_candidates,
    })
}

/// Canlı order book için reward sınırındaki BUY fiyatını hesaplar.
///
/// `inside_ticks = 0` tam Max Spread sınırını hedefler. Resmi reward
/// formülünde sınırdaki pozisyon skoru sıfır olduğundan varsayılan strateji
/// bir tick içeriyi (`inside_ticks = 1`) kullanır.
///
/// Güvenlik için üretilen fiyatın:
/// - best bid'den düşük,
/// - best ask'ten düşük,
/// - midpoint'in BUY tarafında,
/// - reward bandının içinde
/// olması gerekir.
pub fn calculate_reward_edge_buy_price(
    best_bid: f64,
    best_ask: f64,
    tick_size: f64,
    reward_band: f64,
    inside_ticks: u32,
) -> Option<f64> {
    if !best_bid.is_finite()
        || !best_ask.is_finite()
        || !tick_size.is_finite()
        || !reward_band.is_finite()
        || best_bid <= 0.0
        || best_ask >= 1.0
        || best_bid >= best_ask
        || tick_size <= 0.0
        || reward_band <= 0.0
    {
        return None;
    }

    let midpoint = (best_bid + best_ask) / 2.0;
    let raw_price = midpoint - reward_band + inside_ticks as f64 * tick_size;

    // BUY tarafında aşağı yuvarlamak reward bandının dışına çıkarabilir;
    // bu nedenle en yakın geçerli tick'e yukarı yuvarlanır.
    let price = round_up_to_tick(raw_price, tick_size);
    let distance = midpoint - price;

    if price <= 0.0
        || price + 1e-9 >= best_bid
        || price + 1e-9 >= best_ask
        || distance < -1e-9
        || distance > reward_band + 1e-9
    {
        return None;
    }

    Some(price)
}

fn calculate_reward_edge_sell_price(
    best_bid: f64,
    best_ask: f64,
    tick_size: f64,
    reward_band: f64,
    inside_ticks: u32,
) -> Option<f64> {
    if !best_bid.is_finite()
        || !best_ask.is_finite()
        || !tick_size.is_finite()
        || !reward_band.is_finite()
        || best_bid <= 0.0
        || best_ask >= 1.0
        || best_bid >= best_ask
        || tick_size <= 0.0
        || reward_band <= 0.0
    {
        return None;
    }

    let midpoint = (best_bid + best_ask) / 2.0;
    let raw_price = midpoint + reward_band - inside_ticks as f64 * tick_size;

    // SELL tarafında yukarı yuvarlamak reward bandının dışına çıkarabilir;
    // bu nedenle en yakın geçerli tick'e aşağı yuvarlanır.
    let price = round_down_to_tick(raw_price, tick_size);
    let distance = price - midpoint;

    if price >= 1.0
        || price <= best_ask + 1e-9
        || price <= best_bid + 1e-9
        || distance < -1e-9
        || distance > reward_band + 1e-9
    {
        return None;
    }

    Some(price)
}

fn round_up_to_tick(value: f64, tick_size: f64) -> f64 {
    if tick_size <= 0.0 {
        return value;
    }

    (((value / tick_size) - 1e-10).ceil() * tick_size).clamp(0.0, 1.0)
}

fn round_down_to_tick(value: f64, tick_size: f64) -> f64 {
    if tick_size <= 0.0 {
        return value;
    }

    (((value / tick_size) + 1e-10).floor() * tick_size).clamp(0.0, 1.0)
}

fn quote_opportunity_score(quote: &QuoteOpportunity, reward_band: f64) -> f64 {
    let closeness = if reward_band > 0.0 {
        (1.0 - quote.distance_from_midpoint / reward_band).clamp(0.0, 1.0)
    } else {
        0.0
    };

    quote.protective_depth.ln_1p() * (0.25 + closeness * 0.75)
}

fn calculate_history_stats(points: &[HistoryPoint], max_gap_seconds: i64) -> Option<HistoryStats> {
    let mut values = points
        .iter()
        .filter(|point| point.p.is_finite() && (0.0..=1.0).contains(&point.p))
        .map(|point| (point.t, point.p))
        .collect::<Vec<_>>();

    if values.len() < 2 {
        return None;
    }

    values.sort_by_key(|point| point.0);
    values.dedup_by_key(|point| point.0);

    let minimum = values
        .iter()
        .map(|point| point.1)
        .fold(f64::INFINITY, f64::min);

    let maximum = values
        .iter()
        .map(|point| point.1)
        .fold(f64::NEG_INFINITY, f64::max);

    let mut squared_changes = 0.0;
    let mut max_jump = 0.0_f64;
    let mut change_count = 0usize;

    for window in values.windows(2) {
        let elapsed_seconds = window[1].0 - window[0].0;

        if elapsed_seconds <= 0 || elapsed_seconds > max_gap_seconds {
            continue;
        }

        let change = (window[1].1 - window[0].1).abs();

        max_jump = max_jump.max(change);
        squared_changes += change * change;
        change_count += 1;
    }

    let rms_change = if change_count > 0 {
        (squared_changes / change_count as f64).sqrt()
    } else {
        0.0
    };

    let latest = values.last().copied()?;
    let target_timestamp = latest.0 - 86_400;

    let price_24h_ago = values
        .iter()
        .min_by_key(|point| (point.0 - target_timestamp).abs())
        .filter(|point| (point.0 - target_timestamp).abs() <= 3 * 3_600)
        .map(|point| point.1);

    let change_24h = price_24h_ago.map(|old_price| latest.1 - old_price);

    Some(HistoryStats {
        point_count: values.len(),
        range: maximum - minimum,
        max_jump,
        rms_change,
        change_24h,
    })
}

fn active_reward_per_day(configurations: &[RewardConfig], now: &DateTime<Utc>) -> f64 {
    configurations
        .iter()
        .filter(|configuration| {
            let started = match configuration.start_date.as_deref() {
                None => true,
                Some(value) => parse_datetime(value)
                    .map(|start| now.timestamp() >= start.timestamp())
                    .unwrap_or(false),
            };

            let not_ended = match configuration.end_date.as_deref() {
                None => true,
                Some(value) => parse_datetime(value)
                    .map(|end| now.timestamp() <= end.timestamp())
                    .unwrap_or(false),
            };

            started && not_ended
        })
        .filter_map(|configuration| configuration.rate_per_day)
        .filter(|rate| rate.is_finite() && *rate > 0.0)
        .sum()
}

fn reward_spread_cents_to_price(value_in_cents: f64) -> f64 {
    (value_in_cents / 100.0).clamp(0.0, 1.0)
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim().trim_matches('"').trim();

    if value.is_empty() {
        return None;
    }

    // Bazı API cevapları tarihi Unix saniye/milisaniye olarak döndürebilir.
    if let Some(timestamp) = parse_unix_timestamp(value) {
        return Some(timestamp);
    }

    // RFC3339 örnekleri:
    // 2026-08-10T00:00:00Z
    // 2026-08-10T00:00:00.000Z
    // 2026-08-10T00:00:00+00:00
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Some(parsed.with_timezone(&Utc));
    }

    // PostgreSQL benzeri cevapları RFC3339 biçimine yaklaştır:
    // 2026-08-10 00:00:00+00:00 -> 2026-08-10T00:00:00+00:00
    // 2026-08-10 00:00:00 +00:00 -> 2026-08-10T00:00:00+00:00
    // 2026-08-10T00:00:00+00 -> 2026-08-10T00:00:00+00:00
    let normalized = normalize_datetime_string(value);

    if normalized != value {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(&normalized) {
            return Some(parsed.with_timezone(&Utc));
        }
    }

    // Saat dilimi bulunan fakat RFC3339'e tam uymayan yaygın biçimler.
    const OFFSET_FORMATS: &[&str] = &[
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%.f %:z",
        "%Y-%m-%dT%H:%M:%S%.f %:z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%dT%H:%M:%S%.f%z",
    ];

    for format in OFFSET_FORMATS {
        if let Ok(parsed) = DateTime::parse_from_str(value, format) {
            return Some(parsed.with_timezone(&Utc));
        }
    }

    // Saat dilimi yoksa UTC kabul ediyoruz.
    const NAIVE_DATETIME_FORMATS: &[&str] = &[
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y/%m/%d %H:%M:%S%.f",
        "%Y/%m/%dT%H:%M:%S%.f",
    ];

    for format in NAIVE_DATETIME_FORMATS {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, format) {
            return Some(parsed.and_utc());
        }
    }

    const DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%Y/%m/%d"];

    for format in DATE_FORMATS {
        if let Ok(parsed) = NaiveDate::parse_from_str(value, format) {
            return parsed
                .and_hms_opt(0, 0, 0)
                .map(|datetime| datetime.and_utc());
        }
    }

    // Son güvenli fallback: değer YYYY-MM-DD ile başlıyorsa yalnızca
    // tarih bölümünü kullan. Saat biçimi değişse bile market tamamen elenmez.
    if let Some(date_prefix) = value.get(..10) {
        if let Ok(parsed) = NaiveDate::parse_from_str(date_prefix, "%Y-%m-%d") {
            return parsed
                .and_hms_opt(0, 0, 0)
                .map(|datetime| datetime.and_utc());
        }
    }

    None
}

fn normalize_datetime_string(value: &str) -> String {
    let mut normalized = value.trim().to_string();

    // Tarih ile saat arasındaki ilk boşluğu T yap.
    if normalized.len() > 10 && normalized.as_bytes().get(10) == Some(&b' ') {
        normalized.replace_range(10..11, "T");
    }

    // Saat ile timezone arasındaki fazladan boşluğu kaldır.
    if let Some(index) = normalized.rfind(" +") {
        normalized.remove(index);
    } else if let Some(index) = normalized.rfind(" -") {
        normalized.remove(index);
    }

    // +00 veya -05 gibi yalnızca saat içeren offset'i +00:00 / -05:00 yap.
    if normalized.len() >= 3 {
        let suffix = &normalized[normalized.len() - 3..];
        let bytes = suffix.as_bytes();

        if matches!(bytes[0], b'+' | b'-') && bytes[1].is_ascii_digit() && bytes[2].is_ascii_digit()
        {
            normalized.push_str(":00");
        }
    }

    normalized
}

fn parse_unix_timestamp(value: &str) -> Option<DateTime<Utc>> {
    // Tarih biçimlerindeki tire/iki nokta nedeniyle yalnızca saf sayıları dene.
    if !value
        .chars()
        .all(|character| character.is_ascii_digit() || character == '.')
    {
        return None;
    }

    let numeric = value.parse::<f64>().ok()?;

    if !numeric.is_finite() || numeric <= 0.0 {
        return None;
    }

    // Büyüklüğe göre saniye, milisaniye, mikrosaniye veya nanosaniye kabul et.
    let seconds = if numeric >= 1_000_000_000_000_000_000.0 {
        numeric / 1_000_000_000.0
    } else if numeric >= 1_000_000_000_000_000.0 {
        numeric / 1_000_000.0
    } else if numeric >= 1_000_000_000_000.0 {
        numeric / 1_000.0
    } else {
        numeric
    };

    let mut whole_seconds = seconds.trunc() as i64;
    let mut nanoseconds = ((seconds.fract().abs()) * 1_000_000_000.0).round() as u32;

    if nanoseconds >= 1_000_000_000 {
        whole_seconds += 1;
        nanoseconds = 0;
    }

    Utc.timestamp_opt(whole_seconds, nanoseconds).single()
}

fn bounds<I>(values: I) -> (f64, f64)
where
    I: Iterator<Item = f64>,
{
    let mut minimum = f64::INFINITY;
    let mut maximum = f64::NEG_INFINITY;

    for value in values.filter(|value| value.is_finite()) {
        minimum = minimum.min(value);
        maximum = maximum.max(value);
    }

    if !minimum.is_finite() || !maximum.is_finite() {
        return (0.0, 0.0);
    }

    (minimum, maximum)
}

fn normalize_high(value: f64, bounds: (f64, f64)) -> f64 {
    let (minimum, maximum) = bounds;

    if (maximum - minimum).abs() < f64::EPSILON {
        return 0.5;
    }

    ((value - minimum) / (maximum - minimum)).clamp(0.0, 1.0)
}

fn normalize_low(value: f64, bounds: (f64, f64)) -> f64 {
    1.0 - normalize_high(value, bounds)
}

fn effective_protective_depth<I>(sizes: I, level_cap: f64) -> f64
where
    I: Iterator<Item = f64>,
{
    sizes
        .filter(|size| size.is_finite() && *size > 0.0)
        .map(|size| size.min(level_cap))
        .sum()
}

fn endpoint_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn is_http_url(value: &str) -> bool {
    let value = value.trim();
    (value.starts_with("https://") || value.starts_with("http://")) && value.len() > "http://".len()
}

fn deserialize_optional_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;

    Ok(match value {
        None | Some(Value::Null) => None,
        Some(Value::String(string)) => Some(string),
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(Value::Bool(boolean)) => Some(boolean.to_string()),
        Some(other) => Some(other.to_string()),
    })
}

fn deserialize_optional_f64<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.as_ref().and_then(value_to_f64))
}

fn deserialize_f64<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    value_to_f64(&value).ok_or_else(|| D::Error::custom("geçersiz f64 değeri"))
}

fn deserialize_i64<'de, D>(deserializer: D) -> std::result::Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|value| value as i64))
            .ok_or_else(|| D::Error::custom("geçersiz i64 değeri")),

        Value::String(string) => string.parse::<i64>().map_err(D::Error::custom),

        _ => Err(D::Error::custom("geçersiz timestamp değeri")),
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(string) => string.parse::<f64>().ok(),
        _ => None,
    }
}
