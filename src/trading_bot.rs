use std::{collections::BTreeMap, env, str::FromStr, time::Duration};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use polymarket_client_sdk_v2::{
    auth::{LocalSigner, Signer},
    clob::{
        types::{
            request::{BalanceAllowanceRequest, OrdersRequest},
            Amount, AssetType, OrderStatusType, OrderType, Side, SignatureType,
        },
        Client, Config,
    },
    types::{Address, Decimal, U256},
    POLYGON,
};
use reqwest::Client as HttpClient;
use serde_json::{json, Value};
use tokio::{
    net::TcpStream,
    time::{interval, interval_at, sleep, timeout, Instant, MissedTickBehavior},
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::{
    logger::{self, LogLevel, ScoringLogRecord},
    market_scanner::{
        calculate_reward_edge_buy_price, scan_best_markets, RankedMarket, ScannerConfig,
    },
};

const PRICE_SCALE: f64 = 1_000_000.0;
const EPSILON: f64 = 1e-9;

// Emir miktarında maksimum iki ondalık basamak.
const ORDER_SIZE_DECIMALS: usize = 2;

// Emir gönderildikten sonra açık emir listesine yansıması için tolerans.
const ORDER_VISIBILITY_GRACE_SECONDS: u64 = 12;

// İlk order-status kontrolü hemen yapılmayacak.
const ORDER_POLL_INITIAL_DELAY_SECONDS: u64 = 2;

// Emir bu kadar art arda açık emir listesinde bulunamazsa güvenli duruma geç.
const MAX_CONSECUTIVE_ORDER_MISSES: u32 = 5;

// Scoring zamanlamasının varsayılanları. Gerçek değerler .env üzerinden
// değiştirilebilir ve başlangıçta ayrıntılı biçimde loglanır.
const DEFAULT_SCORING_INITIAL_DELAY_SECONDS: u64 = 6;
const DEFAULT_SCORING_GRACE_SECONDS: u64 = 45;
const DEFAULT_MAX_CONSECUTIVE_NOT_SCORING: u32 = 3;
const DEFAULT_SCORING_ERROR_WARN_THRESHOLD: u32 = 3;

// Belirsiz POST sonucu sonrasında aynı emri bulmak için kısa uzlaşma penceresi.
const POST_RECOVERY_ATTEMPTS: usize = 4;
const POST_RECOVERY_DELAY_MILLISECONDS: u64 = 500;

// BUY iptalinden sonra terminal durumu doğrudan doğrulama ayarları.
// Kısmi dolumda yalnızca `size_matched > 0` görülmesi yeterli değildir;
// kalan BUY'ın gerçekten MATCHED veya CANCELED terminal durumuna geçmesi gerekir.
const CANCEL_CONFIRM_ATTEMPTS: usize = 24;
const CANCEL_CONFIRM_DELAY_MILLISECONDS: u64 = 500;
const TARGETED_CANCEL_RETRY_EVERY: usize = 4;

// Otomatik SELL uzlaşması ve miktar karşılaştırma toleransları.
const ORDER_SIZE_MATCH_TOLERANCE: f64 = 0.011;
const SHARE_DUST_TOLERANCE: f64 = 0.005;
const SELL_POST_RECOVERY_ATTEMPTS: usize = 4;
const SELL_POST_RECOVERY_DELAY_MILLISECONDS: u64 = 500;

// Polymarket pUSD ve outcome-token bakiyeleri 6 ondalık temel birim kullanır.
// Bunlar kullanıcı tercihi değil, protokol sabitidir; .env üzerinden değiştirilemez.
const TOKEN_AMOUNT_SCALE: f64 = 1_000_000.0;

// Otomatik pozisyon kapatma zorunludur. Aşağıdaki değerler strateji ayarı değil,
// güvenli uzlaşma için dahili retry/timeout sabitleridir.
const AUTO_SELL_MAX_ATTEMPTS: usize = 8;
const AUTO_SELL_RETRY_MILLISECONDS: u64 = 500;
const AUTO_SELL_BALANCE_WAIT_ATTEMPTS: usize = 20;
const AUTO_SELL_ORDER_CONFIRM_ATTEMPTS: usize = 20;
const AUTO_SELL_ORDER_CONFIRM_MILLISECONDS: u64 = 500;

// Kullanıcı tercihi olmayan operasyonel sabitler.
// WebSocket canlı veri kaynağıdır; REST snapshot yalnızca bütünlük denetimi ve
// kopma/kaçırılmış event durumunda uzlaşma için düşük frekansta kullanılır.
const BALANCE_SAFETY_RATIO: f64 = 0.95;
const WEBSOCKET_PING_SECONDS: u64 = 10;
const WEBSOCKET_PONG_TIMEOUT_SECONDS: u64 = 30;
const WEBSOCKET_SNAPSHOT_TIMEOUT_SECONDS: u64 = 10;
const MARKET_DATA_STALE_SECONDS: u64 = 45;
const HEALTH_CHECK_MILLISECONDS: u64 = 250;
const BOOK_RESYNC_SECONDS: u64 = 15;
const BOOK_REST_TIMEOUT_SECONDS: u64 = 5;
const BOOK_REST_RETRY_ATTEMPTS: usize = 3;
const BOOK_REST_RETRY_BASE_MILLISECONDS: u64 = 250;

// Başarılı lifecycle sonrasında hızlı yeniden tarama; boş sonuçta kısa bekleme.
// Ağ/API hatalarında 429/5xx yükünü büyütmemek için üstel geri çekilme kullanılır.
const FAST_RESCAN_DELAY_MILLISECONDS: u64 = 250;
const NO_OPPORTUNITY_RESCAN_DELAY_MILLISECONDS: u64 = 1_500;
const FAILURE_BACKOFF_BASE_MILLISECONDS: u64 = 500;
const FAILURE_BACKOFF_MAX_MILLISECONDS: u64 = 15_000;

type MarketSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone)]
struct BotConfig {
    live_trading: bool,
    clob_host: String,
    market_ws_url: String,

    quote_inside_ticks: u32,
    min_protective_depth: f64,
    protective_depth_level_cap: f64,
    protective_depth_min_retention_ratio: f64,
    protective_depth_breach_confirmations: u32,
    reprice_tolerance_ticks: f64,
    cancel_min_distance_ticks: f64,
    max_live_spread: f64,

    max_order_usd: f64,
    order_poll_milliseconds: u64,
    scoring_initial_delay_seconds: u64,
    scoring_grace_seconds: u64,
    scoring_check_seconds: u64,
    max_consecutive_not_scoring: u32,
    scoring_error_warn_threshold: u32,

    cancel_all_on_start: bool,
}

impl BotConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            live_trading: env_bool("LIVE_TRADING", false)?,
            clob_host: normalize_base_url(
                &env::var("CLOB_API_URL")
                    .unwrap_or_else(|_| "https://clob.polymarket.com".to_string()),
            ),
            market_ws_url: env::var("MARKET_WS_URL").unwrap_or_else(|_| {
                "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
            }),
            quote_inside_ticks: env_parse("QUOTE_INSIDE_TICKS", 1_u32)?,
            min_protective_depth: env_parse("MIN_PROTECTIVE_DEPTH", 15.0_f64)?,
            protective_depth_level_cap: env_parse("PROTECTIVE_DEPTH_LEVEL_CAP", 50.0_f64)?,
            protective_depth_min_retention_ratio: env_parse(
                "PROTECTIVE_DEPTH_MIN_RETENTION_RATIO",
                0.60_f64,
            )?,
            protective_depth_breach_confirmations: env_parse(
                "PROTECTIVE_DEPTH_BREACH_CONFIRMATIONS",
                2_u32,
            )?,
            reprice_tolerance_ticks: env_parse("REPRICE_TOLERANCE_TICKS", 0.50_f64)?,
            cancel_min_distance_ticks: env_parse("CANCEL_MIN_DISTANCE_TICKS", 0.50_f64)?,
            max_live_spread: env_parse("MAX_LIVE_SPREAD", 0.06_f64)?,
            max_order_usd: env_parse("POLYMARKET_MAX_ORDER_USD", 15.0_f64)?,
            order_poll_milliseconds: env_parse("ORDER_POLL_MILLISECONDS", 1_000_u64)?,
            scoring_initial_delay_seconds: env_parse(
                "SCORING_INITIAL_DELAY_SECONDS",
                DEFAULT_SCORING_INITIAL_DELAY_SECONDS,
            )?,
            scoring_grace_seconds: env_parse(
                "SCORING_GRACE_SECONDS",
                DEFAULT_SCORING_GRACE_SECONDS,
            )?,
            scoring_check_seconds: env_parse("SCORING_CHECK_SECONDS", 5_u64)?,
            max_consecutive_not_scoring: env_parse(
                "MAX_CONSECUTIVE_NOT_SCORING",
                DEFAULT_MAX_CONSECUTIVE_NOT_SCORING,
            )?,
            scoring_error_warn_threshold: env_parse(
                "SCORING_ERROR_WARN_THRESHOLD",
                DEFAULT_SCORING_ERROR_WARN_THRESHOLD,
            )?,
            cancel_all_on_start: env_bool("CANCEL_ALL_ON_START", false)?,
        })
    }

    fn validate(&self) -> Result<()> {
        if !is_http_url(&self.clob_host) {
            bail!(
                "CLOB_API_URL http:// veya https:// ile başlamalı: {}",
                self.clob_host
            );
        }

        if !is_websocket_url(&self.market_ws_url) {
            bail!(
                "MARKET_WS_URL ws:// veya wss:// ile başlamalı: {}",
                self.market_ws_url
            );
        }

        for (name, value) in [
            ("MIN_PROTECTIVE_DEPTH", self.min_protective_depth),
            (
                "PROTECTIVE_DEPTH_LEVEL_CAP",
                self.protective_depth_level_cap,
            ),
            (
                "PROTECTIVE_DEPTH_MIN_RETENTION_RATIO",
                self.protective_depth_min_retention_ratio,
            ),
            ("REPRICE_TOLERANCE_TICKS", self.reprice_tolerance_ticks),
            ("CANCEL_MIN_DISTANCE_TICKS", self.cancel_min_distance_ticks),
            ("MAX_LIVE_SPREAD", self.max_live_spread),
            ("POLYMARKET_MAX_ORDER_USD", self.max_order_usd),
        ] {
            if !value.is_finite() {
                bail!("{name} sonlu bir sayı olmalı");
            }
        }

        if self.min_protective_depth < 0.0 || self.protective_depth_level_cap <= 0.0 {
            bail!("Koruyucu derinlik değerleri geçersiz");
        }

        if self.protective_depth_breach_confirmations == 0 {
            bail!("PROTECTIVE_DEPTH_BREACH_CONFIRMATIONS sıfır olamaz");
        }

        if !(0.0..=1.0).contains(&self.protective_depth_min_retention_ratio) {
            bail!("PROTECTIVE_DEPTH_MIN_RETENTION_RATIO 0 ile 1 arasında olmalı");
        }

        if self.reprice_tolerance_ticks <= 0.0 || self.cancel_min_distance_ticks < 0.0 {
            bail!("Reprice ve minimum mesafe tick değerleri geçersiz");
        }

        if !(0.0..1.0).contains(&self.max_live_spread) {
            bail!("MAX_LIVE_SPREAD 0 ile 1 arasında olmalı");
        }

        if self.max_order_usd <= 0.0 {
            bail!("POLYMARKET_MAX_ORDER_USD sıfırdan büyük olmalı");
        }

        if self.order_poll_milliseconds == 0
            || self.scoring_check_seconds == 0
            || self.scoring_grace_seconds == 0
            || self.max_consecutive_not_scoring == 0
            || self.scoring_error_warn_threshold == 0
        {
            bail!(
                "ORDER_POLL_MILLISECONDS, SCORING_CHECK_SECONDS, SCORING_GRACE_SECONDS, \
                 MAX_CONSECUTIVE_NOT_SCORING ve SCORING_ERROR_WARN_THRESHOLD sıfır olamaz"
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct LiveQuote {
    market: RankedMarket,
    price: f64,
    size: f64,
    estimated_cost: f64,
    book: LocalBook,

    // Emir gönderilmeden hemen önceki canlı book'ta, bizim fiyatımızdan
    // kesin olarak daha iyi olan BUY fiyat seviyesi sayısı. Bu sayı emir
    // açık kaldığı sürece sabit tutulur.
    initial_better_bid_levels: usize,

    // Aynı anda ölçülen, seviye başına cap uygulanmış koruyucu miktar.
    initial_effective_protective_depth: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MonitorOutcome {
    Requote,
    Shutdown,
    FillDetected,
    OrderClosed,
    OrderMissing,
}

impl MonitorOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Requote => "REQUOTE",
            Self::Shutdown => "SHUTDOWN",
            Self::FillDetected => "FILL_DETECTED",
            Self::OrderClosed => "ORDER_CLOSED",
            Self::OrderMissing => "ORDER_MISSING",
        }
    }
}

#[derive(Debug, Clone)]
struct LocalBook {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    tick_size: f64,
    min_order_size: f64,
    snapshot_received: bool,
    resolved: bool,
    last_event_timestamp_ms: Option<i64>,
    last_hash: Option<String>,
}

impl Default for LocalBook {
    fn default() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            tick_size: 0.01,
            min_order_size: 0.0,
            snapshot_received: false,
            resolved: false,
            last_event_timestamp_ms: None,
            last_hash: None,
        }
    }
}

impl LocalBook {
    fn best_bid(&self) -> Option<f64> {
        self.bids
            .last_key_value()
            .map(|(price, _)| key_to_price(*price))
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks
            .first_key_value()
            .map(|(price, _)| key_to_price(*price))
    }

    fn midpoint(&self) -> Option<f64> {
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }

    fn spread(&self) -> Option<f64> {
        Some(self.best_ask()? - self.best_bid()?)
    }

    fn better_bid_level_count(&self, order_price: f64) -> usize {
        let order_key = price_to_key(order_price);
        self.bids.range((order_key + 1)..).count()
    }

    fn better_bid_depth(&self, order_price: f64) -> f64 {
        let order_key = price_to_key(order_price);
        self.bids
            .range((order_key + 1)..)
            .map(|(_, size)| *size)
            .sum()
    }

    fn effective_better_bid_depth(&self, order_price: f64, level_cap: f64) -> f64 {
        let order_key = price_to_key(order_price);
        self.bids
            .range((order_key + 1)..)
            .map(|(_, size)| (*size).min(level_cap))
            .sum()
    }

    fn replace_side(&mut self, side: &str, levels: &[Value]) {
        let target = if side.eq_ignore_ascii_case("BUY") {
            &mut self.bids
        } else {
            &mut self.asks
        };

        target.clear();
        for level in levels {
            let Some(price) = value_f64(level.get("price")) else {
                continue;
            };
            let Some(size) = value_f64(level.get("size")) else {
                continue;
            };
            if price.is_finite() && size.is_finite() && size > 0.0 {
                target.insert(price_to_key(price), size);
            }
        }
    }

    fn update_level(&mut self, side: &str, price: f64, size: f64) {
        let target = if side.eq_ignore_ascii_case("BUY") {
            &mut self.bids
        } else if side.eq_ignore_ascii_case("SELL") {
            &mut self.asks
        } else {
            return;
        };

        let key = price_to_key(price);
        if size <= 0.0 {
            target.remove(&key);
        } else {
            target.insert(key, size);
        }
    }
}

#[derive(Debug)]
enum ApplyMarketResult {
    Applied,
    Ignored,
    RequiresResync(String),
}

#[derive(Debug)]
struct QuoteProtectionGuard {
    initial_better_bid_levels: usize,
    initial_effective_depth: f64,
    consecutive_relative_depth_breaches: u32,
}

impl QuoteProtectionGuard {
    fn new(quote: &LiveQuote) -> Self {
        Self {
            initial_better_bid_levels: quote.initial_better_bid_levels,
            initial_effective_depth: quote.initial_effective_protective_depth,
            consecutive_relative_depth_breaches: 0,
        }
    }

    fn evaluate(
        &mut self,
        book: &LocalBook,
        order_price: f64,
        config: &BotConfig,
        confirm_relative_depth_breach: bool,
    ) -> Option<String> {
        let current_better_levels = book.better_bid_level_count(order_price);

        // Fiyat-seviyesi sırası kilitlidir. Emir açılırken önümüzde kaç farklı
        // BUY fiyat seviyesi varsa bu sayı bir artarsa veya azalırsa emir artık
        // taramanın seçtiği seviyede değildir; beklemeden iptal/rescan yapılır.
        if current_better_levels != self.initial_better_bid_levels {
            return Some(format!(
                "BUY fiyat-seviyesi sırası değişti: önümüzdeki seviye {} -> {}, quote sırası {} -> {}",
                self.initial_better_bid_levels,
                current_better_levels,
                self.initial_better_bid_levels + 1,
                current_better_levels + 1,
            ));
        }

        let current_depth =
            book.effective_better_bid_depth(order_price, config.protective_depth_level_cap);

        // Mutlak tabanın altı doğrudan güvensiz kabul edilir.
        if current_depth + EPSILON < config.min_protective_depth {
            return Some(format!(
                "koruyucu BUY miktarı mutlak tabanın altına düştü: efektif={current_depth:.2} < {:.2} share",
                config.min_protective_depth,
            ));
        }

        // Başlangıçtaki efektif derinliğin belirli bir oranı korunmalıdır.
        // Düşüş sadece miktar değişiminden kaynaklanıyorsa kısa süreli update
        // oynaklığında gereksiz iptali azaltmak için birkaç gerçek book update'i
        // boyunca doğrulanır.
        let relative_floor =
            self.initial_effective_depth * config.protective_depth_min_retention_ratio;
        let required_depth = config.min_protective_depth.max(relative_floor);

        if current_depth + EPSILON >= required_depth {
            self.consecutive_relative_depth_breaches = 0;
            return None;
        }

        if !confirm_relative_depth_breach {
            return None;
        }

        self.consecutive_relative_depth_breaches =
            self.consecutive_relative_depth_breaches.saturating_add(1);

        if self.consecutive_relative_depth_breaches >= config.protective_depth_breach_confirmations
        {
            return Some(format!(
                "koruyucu BUY miktarı başlangıca göre kalıcı biçimde azaldı: başlangıç={:.2}, mevcut={current_depth:.2}, gerekli={required_depth:.2} share, update={}/{}",
                self.initial_effective_depth,
                self.consecutive_relative_depth_breaches,
                config.protective_depth_breach_confirmations,
            ));
        }

        None
    }
}

#[derive(Debug, Default)]
struct FailureBackoff {
    consecutive_failures: u32,
}

impl FailureBackoff {
    fn reset(&mut self) {
        self.consecutive_failures = 0;
    }

    fn next_delay_milliseconds(&mut self) -> u64 {
        let exponent = self.consecutive_failures.min(5);
        let base = FAILURE_BACKOFF_BASE_MILLISECONDS.saturating_mul(1_u64 << exponent);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);

        // Küçük deterministik jitter, birden fazla bot örneğinin aynı anda
        // API'ye yeniden yüklenmesini azaltır; ek rand bağımlılığı gerektirmez.
        let jitter = (self.consecutive_failures as u64 * 137) % 251;
        base.min(FAILURE_BACKOFF_MAX_MILLISECONDS)
            .saturating_add(jitter)
            .min(FAILURE_BACKOFF_MAX_MILLISECONDS)
    }
}

pub async fn run() -> Result<()> {
    // main.rs .env dosyasını zaten yükler. Buradaki tekrar çağrı idempotenttir;
    // dosya yoksa gerçek ortam değişkenleri ve varsayılanlar kullanılabilir.
    let _ = dotenvy::dotenv();

    let bot_config = BotConfig::from_env()?;

    bot_config.validate()?;

    logger::log_event(
        LogLevel::Info,
        "trading_bot",
        "configuration_loaded",
        "Bot yapılandırması doğrulandı",
        json!({
            "live_trading": bot_config.live_trading,
            "clob_host": bot_config.clob_host,
            "market_ws_url": bot_config.market_ws_url,
            "quote_inside_ticks": bot_config.quote_inside_ticks,
            "min_protective_depth": bot_config.min_protective_depth,
            "protective_depth_level_cap": bot_config.protective_depth_level_cap,
            "max_live_spread": bot_config.max_live_spread,
            "max_order_usd": bot_config.max_order_usd,
            "order_poll_milliseconds": bot_config.order_poll_milliseconds,
            "scoring_initial_delay_seconds": bot_config.scoring_initial_delay_seconds,
            "scoring_grace_seconds": bot_config.scoring_grace_seconds,
            "scoring_check_seconds": bot_config.scoring_check_seconds,
            "max_consecutive_not_scoring": bot_config.max_consecutive_not_scoring,
            "scoring_error_warn_threshold": bot_config.scoring_error_warn_threshold,
            "cancel_all_on_start": bot_config.cancel_all_on_start,
        }),
        file!(),
        line!(),
    );

    if bot_config.quote_inside_ticks == 0 {
        crate::log_warn!("warning", 
            "UYARI: QUOTE_INSIDE_TICKS=0 tam reward \
             sınırını hedefler. Sınırdaki emir puan \
             kazanmayabilir."
        );
    }

    let private_key =
        env::var("POLYMARKET_PRIVATE_KEY").context("POLYMARKET_PRIVATE_KEY bulunamadı")?;

    let funder_address =
        env::var("POLYMARKET_FUNDER_ADDRESS").context("POLYMARKET_FUNDER_ADDRESS bulunamadı")?;

    let signature_type_value =
        env::var("POLYMARKET_SIGNATURE_TYPE").unwrap_or_else(|_| "3".to_string());

    let signer = LocalSigner::from_str(private_key.trim())
        .context("POLYMARKET_PRIVATE_KEY geçersiz")?
        .with_chain_id(Some(POLYGON));

    let funder =
        Address::from_str(funder_address.trim()).context("POLYMARKET_FUNDER_ADDRESS geçersiz")?;

    let (signature_type, wallet_type_name) = parse_signature_type(&signature_type_value)?;

    crate::log_info!("console", "Signer / owner adresi (B): {}", signer.address());

    crate::log_info!("console", "Funder / bakiye adresi (A): {funder}");

    crate::log_info!("console", "Cüzdan tipi: {wallet_type_name}");

    crate::log_info!("console", "CLOB host: {}", bot_config.clob_host);

    crate::log_info!("console", "Market WebSocket: {}", bot_config.market_ws_url);

    crate::log_info!("console", 
        "Mod: {}",
        if bot_config.live_trading {
            "CANLI EMİR"
        } else {
            "DRY RUN — emir gönderilmez"
        }
    );

    let book_http = HttpClient::builder()
        .timeout(Duration::from_secs(BOOK_REST_TIMEOUT_SECONDS))
        .user_agent("PolyBot-LiveBook/0.4")
        .build()
        .context("REST order book istemcisi oluşturulamadı")?;

    let mut client = Client::new(&bot_config.clob_host, Config::default())
        .context("Polymarket istemcisi oluşturulamadı")?
        .authentication_builder(&signer)
        .funder(funder)
        .signature_type(signature_type.clone())
        .authenticate()
        .await
        .context(
            "Kimlik doğrulama başarısız. \
             Funder ve signature type değerlerini kontrol et.",
        )?;

    if !client.heartbeats_active() {
        Client::start_heartbeats(&mut client)
            .context("Polymarket order heartbeat başlatılamadı")?;
    }

    crate::log_info!("console", "Order heartbeat aktif: {}", client.heartbeats_active());

    let protocol_version = client
        .version()
        .await
        .context("CLOB protokol sürümü okunamadı")?;

    crate::log_info!("console", "CLOB protokol sürümü: V{protocol_version}");

    if signature_type_value.trim() == "3" && protocol_version != 2 {
        bail!(
            "POLYMARKET_SIGNATURE_TYPE=3 yalnızca CLOB V2 \
             ile kullanılabilir. SDK sürümünü ve \
             CLOB_API_URL=https://clob.polymarket.com \
             ayarını kontrol et."
        );
    }

    if bot_config.cancel_all_on_start && bot_config.live_trading {
        crate::log_info!("console", "Başlangıçta mevcut açık emirler iptal ediliyor...");

        let response = client
            .cancel_all_orders()
            .await
            .context("Başlangıç emirleri iptal edilemedi")?;

        crate::log_info!("console", "Cancel-all cevabı: {response:?}");
    }

    let balance_request = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Collateral)
        .signature_type(signature_type.clone())
        .build();

    let mut shutdown = Box::pin(tokio::signal::ctrl_c());
    let mut failure_backoff = FailureBackoff::default();

    loop {
        crate::log_info!("console", "\nYeni tarama başlatılıyor...");

        let available_pusd = match async {
            client
                .update_balance_allowance(balance_request.clone())
                .await
                .context("Bakiye önbelleği güncellenemedi")?;

            let response = client
                .balance_allowance(balance_request.clone())
                .await
                .context("Bakiye alınamadı")?;

            let raw_balance = response
                .balance
                .to_string()
                .parse::<f64>()
                .context("Bakiye sayıya dönüştürülemedi")?;

            Ok::<f64, anyhow::Error>(raw_balance / TOKEN_AMOUNT_SCALE)
        }
        .await
        {
            Ok(balance) => balance,

            Err(error) => {
                crate::log_warn!("warning", "Bakiye alınamadı: {error:#}");

                let delay_ms = failure_backoff.next_delay_milliseconds();
                crate::log_warn!("warning", "API/ağ hatası sonrası {delay_ms} ms beklenecek.");
                wait_or_shutdown(&mut shutdown, delay_ms).await?;

                continue;
            }
        };

        crate::log_info!("console", 
            "Kullanılabilir pUSD bakiyesi: \
             {available_pusd:.6}"
        );

        let scanner_config = ScannerConfig {
            clob_host: bot_config.clob_host.clone(),

            quote_inside_ticks: bot_config.quote_inside_ticks,

            buy_quotes_only: true,

            min_protective_depth: bot_config.min_protective_depth,

            protective_depth_level_cap: bot_config.protective_depth_level_cap,

            max_live_spread: bot_config.max_live_spread,

            ..ScannerConfig::default()
        };

        /*
        Tarama devam ederken Ctrl+C gelirse taramanın
        tamamlanmasını beklemeden çıkılır.
        */
        let ranked_markets = tokio::select! {
            result = shutdown.as_mut() => {
                result.context(
                    "Ctrl+C sinyali okunamadı"
                )?;

                crate::log_info!("console", 
                    "Kapatma sinyali alındı. \
                     Bot durduruldu."
                );

                return Ok(());
            }

            result = scan_best_markets(
                &scanner_config,
            ) => {
                match result {
                    Ok(markets) => markets,

                    Err(error) => {
                        crate::log_warn!("warning", 
                            "Tarama başarısız: {error:#}"
                        );

                        let delay_ms = failure_backoff.next_delay_milliseconds();
                        crate::log_warn!("warning", "Tarama hatası sonrası {delay_ms} ms beklenecek.");
                        wait_or_shutdown(&mut shutdown, delay_ms).await?;

                        continue;
                    }
                }
            }
        };

        failure_backoff.reset();

        if ranked_markets.is_empty() {
            crate::log_info!("console", "Uygun market bulunamadı.");

            wait_or_shutdown(
                &mut shutdown,
                NO_OPPORTUNITY_RESCAN_DELAY_MILLISECONDS,
            )
            .await?;

            continue;
        }

        /*
        Canlı WebSocket doğrulaması yapılırken de
        Ctrl+C anında yakalanır.
        */
        let selected = tokio::select! {
            result = shutdown.as_mut() => {
                result.context(
                    "Ctrl+C sinyali okunamadı"
                )?;

                crate::log_info!("console", 
                    "Kapatma sinyali alındı. \
                     Bot durduruldu."
                );

                return Ok(());
            }

            result = select_live_quote(
                ranked_markets,
                available_pusd,
                &bot_config,
            ) => result?,
        };

        let Some((quote, socket)) = selected else {
            crate::log_info!("console", 
                "Skorlanan marketler içinde canlı book \
                 ve sermaye sınırlarına uyan emir bulunamadı."
            );

            wait_or_shutdown(
                &mut shutdown,
                NO_OPPORTUNITY_RESCAN_DELAY_MILLISECONDS,
            )
            .await?;

            continue;
        };

        print_selected_quote(&quote, &bot_config);

        logger::log_event(
            LogLevel::Info,
            "trading_bot",
            "quote_selected",
            "Canlı doğrulamadan geçen emir fırsatı seçildi",
            json!({
                "condition_id": quote.market.condition_id,
                "market_slug": quote.market.market_slug,
                "token_id": quote.market.suggested_token_id,
                "outcome": quote.market.suggested_outcome,
                "price": quote.price,
                "size": quote.size,
                "estimated_cost": quote.estimated_cost,
                "score": quote.market.score,
                "reward_per_day": quote.market.reward_per_day,
                "reward_max_spread": quote.market.reward_max_spread,
                "protective_depth": quote.initial_effective_protective_depth,
                "better_bid_levels": quote.initial_better_bid_levels,
                "live_trading": bot_config.live_trading,
            }),
            file!(),
            line!(),
        );

        let order_id = if bot_config.live_trading {
            let price_decimal_places = decimal_places(quote.book.tick_size);
            let price = decimal_from_f64(quote.price, price_decimal_places, "emir fiyatı")?;
            let size = decimal_from_f64(quote.size, ORDER_SIZE_DECIMALS, "emir büyüklüğü")?;

            crate::log_info!("console", 
                "Gönderilecek emir hassasiyeti: \
                 price={} (scale {}), size={} (scale {})",
                price,
                price.scale(),
                size,
                size.scale(),
            );

            if size.scale() > ORDER_SIZE_DECIMALS as u32 {
                bail!(
                    "Emir miktarı hassasiyeti geçersiz: \
                     size={size}, scale={}",
                    size.scale()
                );
            }

            let token_id = U256::from_str(&quote.market.suggested_token_id)
                .context("Token ID U256 olarak okunamadı")?;

            let order = client
                .limit_order()
                .token_id(token_id.clone())
                .price(price)
                .size(size)
                .side(Side::Buy)
                .post_only(true)
                .build()
                .await
                .context("Post-only limit emir oluşturulamadı")?;

            // SDK'nin OrderBuilder klonu heartbeat cancellation token'ını etkileyebildiği
            // için ana client heartbeat görevini emir gönderilmeden önce yeniliyoruz.
            if client.heartbeats_active() {
                client
                    .stop_heartbeats()
                    .await
                    .context("Order builder sonrası eski heartbeat görevi temizlenemedi")?;
            }

            Client::start_heartbeats(&mut client)
                .context("Order builder sonrası heartbeat yeniden başlatılamadı")?;

            let signed = client
                .sign(&signer, order)
                .await
                .context("Emir imzalanamadı")?;

            let recovery_cutoff_ts = Utc::now().timestamp() - 30;
            let mut recovered_order_id: Option<String> = None;

            let response = match client.post_order(signed).await {
                Ok(response) => Some(response),
                Err(error) => {
                    crate::log_warn!("warning", 
                        "Emir gönderme sonucu belirsiz: {error:#}. \
                         Aynı token/fiyat/miktardaki yakın tarihli emir aranıyor."
                    );

                    let request = OrdersRequest::builder().asset_id(token_id.clone()).build();
                    let mut recovered_id: Option<String> = None;

                    for attempt in 1..=POST_RECOVERY_ATTEMPTS {
                        sleep(Duration::from_millis(POST_RECOVERY_DELAY_MILLISECONDS)).await;

                        match client.orders(&request, None).await {
                            Ok(page) => {
                                let candidates = page
                                    .data
                                    .iter()
                                    .filter_map(|order| {
                                        let order_price = parse_nonnegative_number(
                                            &order.price,
                                            "recovered BUY price",
                                        )
                                        .ok()?;
                                        let order_size = parse_nonnegative_number(
                                            &order.original_size,
                                            "recovered BUY original_size",
                                        )
                                        .ok()?;

                                        (matches!(&order.side, Side::Buy)
                                            && order.created_at.timestamp() >= recovery_cutoff_ts
                                            && (order_price - quote.price).abs()
                                                <= quote.book.tick_size / 2.0 + EPSILON
                                            && (order_size - quote.size).abs()
                                                <= ORDER_SIZE_MATCH_TOLERANCE)
                                            .then(|| order.id.clone())
                                    })
                                    .collect::<Vec<_>>();

                                match candidates.as_slice() {
                                    [id] => {
                                        recovered_id = Some(id.clone());
                                        break;
                                    }
                                    [] => {}
                                    _ => {
                                        bail!(
                                            "Belirsiz POST sonrasında birden fazla \
                                             eşleşen açık emir bulundu: {candidates:?}. \
                                             Bot güvenlik için durduruldu."
                                        );
                                    }
                                }
                            }
                            Err(read_error) => {
                                crate::log_warn!("warning", 
                                    "POST uzlaşma sorgusu {attempt}/{} başarısız: \
                                     {read_error:#}",
                                    POST_RECOVERY_ATTEMPTS
                                );
                            }
                        }
                    }

                    let Some(id) = recovered_id else {
                        bail!(
                            "Emir POST sonucu belirsiz ve açık emir kesin olarak \
                             bulunamadı. Yeni emir açılmadı; Polymarket açık \
                             emirlerini elle kontrol et."
                        );
                    };

                    crate::log_info!("console", "Belirsiz POST sonrası emir bulundu: {id}");
                    recovered_order_id = Some(id);
                    None
                }
            };

            if let Some(response) = response {
                crate::log_info!("console", "Emir cevabı: {response:?}");

                if !response.success {
                    crate::log_warn!("warning", 
                        "Emir reddedildi: {}",
                        response
                            .error_msg
                            .as_deref()
                            .unwrap_or("bilinmeyen CLOB hatası")
                    );
                    sleep(Duration::from_millis(FAST_RESCAN_DELAY_MILLISECONDS)).await;
                    continue;
                }

                if response.order_id.trim().is_empty() {
                    bail!(
                        "Başarılı emir cevabında order ID boş. \
                         Bot güvenlik için durduruldu."
                    );
                }

                match &response.status {
                    OrderStatusType::Matched => {
                        crate::log_warn!("warning", 
                            "Post-only BUY gönderim cevabında MATCHED oldu. \
                             Order doğrudan uzlaştırılacak ve dolan miktar otomatik satılacak. \
                             Order ID: {}",
                            response.order_id
                        );
                    }
                    OrderStatusType::Canceled => {
                        crate::log_info!("console", 
                            "BUY emir gönderim sırasında CANCELED oldu. \
                             Olası kısmi dolum doğrudan order endpoint'iyle uzlaştırılacak."
                        );
                    }
                    OrderStatusType::Live
                    | OrderStatusType::Unmatched
                    | OrderStatusType::Delayed => {}
                    other => {
                        crate::log_warn!("warning", 
                            "Emir alışılmadık başlangıç durumunda: {other}. \
                             Doğrudan order endpoint'iyle izlenecek."
                        );
                    }
                }

                crate::log_info!("console", "Açık emir ID: {}", response.order_id);
                Some(response.order_id)
            } else {
                Some(
                    recovered_order_id
                        .context("Belirsiz POST sonrasında kurtarılan order ID kayboldu")?,
                )
            }
        } else {
            crate::log_info!("console", "DRY RUN: Emir gönderilmedi; sanal emir izleniyor.");
            None
        };

        let mut quote = quote;

        let (mut write, mut read) = socket.split();

        let mut ping_interval = interval(Duration::from_secs(WEBSOCKET_PING_SECONDS));

        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut health_interval =
            interval(Duration::from_millis(HEALTH_CHECK_MILLISECONDS));

        health_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut book_resync_interval = interval_at(
            Instant::now() + Duration::from_secs(BOOK_RESYNC_SECONDS),
            Duration::from_secs(BOOK_RESYNC_SECONDS),
        );
        book_resync_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        /*
        Tokio interval'ın ilk tick'i anında çalıştığı için
        order-status sorgusunu interval_at ile geciktiriyoruz.
        */
        let order_poll_period = Duration::from_millis(bot_config.order_poll_milliseconds);

        let mut order_interval = interval_at(
            Instant::now() + Duration::from_secs(ORDER_POLL_INITIAL_DELAY_SECONDS),
            order_poll_period,
        );

        order_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        /*
        Scoring kontrolü de emrin API sistemlerine
        yansıması için gecikmeli başlar.
        */
        let scoring_period = Duration::from_secs(bot_config.scoring_check_seconds);

        let mut scoring_interval = interval_at(
            Instant::now()
                + Duration::from_secs(bot_config.scoring_initial_delay_seconds),
            scoring_period,
        );

        scoring_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let order_created_at = Instant::now();

        let mut consecutive_order_read_errors = 0_u32;

        let mut consecutive_not_scoring = 0_u32;
        let mut consecutive_scoring_errors = 0_u32;
        let mut scoring_check_count = 0_u64;
        let mut scoring_true_count = 0_u64;
        let mut scoring_false_count = 0_u64;
        let mut scoring_error_count = 0_u64;
        let mut first_scoring_confirmed_ms: Option<u128> = None;
        let mut last_scoring_state: Option<bool> = None;

        logger::log_event(
            LogLevel::Info,
            "scoring",
            "scoring_monitor_started",
            "Reward scoring izleme döngüsü başlatıldı",
            json!({
                "order_id": order_id.as_deref(),
                "token_id": quote.market.suggested_token_id,
                "market_slug": quote.market.market_slug,
                "price": quote.price,
                "size": quote.size,
                "initial_delay_seconds": bot_config.scoring_initial_delay_seconds,
                "grace_seconds": bot_config.scoring_grace_seconds,
                "check_interval_seconds": bot_config.scoring_check_seconds,
                "max_consecutive_not_scoring": bot_config.max_consecutive_not_scoring,
                "error_warn_threshold": bot_config.scoring_error_warn_threshold,
            }),
            file!(),
            line!(),
        );

        // Aynı order status her saniye konsola basılmasın.
        let mut last_reported_order_status = String::new();

        let mut last_pong = Instant::now();

        let mut last_market_update = Instant::now();

        let mut quote_protection_guard = QuoteProtectionGuard::new(&quote);

        let outcome = loop {
            tokio::select! {
                result = shutdown.as_mut() => {
                    if let Err(error) = result {
                        crate::log_warn!("warning", 
                            "Ctrl+C sinyali okunamadı: {error}"
                        );
                    }

                    break MonitorOutcome::Shutdown;
                }

                _ = ping_interval.tick() => {
                    if let Err(error) = write
                        .send(
                            Message::Text(
                                "PING".into(),
                            ),
                        )
                        .await
                    {
                        crate::log_warn!("warning", 
                            "WebSocket PING gönderilemedi: {error}"
                        );

                        break MonitorOutcome::Requote;
                    }
                }

                _ = health_interval.tick() => {
                    if last_pong.elapsed()
                        > Duration::from_secs(
                            WEBSOCKET_PONG_TIMEOUT_SECONDS,
                        )
                    {
                        crate::log_warn!("warning", 
                            "WebSocket PONG zaman aşımı; \
                             emir güvenlik için iptal edilecek."
                        );

                        break MonitorOutcome::Requote;
                    }

                    if last_market_update.elapsed()
                        > Duration::from_secs(
                            MARKET_DATA_STALE_SECONDS,
                        )
                    {
                        crate::log_warn!("warning", 
                            "Market verisi {} saniyedir \
                             güncellenmedi; emir güvenlik \
                             için iptal edilecek.",
                            MARKET_DATA_STALE_SECONDS
                        );

                        break MonitorOutcome::Requote;
                    }

                    if let Some(reason) = quote_protection_guard.evaluate(
                        &quote.book,
                        quote.price,
                        &bot_config,
                        false,
                    ) {
                        crate::log_info!("console", "İptal tetiklendi: {reason}");
                        break MonitorOutcome::Requote;
                    }

                    if let Some(reason) =
                        quote_cancel_reason(
                            &quote.book,
                            quote.price,
                            quote
                                .market
                                .reward_max_spread,
                            &bot_config,
                        )
                    {
                        crate::log_info!("console", 
                            "İptal tetiklendi: {reason}"
                        );

                        break MonitorOutcome::Requote;
                    }
                }

                _ = book_resync_interval.tick() => {
                    match fetch_rest_book(
                        &book_http,
                        &bot_config.clob_host,
                        &quote.market.suggested_token_id,
                    )
                    .await
                    {
                        Ok(rest_book) => {
                            log_book_resync_difference(&quote.book, &rest_book);
                            quote.book = rest_book;
                            last_market_update = Instant::now();

                            if let Some(reason) = quote_protection_guard.evaluate(
                                &quote.book,
                                quote.price,
                                &bot_config,
                                true,
                            ) {
                                crate::log_info!("console", "İptal tetiklendi: {reason}");
                                break MonitorOutcome::Requote;
                            }

                            if let Some(reason) = quote_cancel_reason(
                                &quote.book,
                                quote.price,
                                quote.market.reward_max_spread,
                                &bot_config,
                            ) {
                                crate::log_info!("console", "İptal tetiklendi: {reason}");
                                break MonitorOutcome::Requote;
                            }
                        }
                        Err(error) => {
                            crate::log_warn!("warning", 
                                "Periyodik REST book resync başarısız: {error:#}"
                            );
                            break MonitorOutcome::Requote;
                        }
                    }
                }

                message = read.next() => {
                    let Some(message) =
                        message
                    else {
                        crate::log_warn!("warning", 
                            "Market WebSocket kapandı."
                        );

                        break MonitorOutcome::Requote;
                    };

                    match message {
                        Ok(Message::Text(text)) => {
                            let text =
                                text.to_string();

                            if text
                                .trim()
                                .eq_ignore_ascii_case(
                                    "PONG",
                                )
                            {
                                last_pong =
                                    Instant::now();

                                continue;
                            }

                            match serde_json::
                                from_str::<Value>(
                                    &text,
                                )
                            {
                                Ok(value) => {
                                    match apply_market_value(
                                        &mut quote.book,
                                        &value,
                                        &quote.market.suggested_token_id,
                                    ) {
                                        ApplyMarketResult::Applied => {
                                            last_market_update = Instant::now();
                                        }
                                        ApplyMarketResult::Ignored => {
                                            continue;
                                        }
                                        ApplyMarketResult::RequiresResync(reason) => {
                                            crate::log_warn!("warning", 
                                                "WebSocket book resync istedi: {reason}"
                                            );

                                            match fetch_rest_book(
                                                &book_http,
                                                &bot_config.clob_host,
                                                &quote.market.suggested_token_id,
                                            )
                                            .await
                                            {
                                                Ok(rest_book) => {
                                                    quote.book = rest_book;
                                                    last_market_update = Instant::now();
                                                }
                                                Err(error) => {
                                                    crate::log_warn!("warning", 
                                                        "Zorunlu REST book resync başarısız: {error:#}"
                                                    );
                                                    break MonitorOutcome::Requote;
                                                }
                                            }
                                        }
                                    }

                                    if let Some(reason) = quote_protection_guard.evaluate(
                                        &quote.book,
                                        quote.price,
                                        &bot_config,
                                        true,
                                    ) {
                                        crate::log_info!("console", "İptal tetiklendi: {reason}");
                                        break MonitorOutcome::Requote;
                                    }

                                    if let Some(reason) = quote_cancel_reason(
                                        &quote.book,
                                        quote.price,
                                        quote.market.reward_max_spread,
                                        &bot_config,
                                    ) {
                                        crate::log_info!("console", "İptal tetiklendi: {reason}");
                                        break MonitorOutcome::Requote;
                                    }
                                }

                                Err(error) => {
                                    crate::log_warn!("warning", 
                                        "WebSocket JSON okunamadı: \
                                         {error}"
                                    );
                                }
                            }
                        }

                        Ok(Message::Ping(payload)) => {
                            if let Err(error) =
                                write
                                    .send(
                                        Message::Pong(
                                            payload,
                                        ),
                                    )
                                    .await
                            {
                                crate::log_warn!("warning", 
                                    "WebSocket PONG frame \
                                     gönderilemedi: {error}"
                                );

                                break MonitorOutcome::Requote;
                            }

                            last_pong =
                                Instant::now();
                        }

                        Ok(Message::Pong(_)) => {
                            last_pong =
                                Instant::now();
                        }

                        Ok(Message::Close(frame)) => {
                            crate::log_warn!("warning", 
                                "Market WebSocket kapandı: \
                                 {frame:?}"
                            );

                            break MonitorOutcome::Requote;
                        }

                        Ok(_) => {}

                        Err(error) => {
                            crate::log_warn!("warning", 
                                "Market WebSocket hatası: {error}"
                            );

                            break MonitorOutcome::Requote;
                        }
                    }
                }

                /*
                Emir durumu doğrudan tekil order ID endpoint'inden okunur.
                Böylece terminal durum ve kısmi dolum açık-emir sayfasından
                kaybolduğu anda da görülebilir.
                */
                _ = order_interval.tick(),
                if order_id.is_some() => {
                    let id = order_id
                        .as_deref()
                        .expect("guarded by is_some");

                    match client.order(id).await {
                        Ok(order) => {
                            consecutive_order_read_errors = 0;

                            let matched = match parse_nonnegative_number(
                                &order.size_matched,
                                "BUY size_matched",
                            ) {
                                Ok(value) => value,
                                Err(error) => {
                                    crate::log_warn!("warning", 
                                        "BUY dolum miktarı okunamadı; terminal uzlaşmaya geçiliyor: {error:#}"
                                    );
                                    break MonitorOutcome::OrderMissing;
                                }
                            };
                            let current_status = order.status.to_string();

                            if current_status != last_reported_order_status {
                                crate::log_info!("console", 
                                    "Emir durumu: status={}, size_matched={matched}",
                                    order.status
                                );
                                last_reported_order_status = current_status;
                            }

                            if matched > EPSILON
                                || matches!(&order.status, OrderStatusType::Matched)
                            {
                                crate::log_warn!("warning", 
                                    "Dolum tespit edildi: status={}, size_matched={matched}",
                                    order.status
                                );
                                break MonitorOutcome::FillDetected;
                            }

                            match &order.status {
                                OrderStatusType::Canceled => {
                                    break MonitorOutcome::OrderClosed;
                                }
                                OrderStatusType::Delayed => {
                                    crate::log_warn!("warning", 
                                        "Emir delayed durumunda; güvenlik için iptal/rescan yapılacak."
                                    );
                                    break MonitorOutcome::Requote;
                                }
                                OrderStatusType::Live
                                | OrderStatusType::Unmatched
                                | OrderStatusType::Matched => {}
                                _ => {
                                    crate::log_warn!("warning", 
                                        "Bilinmeyen emir durumu: {}",
                                        order.status
                                    );
                                }
                            }
                        }

                        Err(error) => {
                            if order_created_at.elapsed()
                                < Duration::from_secs(ORDER_VISIBILITY_GRACE_SECONDS)
                            {
                                crate::log_warn!("warning", 
                                    "Tekil emir sorgusu başlangıç gecikmesinde başarısız; \
                                     tekrar denenecek: {error:#}"
                                );
                                continue;
                            }

                            consecutive_order_read_errors += 1;
                            crate::log_warn!("warning", 
                                "Tekil emir sorgusu başarısız ({}/{}): {error:#}",
                                consecutive_order_read_errors,
                                MAX_CONSECUTIVE_ORDER_MISSES
                            );

                            if consecutive_order_read_errors
                                >= MAX_CONSECUTIVE_ORDER_MISSES
                            {
                                break MonitorOutcome::OrderMissing;
                            }
                        }
                    }
                }

                /*
                Her scoring sorgusu hem genel JSONL loguna hem de ayrı scoring CSV
                dosyasına yazılır. Grace sonrasında yapılandırılmış sayıda art arda
                false sonucu görülürse emir yeniden fiyatlanmak üzere kapatılır.
                */
                _ = scoring_interval.tick(),
                if order_id.is_some() => {
                    let id = order_id
                        .as_deref()
                        .expect("guarded by is_some");

                    scoring_check_count = scoring_check_count.saturating_add(1);
                    let check_started = Instant::now();
                    let age = order_created_at.elapsed();
                    let in_grace = age
                        < Duration::from_secs(bot_config.scoring_grace_seconds);

                    let scoring_result = client.is_order_scoring(id).await;
                    let request_latency = check_started.elapsed();

                    match scoring_result {
                        Ok(response) if response.scoring => {
                            scoring_true_count = scoring_true_count.saturating_add(1);
                            consecutive_not_scoring = 0;
                            consecutive_scoring_errors = 0;

                            let first_confirmation = first_scoring_confirmed_ms.is_none();
                            if first_confirmation {
                                first_scoring_confirmed_ms = Some(age.as_millis());
                            }

                            let event = if first_confirmation {
                                "scoring_first_confirmed"
                            } else if last_scoring_state == Some(false) {
                                "scoring_restored"
                            } else {
                                "scoring_check"
                            };

                            logger::record_scoring(&ScoringLogRecord {
                                order_id: id,
                                token_id: &quote.market.suggested_token_id,
                                market_slug: &quote.market.market_slug,
                                check_number: scoring_check_count,
                                order_age_ms: age.as_millis(),
                                request_latency_ms: request_latency.as_millis(),
                                state: "SCORING",
                                in_grace,
                                consecutive_false: consecutive_not_scoring,
                                consecutive_errors: consecutive_scoring_errors,
                            });

                            logger::log_event(
                                LogLevel::Info,
                                "scoring",
                                event,
                                if first_confirmation {
                                    "Emir reward servisi tarafından ilk kez scoring olarak doğrulandı"
                                } else {
                                    "Reward scoring kontrolü başarılı"
                                },
                                json!({
                                    "order_id": id,
                                    "token_id": quote.market.suggested_token_id,
                                    "market_slug": quote.market.market_slug,
                                    "check_number": scoring_check_count,
                                    "order_age_ms": age.as_millis(),
                                    "request_latency_ms": request_latency.as_millis(),
                                    "scoring": true,
                                    "in_grace": in_grace,
                                    "first_confirmation": first_confirmation,
                                    "first_scoring_confirmed_ms": first_scoring_confirmed_ms,
                                    "true_count": scoring_true_count,
                                    "false_count": scoring_false_count,
                                    "error_count": scoring_error_count,
                                }),
                                file!(),
                                line!(),
                            );

                            last_scoring_state = Some(true);
                        }

                        Ok(_) => {
                            scoring_false_count = scoring_false_count.saturating_add(1);
                            consecutive_scoring_errors = 0;

                            if in_grace {
                                consecutive_not_scoring = 0;
                            } else {
                                consecutive_not_scoring =
                                    consecutive_not_scoring.saturating_add(1);
                            }

                            logger::record_scoring(&ScoringLogRecord {
                                order_id: id,
                                token_id: &quote.market.suggested_token_id,
                                market_slug: &quote.market.market_slug,
                                check_number: scoring_check_count,
                                order_age_ms: age.as_millis(),
                                request_latency_ms: request_latency.as_millis(),
                                state: "NOT_SCORING",
                                in_grace,
                                consecutive_false: consecutive_not_scoring,
                                consecutive_errors: consecutive_scoring_errors,
                            });

                            logger::log_event(
                                if in_grace {
                                    LogLevel::Info
                                } else {
                                    LogLevel::Warn
                                },
                                "scoring",
                                if in_grace {
                                    "scoring_pending_in_grace"
                                } else {
                                    "scoring_false_after_grace"
                                },
                                if in_grace {
                                    "Reward servisi henüz scoring göstermiyor; grace süresi devam ediyor"
                                } else {
                                    "Reward servisi grace süresinden sonra scoring=false döndürdü"
                                },
                                json!({
                                    "order_id": id,
                                    "token_id": quote.market.suggested_token_id,
                                    "market_slug": quote.market.market_slug,
                                    "check_number": scoring_check_count,
                                    "order_age_ms": age.as_millis(),
                                    "request_latency_ms": request_latency.as_millis(),
                                    "scoring": false,
                                    "in_grace": in_grace,
                                    "grace_seconds": bot_config.scoring_grace_seconds,
                                    "consecutive_false": consecutive_not_scoring,
                                    "max_consecutive_false": bot_config.max_consecutive_not_scoring,
                                    "true_count": scoring_true_count,
                                    "false_count": scoring_false_count,
                                    "error_count": scoring_error_count,
                                }),
                                file!(),
                                line!(),
                            );

                            last_scoring_state = Some(false);

                            if !in_grace
                                && consecutive_not_scoring
                                    >= bot_config.max_consecutive_not_scoring
                            {
                                logger::log_event(
                                    LogLevel::Error,
                                    "scoring",
                                    "scoring_requote_triggered",
                                    "Emir art arda scoring=false sonuçları nedeniyle iptal edilecek",
                                    json!({
                                        "order_id": id,
                                        "token_id": quote.market.suggested_token_id,
                                        "market_slug": quote.market.market_slug,
                                        "order_age_ms": age.as_millis(),
                                        "consecutive_false": consecutive_not_scoring,
                                        "threshold": bot_config.max_consecutive_not_scoring,
                                        "checks": scoring_check_count,
                                        "true_count": scoring_true_count,
                                        "false_count": scoring_false_count,
                                        "error_count": scoring_error_count,
                                    }),
                                    file!(),
                                    line!(),
                                );

                                break MonitorOutcome::Requote;
                            }
                        }

                        Err(error) => {
                            scoring_error_count = scoring_error_count.saturating_add(1);
                            consecutive_scoring_errors =
                                consecutive_scoring_errors.saturating_add(1);

                            logger::record_scoring(&ScoringLogRecord {
                                order_id: id,
                                token_id: &quote.market.suggested_token_id,
                                market_slug: &quote.market.market_slug,
                                check_number: scoring_check_count,
                                order_age_ms: age.as_millis(),
                                request_latency_ms: request_latency.as_millis(),
                                state: "ERROR",
                                in_grace,
                                consecutive_false: consecutive_not_scoring,
                                consecutive_errors: consecutive_scoring_errors,
                            });

                            logger::log_event(
                                LogLevel::Warn,
                                "scoring",
                                "scoring_request_failed",
                                format!(
                                    "Scoring kontrolü başarısız; emir yalnızca bu nedenle iptal edilmeyecek: {error:#}"
                                ),
                                json!({
                                    "order_id": id,
                                    "token_id": quote.market.suggested_token_id,
                                    "market_slug": quote.market.market_slug,
                                    "check_number": scoring_check_count,
                                    "order_age_ms": age.as_millis(),
                                    "request_latency_ms": request_latency.as_millis(),
                                    "in_grace": in_grace,
                                    "consecutive_errors": consecutive_scoring_errors,
                                    "error_warn_threshold": bot_config.scoring_error_warn_threshold,
                                    "true_count": scoring_true_count,
                                    "false_count": scoring_false_count,
                                    "error_count": scoring_error_count,
                                    "error": format!("{error:#}"),
                                }),
                                file!(),
                                line!(),
                            );

                            if consecutive_scoring_errors
                                == bot_config.scoring_error_warn_threshold
                            {
                                logger::log_event(
                                    LogLevel::Error,
                                    "scoring",
                                    "scoring_endpoint_degraded",
                                    "Scoring endpoint art arda hata veriyor; scoring durumu doğrulanamıyor",
                                    json!({
                                        "order_id": id,
                                        "consecutive_errors": consecutive_scoring_errors,
                                        "threshold": bot_config.scoring_error_warn_threshold,
                                        "last_error": format!("{error:#}"),
                                    }),
                                    file!(),
                                    line!(),
                                );
                            }
                        }
                    }
                }
            }
        };

        logger::log_event(
            LogLevel::Info,
            "scoring",
            "scoring_monitor_summary",
            "Reward scoring izleme döngüsü sona erdi",
            json!({
                "order_id": order_id.as_deref(),
                "token_id": quote.market.suggested_token_id,
                "market_slug": quote.market.market_slug,
                "monitor_outcome": outcome.as_str(),
                "duration_ms": order_created_at.elapsed().as_millis(),
                "checks": scoring_check_count,
                "scoring_true_count": scoring_true_count,
                "scoring_false_count": scoring_false_count,
                "scoring_error_count": scoring_error_count,
                "first_scoring_confirmed_ms": first_scoring_confirmed_ms,
                "final_consecutive_false": consecutive_not_scoring,
                "final_consecutive_errors": consecutive_scoring_errors,
            }),
            file!(),
            line!(),
        );

        /*
        BUY yaşam döngüsünün tek güvenli çıkış kapısı burasıdır:

        1. Order doğrudan ID ile okunur.
        2. LIVE / UNMATCHED / DELAYED ise hedefli cancel tekrarlanır.
        3. Kısmi dolum görülmesi, kalan BUY'ın iptal edildiği anlamına gelmez.
        4. Yalnızca MATCHED veya CANCELED terminal durumu kabul edilir.
        5. Terminal duruma kadar oluşan ilave dolumlar final size_matched'e eklenir.
        6. Final dolum miktarı sıfırdan büyükse aynı token otomatik SELL edilir.
        */
        let mut buy_terminal_confirmed = order_id.is_none();
        let mut final_buy_original_size = quote.size;
        let mut final_buy_filled_size = 0.0_f64;
        let mut final_buy_status = "DRY_RUN".to_string();

        if let Some(id) = order_id.as_deref() {
            let mut cancel_all_attempted = false;

            for attempt in 1..=CANCEL_CONFIRM_ATTEMPTS {
                match client.order(id).await {
                    Ok(order) => {
                        let original_size = parse_nonnegative_number(
                            &order.original_size,
                            "BUY original_size",
                        )?;
                        let matched_size = parse_nonnegative_number(
                            &order.size_matched,
                            "BUY size_matched",
                        )?;

                        if original_size <= 0.0 {
                            bail!(
                                "BUY order original_size geçersiz: order_id={id}, value={original_size}"
                            );
                        }

                        if matched_size > original_size + ORDER_SIZE_MATCH_TOLERANCE {
                            bail!(
                                "BUY size_matched original_size'dan büyük: \
                                 order_id={id}, original={original_size:.6}, matched={matched_size:.6}"
                            );
                        }

                        // Cancel yarışı sırasında yeni fill oluşabilir. Her okumada son
                        // görülen kümülatif miktar saklanır; SELL yalnızca final değerle açılır.
                        final_buy_original_size = original_size;
                        final_buy_filled_size = final_buy_filled_size.max(matched_size);
                        final_buy_status = order.status.to_string();

                        match &order.status {
                            OrderStatusType::Matched | OrderStatusType::Canceled => {
                                buy_terminal_confirmed = true;
                                crate::log_info!("console", 
                                    "BUY terminal durum doğrulandı: order_id={id}, \
                                     status={}, original={:.6}, matched={:.6}, remaining={:.6}",
                                    order.status,
                                    final_buy_original_size,
                                    final_buy_filled_size,
                                    (final_buy_original_size - final_buy_filled_size).max(0.0),
                                );
                                break;
                            }

                            OrderStatusType::Live
                            | OrderStatusType::Unmatched
                            | OrderStatusType::Delayed => {
                                // İlk okumada ve belirli aralıklarla hedefli cancel yenilenir.
                                if attempt == 1 || attempt % TARGETED_CANCEL_RETRY_EVERY == 0 {
                                    match client.cancel_order(id).await {
                                        Ok(response) => {
                                            crate::log_info!("console", 
                                                "BUY hedefli iptal {attempt}/{} cevabı: {response:?}",
                                                CANCEL_CONFIRM_ATTEMPTS
                                            );
                                        }
                                        Err(error) => {
                                            crate::log_warn!("warning", 
                                                "BUY hedefli iptal {attempt}/{} başarısız: {error:#}",
                                                CANCEL_CONFIRM_ATTEMPTS
                                            );
                                        }
                                    }
                                }

                                // Hedefli cancel birkaç kez sonuç vermediyse son çare olarak
                                // hesaptaki bütün açık emirler iptal edilir. Buna rağmen terminal
                                // status görülmeden devam edilmez.
                                if !cancel_all_attempted
                                    && attempt >= CANCEL_CONFIRM_ATTEMPTS / 2
                                {
                                    cancel_all_attempted = true;
                                    crate::log_warn!("warning", 
                                        "BUY hâlâ terminal değil; son çare cancel-all deneniyor."
                                    );
                                    match client.cancel_all_orders().await {
                                        Ok(response) => {
                                            crate::log_info!("console", "Cancel-all cevabı: {response:?}");
                                        }
                                        Err(error) => {
                                            crate::log_warn!("warning", "Cancel-all başarısız: {error:#}");
                                        }
                                    }
                                }

                                crate::log_warn!("warning", 
                                    "BUY terminal doğrulaması {attempt}/{}: \
                                     status={}, matched={:.6}, kalan BUY henüz kesin kapanmadı.",
                                    CANCEL_CONFIRM_ATTEMPTS,
                                    order.status,
                                    final_buy_filled_size,
                                );
                            }

                            other => {
                                crate::log_warn!("warning", 
                                    "BUY alışılmadık durumda; terminal doğrulama devam ediyor: \
                                     order_id={id}, status={other}, matched={:.6}",
                                    final_buy_filled_size,
                                );

                                if attempt == 1 || attempt % TARGETED_CANCEL_RETRY_EVERY == 0 {
                                    if let Err(error) = client.cancel_order(id).await {
                                        crate::log_warn!("warning", 
                                            "Alışılmadık BUY durumunda hedefli iptal başarısız: {error:#}"
                                        );
                                    }
                                }
                            }
                        }
                    }

                    Err(error) => {
                        crate::log_warn!("warning", 
                            "BUY terminal sorgusu {attempt}/{} başarısız: {error:#}",
                            CANCEL_CONFIRM_ATTEMPTS
                        );

                        // Read hatasında da order açık olabilir; hedefli cancel idempotent
                        // kabul edilerek belirli aralıklarla tekrar gönderilir.
                        if attempt == 1 || attempt % TARGETED_CANCEL_RETRY_EVERY == 0 {
                            if let Err(cancel_error) = client.cancel_order(id).await {
                                crate::log_warn!("warning", 
                                    "BUY sorgu hatası sonrası hedefli iptal başarısız: {cancel_error:#}"
                                );
                            }
                        }
                    }
                }

                if !buy_terminal_confirmed {
                    sleep(Duration::from_millis(CANCEL_CONFIRM_DELAY_MILLISECONDS)).await;
                }
            }

            if !buy_terminal_confirmed {
                bail!(
                    "BUY emrin MATCHED veya CANCELED terminal durumuna geçtiği \
                     doğrulanamadı. Yeni emir ya da SELL açılmadı. \
                     Order ID: {id}. Polymarket açık emirlerini elle kontrol et."
                );
            }
        }

        let final_buy_remaining =
            (final_buy_original_size - final_buy_filled_size).max(0.0);

        if final_buy_filled_size > SHARE_DUST_TOLERANCE {
            let fill_kind = if final_buy_remaining <= SHARE_DUST_TOLERANCE {
                "TAM DOLUM"
            } else {
                "KISMİ DOLUM"
            };

            crate::log_info!("console", 
                "{fill_kind}: BUY status={final_buy_status}, \
                 original={final_buy_original_size:.6}, \
                 filled={final_buy_filled_size:.6}, \
                 canceled_remaining={final_buy_remaining:.6}"
            );

            if !bot_config.live_trading {
                bail!("DRY RUN sırasında gerçek BUY dolumu beklenmiyordu");
            }

            let token_id = U256::from_str(&quote.market.suggested_token_id)
                .context("Otomatik SELL token ID U256 olarak okunamadı")?;

            let conditional_balance_request = BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Conditional)
                .token_id(token_id.clone())
                .signature_type(signature_type.clone())
                .build();

            // BUY emirleri iki ondalık share hassasiyetiyle açıldığı için final fill de
            // iki ondalıkta satılabilir olmalıdır. Aşağı yuvarlama oversell riskini önler.
            let target_sell_size = floor_to_decimals(
                final_buy_filled_size + EPSILON,
                ORDER_SIZE_DECIMALS,
            );

            let unsellable_fraction =
                (final_buy_filled_size - target_sell_size).max(0.0);

            if target_sell_size <= SHARE_DUST_TOLERANCE
                || unsellable_fraction > SHARE_DUST_TOLERANCE
            {
                bail!(
                    "Dolum {:.8} share fakat güvenli iki ondalık SELL miktarına \
                     dönüştürülemiyor: sellable={target_sell_size:.8}, \
                     residual={unsellable_fraction:.8}. Kalan BUY kapalı; \
                     pozisyon elle kontrol edilmeli.",
                    final_buy_filled_size
                );
            }

            crate::log_info!("console", 
                "Otomatik pozisyon kapatma başlıyor: token={}, hedef SELL={:.2} share, \
                 yöntem=MARKET FAK",
                quote.market.suggested_token_id,
                target_sell_size,
            );

            let mut total_sold = 0.0_f64;

            for sell_attempt in 1..=AUTO_SELL_MAX_ATTEMPTS {
                let remaining_to_sell = (target_sell_size - total_sold).max(0.0);

                if remaining_to_sell <= SHARE_DUST_TOLERANCE {
                    break;
                }

                let sell_size = floor_to_decimals(
                    remaining_to_sell + EPSILON,
                    ORDER_SIZE_DECIMALS,
                );

                if sell_size <= SHARE_DUST_TOLERANCE {
                    break;
                }

                // Match CLOB tarafında görünse bile conditional token bakiyesi kısa bir
                // süre sonra güncellenebilir. SELL göndermeden önce balance görünürlüğü beklenir.
                let mut token_balance_ready = false;
                let mut last_token_balance = 0.0_f64;

                for balance_attempt in 1..=AUTO_SELL_BALANCE_WAIT_ATTEMPTS {
                    match client
                        .update_balance_allowance(conditional_balance_request.clone())
                        .await
                    {
                        Ok(_) => {}
                        Err(error) => {
                            crate::log_warn!("warning", 
                                "Conditional balance cache güncellemesi {balance_attempt}/{} \
                                 başarısız: {error:#}",
                                AUTO_SELL_BALANCE_WAIT_ATTEMPTS
                            );
                        }
                    }

                    match client
                        .balance_allowance(conditional_balance_request.clone())
                        .await
                    {
                        Ok(response) => {
                            let raw_balance = parse_nonnegative_number(
                                &response.balance,
                                "conditional token balance",
                            )?;
                            last_token_balance = raw_balance / TOKEN_AMOUNT_SCALE;

                            if last_token_balance + SHARE_DUST_TOLERANCE >= sell_size {
                                token_balance_ready = true;
                                break;
                            }

                            crate::log_warn!("warning", 
                                "Dolmuş token bakiyesi henüz görünür değil \
                                 ({balance_attempt}/{}): balance={last_token_balance:.6}, \
                                 gerekli={sell_size:.6}",
                                AUTO_SELL_BALANCE_WAIT_ATTEMPTS
                            );
                        }
                        Err(error) => {
                            crate::log_warn!("warning", 
                                "Conditional token bakiyesi {balance_attempt}/{} okunamadı: \
                                 {error:#}",
                                AUTO_SELL_BALANCE_WAIT_ATTEMPTS
                            );
                        }
                    }

                    sleep(Duration::from_millis(
                        AUTO_SELL_RETRY_MILLISECONDS,
                    ))
                    .await;
                }

                if !token_balance_ready {
                    bail!(
                        "Otomatik SELL için token bakiyesi doğrulanamadı: \
                         görünen={last_token_balance:.6}, gerekli={sell_size:.6}. \
                         Kalan BUY kapalı; {:.6} share pozisyon elle kontrol edilmeli.",
                        remaining_to_sell
                    );
                }

                let sell_size_decimal = decimal_from_f64(
                    sell_size,
                    ORDER_SIZE_DECIMALS,
                    "otomatik SELL büyüklüğü",
                )?;
                let sell_amount = Amount::shares(sell_size_decimal)
                    .context("Otomatik SELL share miktarı oluşturulamadı")?;

                // FAK: mevcut BUY likiditesinde mümkün olan miktarı hemen satar,
                // dolmayan kısmı tahtada bırakmadan iptal eder.
                let sell_order_result = client
                    .market_order()
                    .token_id(token_id.clone())
                    .amount(sell_amount)
                    .side(Side::Sell)
                    .order_type(OrderType::FAK)
                    .build()
                    .await;

                // MarketOrderBuilder da Client klonladığı için heartbeat görevini her
                // build denemesinden sonra yeniliyoruz; build başarısız olsa bile.
                if client.heartbeats_active() {
                    client
                        .stop_heartbeats()
                        .await
                        .context("SELL builder sonrası eski heartbeat temizlenemedi")?;
                }
                Client::start_heartbeats(&mut client)
                    .context("SELL builder sonrası heartbeat yeniden başlatılamadı")?;

                let sell_order = match sell_order_result {
                    Ok(order) => order,
                    Err(error) => {
                        crate::log_warn!("warning", 
                            "Otomatik SELL market order oluşturulamadı \
                             ({sell_attempt}/{}): {error:#}",
                            AUTO_SELL_MAX_ATTEMPTS
                        );
                        sleep(Duration::from_millis(
                            AUTO_SELL_RETRY_MILLISECONDS,
                        ))
                        .await;
                        continue;
                    }
                };

                let signed_sell = client
                    .sign(&signer, sell_order)
                    .await
                    .context("Otomatik SELL imzalanamadı")?;

                let sell_recovery_cutoff_ts = Utc::now().timestamp() - 3;
                let mut recovered_sell_order_id: Option<String> = None;

                let sell_response = match client.post_order(signed_sell).await {
                    Ok(response) => Some(response),
                    Err(error) => {
                        crate::log_warn!("warning", 
                            "Otomatik SELL POST sonucu belirsiz: {error:#}. \
                             Aynı token/side/miktardaki yakın tarihli emir aranıyor."
                        );

                        let request = OrdersRequest::builder()
                            .asset_id(token_id.clone())
                            .build();

                        for recovery_attempt in 1..=SELL_POST_RECOVERY_ATTEMPTS {
                            sleep(Duration::from_millis(
                                SELL_POST_RECOVERY_DELAY_MILLISECONDS,
                            ))
                            .await;

                            match client.orders(&request, None).await {
                                Ok(page) => {
                                    let candidates = page
                                        .data
                                        .iter()
                                        .filter_map(|order| {
                                            let original_size = parse_nonnegative_number(
                                                &order.original_size,
                                                "recovered SELL original_size",
                                            )
                                            .ok()?;

                                            (matches!(&order.side, Side::Sell)
                                                && order.created_at.timestamp()
                                                    >= sell_recovery_cutoff_ts
                                                && (original_size - sell_size).abs()
                                                    <= ORDER_SIZE_MATCH_TOLERANCE)
                                                .then(|| order.id.clone())
                                        })
                                        .collect::<Vec<_>>();

                                    match candidates.as_slice() {
                                        [id] => {
                                            recovered_sell_order_id = Some(id.clone());
                                            break;
                                        }
                                        [] => {}
                                        _ => {
                                            bail!(
                                                "Belirsiz SELL POST sonrasında birden fazla \
                                                 eşleşen emir bulundu: {candidates:?}. \
                                                 Duplicate SELL riski nedeniyle bot durduruldu."
                                            );
                                        }
                                    }
                                }
                                Err(read_error) => {
                                    crate::log_warn!("warning", 
                                        "SELL POST uzlaşma sorgusu {recovery_attempt}/{} \
                                         başarısız: {read_error:#}",
                                        SELL_POST_RECOVERY_ATTEMPTS
                                    );
                                }
                            }
                        }

                        if recovered_sell_order_id.is_none() {
                            bail!(
                                "Otomatik SELL POST sonucu belirsiz ve emir kesin olarak \
                                 bulunamadı. Duplicate SELL açılmadı. Kalan BUY kapalı; \
                                 yaklaşık {remaining_to_sell:.6} share elle kontrol edilmeli."
                            );
                        }

                        None
                    }
                };

                let sell_order_id = if let Some(response) = sell_response {
                    crate::log_info!("console", "Otomatik SELL cevabı: {response:?}");

                    if !response.success {
                        crate::log_warn!("warning", 
                            "Otomatik SELL reddedildi ({sell_attempt}/{}): {}",
                            AUTO_SELL_MAX_ATTEMPTS,
                            response
                                .error_msg
                                .as_deref()
                                .unwrap_or("bilinmeyen CLOB hatası")
                        );
                        sleep(Duration::from_millis(
                            AUTO_SELL_RETRY_MILLISECONDS,
                        ))
                        .await;
                        continue;
                    }

                    if response.order_id.trim().is_empty() {
                        bail!("Başarılı SELL cevabında order ID boş");
                    }

                    response.order_id
                } else {
                    let id = recovered_sell_order_id
                        .context("Belirsiz SELL sonrasında kurtarılan order ID kayboldu")?;
                    crate::log_info!("console", "Belirsiz SELL POST sonrası emir bulundu: {id}");
                    id
                };

                let mut sell_terminal_confirmed = false;
                let mut sold_this_attempt = 0.0_f64;

                for confirm_attempt in 1..=AUTO_SELL_ORDER_CONFIRM_ATTEMPTS {
                    match client.order(&sell_order_id).await {
                        Ok(order) => {
                            let original_size = parse_nonnegative_number(
                                &order.original_size,
                                "SELL original_size",
                            )?;
                            let matched_size = parse_nonnegative_number(
                                &order.size_matched,
                                "SELL size_matched",
                            )?;

                            if matched_size > original_size + ORDER_SIZE_MATCH_TOLERANCE
                                || original_size > sell_size + ORDER_SIZE_MATCH_TOLERANCE
                            {
                                bail!(
                                    "SELL order miktarları beklenmedik: order_id={}, \
                                     requested={sell_size:.6}, original={original_size:.6}, \
                                     matched={matched_size:.6}",
                                    sell_order_id
                                );
                            }

                            sold_this_attempt = sold_this_attempt.max(matched_size);

                            match &order.status {
                                OrderStatusType::Matched | OrderStatusType::Canceled => {
                                    sell_terminal_confirmed = true;
                                    crate::log_info!("console", 
                                        "SELL terminal durum doğrulandı: order_id={}, \
                                         status={}, requested={sell_size:.6}, \
                                         sold={sold_this_attempt:.6}",
                                        sell_order_id,
                                        order.status,
                                    );
                                    break;
                                }
                                OrderStatusType::Live
                                | OrderStatusType::Unmatched
                                | OrderStatusType::Delayed => {
                                    // FAK normalde açık kalmaz. Kalırsa oversell riskini önlemek
                                    // için hedefli cancel yapılır ve terminal status beklenir.
                                    crate::log_warn!("warning", 
                                        "FAK SELL hâlâ açık görünüyor: status={}. \
                                         Hedefli iptal gönderiliyor.",
                                        order.status
                                    );
                                    if let Err(error) = client.cancel_order(&sell_order_id).await {
                                        crate::log_warn!("warning", "Açık SELL iptal edilemedi: {error:#}");
                                    }
                                }
                                other => {
                                    crate::log_warn!("warning", 
                                        "SELL alışılmadık durumda: order_id={}, status={other}",
                                        sell_order_id
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            crate::log_warn!("warning", 
                                "SELL terminal sorgusu {confirm_attempt}/{} başarısız: {error:#}",
                                AUTO_SELL_ORDER_CONFIRM_ATTEMPTS
                            );
                        }
                    }

                    sleep(Duration::from_millis(
                        AUTO_SELL_ORDER_CONFIRM_MILLISECONDS,
                    ))
                    .await;
                }

                if !sell_terminal_confirmed {
                    bail!(
                        "SELL order terminal durumu doğrulanamadı: order_id={sell_order_id}. \
                         Duplicate SELL riski nedeniyle yeni SELL açılmadı."
                    );
                }

                if sold_this_attempt > sell_size + ORDER_SIZE_MATCH_TOLERANCE {
                    bail!(
                        "SELL beklenenden fazla dolmuş görünüyor: \
                         requested={sell_size:.6}, sold={sold_this_attempt:.6}"
                    );
                }

                total_sold += sold_this_attempt;

                crate::log_info!("console", 
                    "Otomatik SELL ilerlemesi: bu deneme={sold_this_attempt:.6}, \
                     toplam={total_sold:.6}/{target_sell_size:.6}, \
                     kalan={:.6}",
                    (target_sell_size - total_sold).max(0.0),
                );

                if sold_this_attempt <= SHARE_DUST_TOLERANCE {
                    crate::log_warn!("warning", 
                        "Bu FAK SELL denemesinde likidite alınamadı; yeniden denenecek."
                    );
                }

                sleep(Duration::from_millis(
                    AUTO_SELL_RETRY_MILLISECONDS,
                ))
                .await;
            }

            // Kalan BUY'ın iptal edilmesi yalnızca yeni alımı durdurur; daha önce
            // dolan outcome tokenları pozisyondur. Yeniden tarama ancak bu miktarın
            // tamamının SELL ile kapandığı doğrulandıktan sonra yapılır.
            let unsold = (target_sell_size - total_sold).max(0.0);

            if unsold > SHARE_DUST_TOLERANCE {
                bail!(
                    "Otomatik SELL denemeleri sonunda pozisyon tamamen kapanmadı: \
                     hedef={target_sell_size:.6}, satılan={total_sold:.6}, \
                     satılamayan={unsold:.6} share. Kalan BUY kesin kapalıdır; \
                     bu token pozisyonu elle kapatılmalı."
                );
            }

            crate::log_info!("console", 
                "Pozisyonun tamamen kapandığı doğrulandı: BUY filled={final_buy_filled_size:.6}, \
                 SELL confirmed={total_sold:.6} share. Şimdi yeniden tarama yapılacak."
            );
        } else {
            crate::log_info!("console", 
                "BUY terminal durumda ve dolum yok: status={final_buy_status}. \
                 Yeniden tarama yapılacak."
            );
        }

        if matches!(outcome, MonitorOutcome::Shutdown) {
            crate::log_info!("console", 
                "Kapatma sinyali alındı; BUY terminalleştirildi ve varsa \
                 dolan pozisyon kapatıldı. Bot durduruldu."
            );
            return Ok(());
        }

        sleep(Duration::from_millis(FAST_RESCAN_DELAY_MILLISECONDS)).await;
    }
}

async fn select_live_quote(
    markets: Vec<RankedMarket>,
    available_pusd: f64,
    config: &BotConfig,
) -> Result<Option<(LiveQuote, MarketSocket)>> {
    for market in markets {
        if !market.suggested_side.eq_ignore_ascii_case("BUY") {
            continue;
        }

        crate::log_info!("console", 
            "\nCanlı doğrulama: {:.1}/100 — {}",
            market.score, market.question
        );

        let connection = connect_market_socket(
            &config.market_ws_url,
            &market.suggested_token_id,
            WEBSOCKET_SNAPSHOT_TIMEOUT_SECONDS,
        )
        .await;

        let (socket, book) = match connection {
            Ok(value) => value,

            Err(error) => {
                crate::log_warn!("warning", 
                    "  WebSocket snapshot alınamadı: \
                     {error:#}"
                );

                continue;
            }
        };

        let Some(best_bid) = book.best_bid() else {
            continue;
        };

        let Some(best_ask) = book.best_ask() else {
            continue;
        };

        let Some(price) = calculate_reward_edge_buy_price(
            best_bid,
            best_ask,
            book.tick_size,
            market.reward_max_spread,
            config.quote_inside_ticks,
        ) else {
            crate::log_info!("console", 
                "  Canlı book reward-edge \
                 BUY fiyatı üretmedi."
            );

            continue;
        };

        let better_bid_levels = book.better_bid_level_count(price);

        if better_bid_levels == 0 {
            crate::log_info!("console", "  Reward-edge fiyatının önünde daha iyi BUY fiyat seviyesi yok.");
            continue;
        }

        let protective_depth =
            book.effective_better_bid_depth(price, config.protective_depth_level_cap);

        if protective_depth + EPSILON < config.min_protective_depth {
            crate::log_info!("console", 
                "  Koruyucu derinlik yetersiz: \
                 {:.2} < {:.2}",
                protective_depth, config.min_protective_depth
            );

            continue;
        }

        /*
        Emir miktarı kullanıcıdan ayrıca alınmaz. Reward minimum size ile
        CLOB minimum size arasından büyük olan seçilir; kullanıcı yalnızca
        emir başına maksimum USD riskini belirler.
        */
        let requested_size_raw = market.reward_min_size.max(book.min_order_size);

        /*
        Reward minimumunun altına düşmemek için
        iki ondalığa yukarı yuvarlanır.
        */
        let requested_size = round_up_to_decimals(requested_size_raw, ORDER_SIZE_DECIMALS);

        let estimated_cost = price * requested_size;

        let balance_limit = available_pusd * BALANCE_SAFETY_RATIO;

        let capital_limit = config.max_order_usd.min(balance_limit);

        if estimated_cost > capital_limit + EPSILON {
            crate::log_info!("console", 
                "  Sermaye sınırı aşılıyor: \
                 maliyet ${estimated_cost:.4}, \
                 limit ${capital_limit:.4}"
            );

            continue;
        }

        return Ok(Some((
            LiveQuote {
                market,
                price,
                size: requested_size,
                estimated_cost,
                book,
                initial_better_bid_levels: better_bid_levels,
                initial_effective_protective_depth: protective_depth,
            },
            socket,
        )));
    }

    Ok(None)
}

fn quote_cancel_reason(
    book: &LocalBook,
    order_price: f64,
    reward_band: f64,
    config: &BotConfig,
) -> Option<String> {
    if book.resolved {
        return Some("market resolved oldu".to_string());
    }

    let Some(best_bid) = book.best_bid() else {
        return Some("order book BUY tarafı boşaldı".to_string());
    };

    let Some(best_ask) = book.best_ask() else {
        return Some("order book SELL tarafı boşaldı".to_string());
    };

    let Some(midpoint) = book.midpoint() else {
        return Some("midpoint hesaplanamadı".to_string());
    };

    let Some(spread) = book.spread() else {
        return Some("canlı spread hesaplanamadı".to_string());
    };

    let tick = book.tick_size.max(0.0001);

    let distance = midpoint - order_price;

    /*
    Best ask bizim BUY fiyatımıza geldiyse
    emir eşleşme riski taşır.
    */
    if best_ask <= order_price + EPSILON {
        return Some(format!(
            "emir marketable oldu: \
             order={order_price:.6}, \
             best_ask={best_ask:.6}"
        ));
    }

    /*
    Bizim emrimiz en iyi BUY seviyesine geldiyse
    önümüzdeki koruyucu emirler bitmiştir.
    */
    if best_bid <= order_price + EPSILON {
        return Some(format!(
            "önümüzde daha iyi BUY seviyesi kalmadı: \
             best_bid={best_bid:.6}"
        ));
    }

    let minimum_distance = config.cancel_min_distance_ticks * tick;

    if distance < minimum_distance - EPSILON {
        return Some(format!(
            "midpoint emre fazla yaklaştı: \
             uzaklık={:.3} cent",
            distance * 100.0
        ));
    }

    if distance > reward_band + EPSILON {
        return Some(format!(
            "emir reward bandı dışında kaldı: \
             uzaklık={:.3}c, limit={:.3}c",
            distance * 100.0,
            reward_band * 100.0
        ));
    }

    let desired_price: f64 = match calculate_reward_edge_buy_price(
        best_bid,
        best_ask,
        tick,
        reward_band,
        config.quote_inside_ticks,
    ) {
        Some(price) => price,

        None => {
            return Some(
                "canlı book artık reward-edge \
                     ve korumalı BUY fiyatı üretmiyor"
                    .to_string(),
            );
        }
    };

    let price_change_ticks: f64 = (desired_price - order_price).abs() / tick;

    if price_change_ticks + EPSILON >= config.reprice_tolerance_ticks {
        return Some(format!(
            "reward-edge fiyatı değişti: \
             mevcut={order_price:.6}, \
             yeni={desired_price:.6}, \
             fark={price_change_ticks:.2} tick"
        ));
    }

    if spread > config.max_live_spread + EPSILON {
        return Some(format!(
            "canlı spread büyüdü: \
             {:.2} cent > {:.2} cent",
            spread * 100.0,
            config.max_live_spread * 100.0
        ));
    }

    None
}

async fn connect_market_socket(
    market_ws_url: &str,
    token_id: &str,
    snapshot_timeout_seconds: u64,
) -> Result<(MarketSocket, LocalBook)> {
    let (mut socket, _) = connect_async(market_ws_url)
        .await
        .context("Market WebSocket bağlantısı kurulamadı")?;

    let subscription = json!({
        "assets_ids": [token_id],
        "type": "market",
        "custom_feature_enabled": true,
    });

    socket
        .send(Message::Text(subscription.to_string().into()))
        .await
        .context("Market WebSocket aboneliği gönderilemedi")?;

    let mut book = LocalBook::default();

    timeout(Duration::from_secs(snapshot_timeout_seconds), async {
        loop {
            let message = socket
                .next()
                .await
                .context("Snapshot beklerken WebSocket kapandı")?
                .context("Snapshot WebSocket mesajı hatalı")?;

            match message {
                Message::Text(text) => {
                    let text = text.to_string();
                    if text.trim().eq_ignore_ascii_case("PONG") {
                        continue;
                    }

                    let value = serde_json::from_str::<Value>(&text)
                        .context("İlk WebSocket JSON mesajı okunamadı")?;

                    match apply_market_value(&mut book, &value, token_id) {
                        ApplyMarketResult::RequiresResync(reason) => {
                            bail!("İlk WebSocket snapshot/update geçersiz: {reason}");
                        }
                        ApplyMarketResult::Applied | ApplyMarketResult::Ignored => {}
                    }

                    if book.snapshot_received
                        && book.best_bid().is_some()
                        && book.best_ask().is_some()
                    {
                        return Ok::<(), anyhow::Error>(());
                    }
                }
                Message::Ping(payload) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .context("Snapshot sırasında PONG gönderilemedi")?;
                }
                Message::Close(frame) => {
                    bail!("Snapshot alınmadan WebSocket kapandı: {frame:?}");
                }
                _ => {}
            }
        }
    })
    .await
    .context("İlk order book snapshot zaman aşımına uğradı")??;

    Ok((socket, book))
}

fn apply_market_value(book: &mut LocalBook, value: &Value, token_id: &str) -> ApplyMarketResult {
    if let Some(items) = value.as_array() {
        let mut applied = false;

        for item in items {
            match apply_market_value(book, item, token_id) {
                ApplyMarketResult::Applied => applied = true,
                ApplyMarketResult::Ignored => {}
                ApplyMarketResult::RequiresResync(reason) => {
                    return ApplyMarketResult::RequiresResync(reason);
                }
            }
        }

        return if applied {
            ApplyMarketResult::Applied
        } else {
            ApplyMarketResult::Ignored
        };
    }

    let Some(event_type) = value.get("event_type").and_then(Value::as_str) else {
        return ApplyMarketResult::Ignored;
    };

    match event_type {
        "book" => {
            if !asset_matches(value, token_id) {
                return ApplyMarketResult::Ignored;
            }

            if book.snapshot_received && event_is_stale_or_duplicate(book, value, None) {
                return ApplyMarketResult::Ignored;
            }

            let Some(bids) = value.get("bids").and_then(Value::as_array) else {
                return ApplyMarketResult::RequiresResync(
                    "book mesajında bids alanı yok".to_string(),
                );
            };
            let Some(asks) = value.get("asks").and_then(Value::as_array) else {
                return ApplyMarketResult::RequiresResync(
                    "book mesajında asks alanı yok".to_string(),
                );
            };

            book.replace_side("BUY", bids);
            book.replace_side("SELL", asks);

            if let Some(tick) = value_f64(value.get("tick_size")) {
                if tick.is_finite() && tick > 0.0 && tick < 1.0 {
                    book.tick_size = tick;
                }
            }

            if let Some(minimum) = value_f64(value.get("min_order_size")) {
                if minimum.is_finite() && minimum >= 0.0 {
                    book.min_order_size = minimum;
                }
            }

            if book
                .best_bid()
                .zip(book.best_ask())
                .map(|(bid, ask)| bid + EPSILON >= ask)
                .unwrap_or(true)
            {
                return ApplyMarketResult::RequiresResync(
                    "book mesajı boş veya crossed order book üretti".to_string(),
                );
            }

            book.snapshot_received = true;
            update_event_metadata(book, value, None);
            ApplyMarketResult::Applied
        }

        "price_change" => {
            let Some(changes) = value.get("price_changes").and_then(Value::as_array) else {
                return ApplyMarketResult::RequiresResync(
                    "price_change mesajında price_changes alanı yok".to_string(),
                );
            };

            let mut matching_hash: Option<&str> = None;
            let mut matching_changes = Vec::new();

            for change in changes {
                if asset_matches(change, token_id) {
                    matching_hash = change.get("hash").and_then(Value::as_str).or(matching_hash);
                    matching_changes.push(change);
                }
            }

            if matching_changes.is_empty() {
                return ApplyMarketResult::Ignored;
            }

            if !book.snapshot_received {
                return ApplyMarketResult::Ignored;
            }

            if event_is_stale_or_duplicate(book, value, matching_hash) {
                return ApplyMarketResult::Ignored;
            }

            for change in matching_changes {
                let Some(side) = change.get("side").and_then(Value::as_str) else {
                    return ApplyMarketResult::RequiresResync(
                        "eşleşen price_change içinde side yok".to_string(),
                    );
                };
                let Some(price) = value_f64(change.get("price")) else {
                    return ApplyMarketResult::RequiresResync(
                        "eşleşen price_change içinde price okunamadı".to_string(),
                    );
                };
                let Some(size) = value_f64(change.get("size")) else {
                    return ApplyMarketResult::RequiresResync(
                        "eşleşen price_change içinde size okunamadı".to_string(),
                    );
                };

                if !price.is_finite()
                    || !size.is_finite()
                    || !(0.0..=1.0).contains(&price)
                    || size < 0.0
                    || !(side.eq_ignore_ascii_case("BUY") || side.eq_ignore_ascii_case("SELL"))
                {
                    return ApplyMarketResult::RequiresResync(
                        "price_change geçersiz fiyat, miktar veya taraf içeriyor".to_string(),
                    );
                }

                book.update_level(side, price, size);
            }

            if book
                .best_bid()
                .zip(book.best_ask())
                .map(|(bid, ask)| bid + EPSILON >= ask)
                .unwrap_or(true)
            {
                return ApplyMarketResult::RequiresResync(
                    "price_change local book'u boş veya crossed duruma getirdi".to_string(),
                );
            }

            update_event_metadata(book, value, matching_hash);
            ApplyMarketResult::Applied
        }

        "tick_size_change" => {
            if !asset_matches(value, token_id) {
                return ApplyMarketResult::Ignored;
            }

            if event_is_stale_or_duplicate(book, value, None) {
                return ApplyMarketResult::Ignored;
            }

            let Some(tick) = value_f64(value.get("new_tick_size")) else {
                return ApplyMarketResult::RequiresResync(
                    "tick_size_change içinde new_tick_size okunamadı".to_string(),
                );
            };

            if !tick.is_finite() || tick <= 0.0 || tick >= 1.0 {
                return ApplyMarketResult::RequiresResync(
                    "tick_size_change geçersiz tick içeriyor".to_string(),
                );
            }

            book.tick_size = tick;
            update_event_metadata(book, value, None);
            ApplyMarketResult::Applied
        }

        "market_resolved" => {
            if !market_event_contains_asset(value, token_id) {
                return ApplyMarketResult::Ignored;
            }

            book.resolved = true;
            update_event_metadata(book, value, None);
            ApplyMarketResult::Applied
        }

        _ => ApplyMarketResult::Ignored,
    }
}

async fn fetch_rest_book(http: &HttpClient, clob_host: &str, token_id: &str) -> Result<LocalBook> {
    for attempt in 1..=BOOK_REST_RETRY_ATTEMPTS {
        match fetch_rest_book_once(http, clob_host, token_id).await {
            Ok(book) => return Ok(book),
            Err(error) if attempt < BOOK_REST_RETRY_ATTEMPTS => {
                let delay_ms = BOOK_REST_RETRY_BASE_MILLISECONDS
                    .saturating_mul(1_u64 << (attempt - 1))
                    .min(1_000);

                crate::log_warn!("warning", 
                    "REST book denemesi {attempt}/{} başarısız: {error:#};                      {delay_ms} ms sonra tekrar denenecek.",
                    BOOK_REST_RETRY_ATTEMPTS
                );
                sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("BOOK_REST_RETRY_ATTEMPTS en az 1 olmalı")
}

async fn fetch_rest_book_once(
    http: &HttpClient,
    clob_host: &str,
    token_id: &str,
) -> Result<LocalBook> {
    let response = http
        .get(endpoint_url(clob_host, "/book"))
        .query(&[("token_id", token_id)])
        .send()
        .await
        .context("REST order book isteği gönderilemedi")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "REST order book endpoint'i HTTP {} döndürdü: {}",
            status,
            body.chars().take(300).collect::<String>()
        );
    }

    let value = response
        .json::<Value>()
        .await
        .context("REST order book cevabı çözümlenemedi")?;

    if !asset_matches(&value, token_id) {
        bail!("REST order book farklı veya eksik asset_id döndürdü");
    }

    let bids = value
        .get("bids")
        .and_then(Value::as_array)
        .context("REST order book bids alanı yok")?;
    let asks = value
        .get("asks")
        .and_then(Value::as_array)
        .context("REST order book asks alanı yok")?;

    let mut book = LocalBook::default();
    book.replace_side("BUY", bids);
    book.replace_side("SELL", asks);
    book.tick_size = value_f64(value.get("tick_size"))
        .filter(|tick| tick.is_finite() && *tick > 0.0 && *tick < 1.0)
        .unwrap_or(0.01);
    book.min_order_size = value_f64(value.get("min_order_size"))
        .filter(|size| size.is_finite() && *size >= 0.0)
        .unwrap_or_default();
    book.snapshot_received = true;
    update_event_metadata(&mut book, &value, None);

    let best_bid = book.best_bid().context("REST order book BUY tarafı boş")?;
    let best_ask = book.best_ask().context("REST order book SELL tarafı boş")?;
    if best_bid + EPSILON >= best_ask {
        bail!("REST order book crossed/geçersiz: bid={best_bid}, ask={best_ask}");
    }

    Ok(book)
}

fn log_book_resync_difference(current: &LocalBook, rest: &LocalBook) {
    let bid_changed = match (current.best_bid(), rest.best_bid()) {
        (Some(left), Some(right)) => (left - right).abs() > EPSILON,
        (None, None) => false,
        _ => true,
    };
    let ask_changed = match (current.best_ask(), rest.best_ask()) {
        (Some(left), Some(right)) => (left - right).abs() > EPSILON,
        (None, None) => false,
        _ => true,
    };
    let tick_changed = (current.tick_size - rest.tick_size).abs() > EPSILON;

    if bid_changed || ask_changed || tick_changed {
        crate::log_warn!("warning", 
            "REST resync local book farkı düzeltti: \
             bid {:?}->{:?}, ask {:?}->{:?}, tick {:.6}->{:.6}",
            current.best_bid(),
            rest.best_bid(),
            current.best_ask(),
            rest.best_ask(),
            current.tick_size,
            rest.tick_size,
        );
    }
}

fn event_is_stale_or_duplicate(
    book: &LocalBook,
    value: &Value,
    hash_override: Option<&str>,
) -> bool {
    let timestamp = value_i64(value.get("timestamp"));
    let hash = hash_override.or_else(|| value.get("hash").and_then(Value::as_str));

    if let (Some(current), Some(incoming)) = (book.last_event_timestamp_ms, timestamp) {
        if incoming < current {
            return true;
        }

        if incoming == current && hash.is_some() && hash == book.last_hash.as_deref() {
            return true;
        }
    }

    false
}

fn update_event_metadata(book: &mut LocalBook, value: &Value, hash_override: Option<&str>) {
    if let Some(timestamp) = value_i64(value.get("timestamp")) {
        if book
            .last_event_timestamp_ms
            .map(|current| timestamp >= current)
            .unwrap_or(true)
        {
            book.last_event_timestamp_ms = Some(timestamp);
        }
    }

    if let Some(hash) = hash_override.or_else(|| value.get("hash").and_then(Value::as_str)) {
        book.last_hash = Some(hash.to_string());
    }
}

fn asset_matches(value: &Value, token_id: &str) -> bool {
    value
        .get("asset_id")
        .or_else(|| value.get("assetId"))
        .and_then(Value::as_str)
        .map(|asset| asset == token_id)
        .unwrap_or(false)
}

fn market_event_contains_asset(value: &Value, token_id: &str) -> bool {
    if asset_matches(value, token_id) {
        return true;
    }

    value
        .get("assets_ids")
        .or_else(|| value.get("asset_ids"))
        .and_then(Value::as_array)
        .map(|assets| {
            assets.iter().any(|asset| {
                asset
                    .as_str()
                    .map(|asset| asset == token_id)
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn endpoint_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn print_selected_quote(quote: &LiveQuote, config: &BotConfig) {
    let best_bid = quote.book.best_bid().unwrap_or_default();

    let best_ask = quote.book.best_ask().unwrap_or_default();

    let midpoint = quote.book.midpoint().unwrap_or_default();

    let protective_raw = quote.book.better_bid_depth(quote.price);
    let protective_effective = quote
        .book
        .effective_better_bid_depth(quote.price, config.protective_depth_level_cap);

    crate::log_info!("console", "\n==========================================");

    crate::log_info!("console", " SEÇİLEN MARKET");

    crate::log_info!("console", "==========================================");

    crate::log_info!("console", "Skor: {:.2}/100", quote.market.score);

    crate::log_info!("console", "Soru: {}", quote.market.question);

    crate::log_info!("console", 
        "Outcome: {} | BUY @ {:.6} | {:.2} share",
        quote.market.suggested_outcome, quote.price, quote.size
    );

    crate::log_info!("console", 
        "Tahmini sermaye: ${:.6} | \
         Reward min size: {:.4}",
        quote.estimated_cost, quote.market.reward_min_size
    );

    crate::log_info!("console", 
        "Best bid: {:.6} | \
         Best ask: {:.6} | \
         Midpoint: {:.6}",
        best_bid, best_ask, midpoint
    );

    crate::log_info!("console", 
        "Reward Max Spread: {:.3} cent | \
         Tick: {:.6}",
        quote.market.reward_max_spread * 100.0,
        quote.book.tick_size
    );

    crate::log_info!("console", 
        "Kilitlenen BUY fiyat-seviyesi: önümüzde {} seviye | quote sırası {}",
        quote.initial_better_bid_levels,
        quote.initial_better_bid_levels + 1,
    );

    crate::log_info!("console", 
        "Daha iyi BUY emir derinliği: ham {:.2} | efektif {:.2} share",
        protective_raw, protective_effective
    );

    crate::log_info!("console", 
        "Miktar koruması: mutlak taban {:.2} | başlangıcın en az %{:.0}'ı",
        config.min_protective_depth,
        config.protective_depth_min_retention_ratio * 100.0,
    );

    crate::log_info!("console", "Token ID: {}", quote.market.suggested_token_id);
}

async fn wait_or_shutdown(
    shutdown: &mut std::pin::Pin<Box<impl std::future::Future<Output = std::io::Result<()>>>>,
    milliseconds: u64,
) -> Result<()> {
    tokio::select! {
        result = shutdown.as_mut() => {
            result.context(
                "Ctrl+C sinyali okunamadı",
            )?;

            bail!(
                "Kapatma sinyali alındı"
            );
        }

        _ = sleep(
            Duration::from_millis(
                milliseconds,
            ),
        ) => Ok(()),
    }
}

fn parse_signature_type(value: &str) -> Result<(SignatureType, &'static str)> {
    match value.trim() {
        "1" => Ok((SignatureType::Proxy, "POLY_PROXY")),

        "2" => Ok((SignatureType::GnosisSafe, "GNOSIS_SAFE")),

        "3" => Ok((SignatureType::Poly1271, "POLY_1271")),

        other => bail!(
            "Desteklenmeyen \
             POLYMARKET_SIGNATURE_TYPE: {other}. \
             Proxy=1, GnosisSafe=2, Poly1271=3"
        ),
    }
}

/*
SDK Decimal/U256 benzeri sayısal alanları finansal yaşam döngüsünde
sessizce 0'a çevirmiyoruz. Parse hatası açık bir güvenlik hatasıdır.
*/
fn parse_nonnegative_number(value: &impl ToString, field_name: &str) -> Result<f64> {
    let text = value.to_string();
    let parsed = text
        .parse::<f64>()
        .with_context(|| format!("{field_name} sayıya dönüştürülemedi: {text}"))?;

    if !parsed.is_finite() || parsed < 0.0 {
        bail!("{field_name} sonlu ve negatif olmayan bir sayı olmalı: {parsed}");
    }

    Ok(parsed)
}

/*
SELL miktarını aşağı yuvarlar. Böylece hesaplanan/raporlanan dolumdan
fazla conditional token satılmaya çalışılmaz.
*/
fn floor_to_decimals(value: f64, decimals: usize) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    ((value * factor) + EPSILON).floor() / factor
}

/*
Miktarı belirtilen ondalık sayısına
yukarı yuvarlar.

20.001 -> 20.01
50.000 -> 50.00
*/
fn round_up_to_decimals(value: f64, decimals: usize) -> f64 {
    let factor = 10_f64.powi(decimals as i32);

    ((value * factor) - EPSILON).ceil() / factor
}

/*
f64 değeri önce sabit hassasiyetli metne,
ardından Decimal değerine dönüştürülür.
*/
fn decimal_from_f64(value: f64, decimals: usize, field_name: &str) -> Result<Decimal> {
    if !value.is_finite() || value <= 0.0 {
        bail!("{field_name} geçersiz: {value}");
    }

    let text = format!("{value:.decimals$}");

    let decimal = Decimal::from_str(&text).with_context(|| {
        format!(
            "{field_name} Decimal \
                     olarak okunamadı: {text}"
        )
    })?;

    if decimal.scale() != decimals as u32 {
        bail!(
            "{field_name} Decimal ölçeği \
             beklenenden farklı: \
             değer={decimal}, \
             beklenen={decimals}, \
             gerçek={}",
            decimal.scale()
        );
    }

    Ok(decimal)
}

/*
Tick büyüklüğünden fiyatın kaç ondalıklı
gönderileceğini hesaplar.

0.01   -> 2
0.001  -> 3
0.0001 -> 4
*/
fn decimal_places(tick_size: f64) -> usize {
    for places in 0..=8 {
        let scaled = tick_size * 10_f64.powi(places as i32);

        if (scaled - scaled.round()).abs() < 1e-8 {
            return places;
        }
    }

    8
}

fn price_to_key(price: f64) -> i64 {
    (price * PRICE_SCALE).round() as i64
}

fn key_to_price(key: i64) -> f64 {
    key as f64 / PRICE_SCALE
}

fn value_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),

        Value::String(text) => text.parse::<f64>().ok(),

        _ => None,
    }
}

fn normalize_base_url(value: &str) -> String {
    value.trim().trim_end_matches('/').to_string()
}

fn is_http_url(value: &str) -> bool {
    let value = value.trim();
    (value.starts_with("https://") || value.starts_with("http://")) && value.len() > "http://".len()
}

fn is_websocket_url(value: &str) -> bool {
    let value = value.trim();
    (value.starts_with("wss://") || value.starts_with("ws://")) && value.len() > "ws://".len()
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };

    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),

        "0" | "false" | "no" | "off" => Ok(false),

        _ => bail!(
            "{name} boolean olarak \
             okunamadı: {value}"
        ),
    }
}

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };

    value
        .trim()
        .parse::<T>()
        .map_err(|error| anyhow::anyhow!("{name} okunamadı: {error}"))
}

fn env_optional_parse<T>(name: &str) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };

    if value.trim().is_empty() {
        return Ok(None);
    }

    value
        .trim()
        .parse::<T>()
        .map(Some)
        .map_err(|error| anyhow::anyhow!("{name} okunamadı: {error}"))
}
