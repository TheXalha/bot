# PolyBot ayrıntılı log sistemi

Bu sürümde `src/bin/market_scanner.rs` yoktur. Uygulama tek binary olarak
`src/main.rs -> trading_bot::run()` akışıyla çalışır.

## Üretilen dosyalar

Varsayılan olarak proje kökündeki `logs/` klasörüne günlük dosyalar yazılır:

- `polybot-YYYY-MM-DD.log`
  - İnsan tarafından okunabilir sekme ayrımlı tam log.
- `polybot-YYYY-MM-DD.jsonl`
  - Her satırı bağımsız JSON olan yapılandırılmış tam log.
- `polybot-errors-YYYY-MM-DD.jsonl`
  - Yalnızca WARN ve ERROR seviyeleri.
- `polybot-scoring-YYYY-MM-DD.csv`
  - Her scoring sorgusunun ayrı analiz tablosu.

Her log kaydında UTC zaman, seviye, session ID, process ID, thread,
component, event, kaynak dosya/satır ve mesaj bulunur.

## Scoring CSV sütunları

- `timestamp`
- `session_id`
- `order_id`
- `token_id`
- `market_slug`
- `check_number`
- `order_age_ms`
- `request_latency_ms`
- `state`: `SCORING`, `NOT_SCORING` veya `ERROR`
- `in_grace`
- `consecutive_false`
- `consecutive_errors`

Bu CSV sayesinde ilk scoring doğrulamasının kaç milisaniyede geldiği,
endpoint gecikmeleri ve yanlış requote ihtimali ölçülebilir.

## Eklenen önemli olaylar

- `logger_initialized`
- `application_start`
- `application_stop`
- `application_fatal_error`
- `configuration_loaded`
- `scan_started`
- `scan_completed`
- `scan_finished_empty_preliminary`
- `scan_finished_empty_deep_filter`
- `quote_selected`
- `scoring_monitor_started`
- `scoring_first_confirmed`
- `scoring_restored`
- `scoring_pending_in_grace`
- `scoring_false_after_grace`
- `scoring_request_failed`
- `scoring_endpoint_degraded`
- `scoring_requote_triggered`
- `scoring_monitor_summary`
- `unhandled_panic`

Mevcut bütün `println!` ve `eprintln!` çağrıları da dosya/satır bilgisiyle
logger üzerinden geçirilir. Böylece API retry, WebSocket, order sorgusu,
cancel, BUY/SELL uzlaştırması ve scanner hataları günlük dosyalarda kalır.

## Güvenlik

Logger private key değerini bilinçli olarak yazmaz. Gelecekte bir mesaj içinde
`POLYMARKET_PRIVATE_KEY=...` biçiminde değer üretilirse logger bunu maskelemeye
çalışır. Yine de log dosyaları cüzdan adresi, order ID, token ID, market ve fiyat
bilgisi içerdiğinden herkese açık paylaşılmamalıdır.

`.gitignore` dosyanıza şunu ekleyin:

```gitignore
logs/
*.log
```

## Uygulama

1. `src/main.rs`, `src/market_scanner.rs` ve `src/trading_bot.rs` dosyalarını değiştirin.
2. Yeni `src/logger.rs` dosyasını ekleyin.
3. `src/bin/market_scanner.rs` ve boşsa `src/bin/` klasörünü kaldırılmış tutun.
4. Örnek ayarları mevcut `.env` dosyanıza ekleyin.
5. Proje kökünde çalıştırın:

```bash
cargo fmt --all
cargo check
```

Bu çalışma ortamında Rust/Cargo kurulu olmadığı için gerçek `cargo check`
çalıştırılamadı. Dosyalarda ayraç, string, yorum ve modül bağlantısı için statik
kontroller uygulanmıştır.
