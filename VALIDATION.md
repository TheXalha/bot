# Statik doğrulama raporu

## Uygulanan kontroller

- `src/main.rs`, `src/logger.rs`, `src/market_scanner.rs` ve
  `src/trading_bot.rs` için parantez, köşeli parantez ve süslü parantez dengesi.
- String, char, satır yorumu ve iç içe blok yorumu kapanış kontrolleri.
- `BotConfig` alanları ile `bot_config.<alan>` referanslarının karşılaştırılması.
- `ScannerConfig` alanları ile scanner/bot referanslarının karşılaştırılması.
- Eski sabit scoring kullanımının kaldırıldığı kontrolü.
- `src/bin/market_scanner.rs` dosyasının pakette bulunmadığı kontrolü.
- `market_scanner.rs` ve `trading_bot.rs` içindeki eski doğrudan
  `println!`/`eprintln!` çağrılarının logger makrolarına yönlendirildiği kontrolü.
- Logger'ın private key değerini yapılandırılmış startup alanlarına eklemediği kontrolü.

## Sonuç

Yukarıdaki statik kontroller başarılıdır.

## Sınır

Bu çalışma ortamında `rustc`, `cargo` ve `rustfmt` kurulu değildir. Bu nedenle
bağımlılıklarla gerçek derleme yapılamadı. Projenizde dosyaları kopyaladıktan
sonra aşağıdaki iki komut nihai doğrulamadır:

```bash
cargo fmt --all
cargo check
```

Logger yeni bir üçüncü taraf bağımlılık eklemez. Projede zaten kullanılan
`anyhow`, `chrono` ve `serde_json` crate'lerini kullanır.
