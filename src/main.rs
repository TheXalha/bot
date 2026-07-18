mod logger;
mod market_scanner;
mod trading_bot;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logger ayarlarının .env içinden okunabilmesi için dosya önce burada yüklenir.
    // .env yoksa ortam değişkenleri ve varsayılanlar kullanılmaya devam eder.
    let _ = dotenvy::dotenv();

    logger::init_from_env()?;

    crate::log_info!(
        "application_start",
        "PolyBot başlatılıyor; session_id={}",
        logger::session_id().unwrap_or_else(|| "unknown".to_string())
    );

    let result = trading_bot::run().await;

    match &result {
        Ok(()) => {
            crate::log_info!("application_stop", "PolyBot normal biçimde durdu");
        }
        Err(error) => {
            crate::log_error!(
                "application_fatal_error",
                "PolyBot fatal hata ile durdu: {error:#}"
            );
        }
    }

    logger::flush();
    result
}
