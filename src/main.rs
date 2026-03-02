use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::info;

mod storage;
mod ws_client;

use storage::StorageWriter;
use ws_client::spawn_connection;

/// Sina Finance WebSocket real-time data collector.
///
/// Connects to wss://hq.sinajs.cn/wskt and persists stock tick data to daily CSV files.
/// Large stock lists are split across multiple parallel connections (--chunk-size).
#[derive(Parser)]
#[command(name = "sina-collector", version)]
struct Cli {
    /// Stock list file — one code per line, e.g. sz300394 (lines starting with # are ignored)
    #[arg(short, long, default_value = "stocks.txt")]
    stocks: PathBuf,

    /// Directory for output CSV files
    #[arg(short, long, default_value = "data")]
    output: PathBuf,

    /// Max stocks per WebSocket connection (tune based on URL length / server limits)
    #[arg(long, default_value_t = 500)]
    chunk_size: usize,

    /// Internal channel buffer (records in flight between WS tasks and storage)
    #[arg(long, default_value_t = 131_072)]
    buffer: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sina_collector=info".into()),
        )
        .init();

    let cli = Cli::parse();

    // Load and validate stock list
    let content = std::fs::read_to_string(&cli.stocks)
        .with_context(|| format!("Cannot read stock list: {:?}", cli.stocks))?;

    let stocks: Vec<String> = content
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();

    anyhow::ensure!(!stocks.is_empty(), "Stock list is empty");
    info!(
        "Loaded {} stocks from {:?}",
        stocks.len(),
        cli.stocks
    );

    std::fs::create_dir_all(&cli.output)
        .with_context(|| format!("Cannot create output dir: {:?}", cli.output))?;

    // Shared channel: WS tasks → storage task
    let (tx, rx) = mpsc::channel::<String>(cli.buffer);

    // Storage runs as a background task
    let output_dir = cli.output.clone();
    let storage_handle = tokio::spawn(async move {
        if let Err(e) = StorageWriter::run(rx, output_dir).await {
            tracing::error!("Storage error: {e:#}");
        }
    });

    // Split stocks into chunks, one WebSocket connection per chunk
    let chunks: Vec<Vec<String>> = stocks
        .chunks(cli.chunk_size)
        .map(|c| c.to_vec())
        .collect();

    info!(
        "Starting {} connection(s) (~{} stocks each)",
        chunks.len(),
        cli.chunk_size
    );

    let mut conn_handles = Vec::with_capacity(chunks.len());
    for (i, chunk) in chunks.into_iter().enumerate() {
        let tx = tx.clone();
        conn_handles.push(tokio::spawn(spawn_connection(chunk, tx, i)));
    }
    // Drop the original tx so storage exits when all connections close
    drop(tx);

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    info!("Ctrl+C received — shutting down");

    for h in conn_handles {
        h.abort();
    }

    // Wait for storage to flush
    let _ = storage_handle.await;
    info!("Done");
    Ok(())
}

