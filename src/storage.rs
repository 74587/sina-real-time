use anyhow::Result;
use chrono::Local;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tracing::info;

pub struct StorageWriter;

impl StorageWriter {
    /// Reads raw records from the channel and writes them to daily CSV files.
    /// Files are named `data_YYYY-MM-DD.csv` in `output_dir`.
    /// Format: received_at,code,fields
    pub async fn run(mut rx: Receiver<String>, output_dir: PathBuf) -> Result<()> {
        let mut current_date = String::new();
        let mut writer: Option<BufWriter<File>> = None;
        let mut total: u64 = 0;
        let mut since_last_stat: u64 = 0;

        // Flush CSV every 5 seconds
        let mut flush_tick = tokio::time::interval(Duration::from_secs(5));
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Print throughput stats every 60 seconds (heartbeat so user sees tool is alive)
        let mut stat_tick = tokio::time::interval(Duration::from_secs(60));
        stat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // skip the immediate first tick
        stat_tick.tick().await;

        loop {
            tokio::select! {
                biased;

                msg = rx.recv() => {
                    let Some(record) = msg else {
                        break; // all senders dropped = shutdown
                    };

                    let now = Local::now();
                    let date_str = now.format("%Y-%m-%d").to_string();
                    let timestamp = now.format("%Y-%m-%dT%H:%M:%S%.3f").to_string();

                    // Rotate to a new file on date change
                    if date_str != current_date {
                        if let Some(mut w) = writer.take() {
                            w.flush()?;
                        }
                        writer = Some(open_daily_file(&output_dir, &date_str)?);
                        current_date = date_str;
                    }

                    if let Some(w) = writer.as_mut() {
                        if let Some((code, fields)) = record.split_once('=') {
                            writeln!(w, "{timestamp},{code},\"{fields}\"")?;
                        } else {
                            writeln!(w, "{timestamp},,\"{record}\"")?;
                        }
                        total += 1;
                        since_last_stat += 1;
                    }
                }

                _ = flush_tick.tick() => {
                    if let Some(w) = writer.as_mut() {
                        w.flush()?;
                    }
                }

                _ = stat_tick.tick() => {
                    let rate = since_last_stat / 60;
                    if since_last_stat > 0 {
                        info!("[stats] {rate} records/sec (total: {total})");
                    } else {
                        info!("[stats] No data in last 60s (total: {total}) — market may be closed or connections reconnecting");
                    }
                    since_last_stat = 0;
                }
            }
        }

        // Final flush before exit
        if let Some(mut w) = writer {
            w.flush()?;
        }
        info!("Storage closed. Total records written: {total}");
        Ok(())
    }
}

fn open_daily_file(dir: &PathBuf, date: &str) -> Result<BufWriter<File>> {
    let path = dir.join(format!("data_{date}.csv"));
    let is_new = !path.exists();
    let file = OpenOptions::new().create(true).append(true).open(&path)?;
    let mut writer = BufWriter::with_capacity(512 * 1024, file); // 512 KB buffer
    if is_new {
        writeln!(writer, "received_at,code,fields")?;
    }
    info!("Opened data file: {:?}", path);
    Ok(writer)
}

