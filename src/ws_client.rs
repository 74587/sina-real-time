use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, interval, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

const WSS_BASE: &str = "wss://hq.sinajs.cn/wskt";
const ORIGIN: &str = "https://finance.sina.com.cn";
const USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

/// Send a WebSocket ping at this interval to keep the connection alive.
const PING_INTERVAL: Duration = Duration::from_secs(20);

/// If no message (data or Pong) is received within this duration, treat the connection as dead.
/// Must be greater than PING_INTERVAL so a pong reply can reset the clock first.
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// Runs a WebSocket connection for the given stock chunk, reconnecting on error.
pub async fn spawn_connection(stocks: Vec<String>, tx: Sender<String>, conn_id: usize) {
    let list = stocks.join(",");
    let url = format!("{}?list={}", WSS_BASE, list);

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);
    let mut attempt = 0u32;

    // Spread out initial reconnects so all 11 connections don't hammer the server at once.
    // Each conn waits an extra conn_id * 150ms.
    if conn_id > 0 {
        sleep(Duration::from_millis(conn_id as u64 * 150)).await;
    }

    loop {
        if attempt > 0 {
            // Add per-connection jitter to prevent thundering-herd reconnects.
            let jitter = Duration::from_millis(conn_id as u64 * 50);
            warn!(
                "[conn {conn_id}] Reconnect attempt {attempt}, waiting {:.1}s",
                (backoff + jitter).as_secs_f32()
            );
            sleep(backoff + jitter).await;
            backoff = (backoff * 2).min(max_backoff);
        }

        // Stop if the storage channel is gone (shutdown)
        if tx.is_closed() {
            info!("[conn {conn_id}] Channel closed, stopping");
            return;
        }

        match build_request(&url) {
            Err(e) => {
                error!("[conn {conn_id}] Bad request: {e}");
                return;
            }
            Ok(req) => match connect_async(req).await {
                Err(e) => {
                    error!("[conn {conn_id}] Connect failed: {e}");
                    attempt += 1;
                }
                Ok((ws_stream, _)) => {
                    info!("[conn {conn_id}] Connected ({} stocks)", stocks.len());
                    // Reset backoff only after we've successfully received data.
                    // If the server immediately drops us, keep backing off.
                    let received = read_loop(ws_stream, &tx, conn_id).await;
                    match received {
                        Ok(n) if n > 0 => {
                            // Got real data → healthy connection, reset backoff
                            backoff = Duration::from_secs(1);
                            attempt = 0;
                            info!("[conn {conn_id}] Disconnected after {n} records");
                        }
                        Ok(_) => {
                            warn!("[conn {conn_id}] Connected but received no data (idle timeout or immediate close)");
                            attempt += 1;
                        }
                        Err(e) => {
                            warn!("[conn {conn_id}] Read error: {e}");
                            // Server hard-closed the TCP connection — reconnect immediately.
                            // If the new connect also fails, normal backoff will kick in.
                            let msg = e.to_string();
                            if msg.contains("Connection reset") || msg.contains("without closing") {
                                backoff = Duration::from_secs(1);
                                attempt = 0; // skip backoff wait on next iteration
                            } else {
                                attempt += 1;
                            }
                        }
                    }
                }
            },
        }
    }
}

fn build_request(url: &str) -> Result<http::Request<()>> {
    // tungstenite requires Sec-WebSocket-Key when using a custom http::Request
    let key = tokio_tungstenite::tungstenite::handshake::client::generate_key();
    Ok(http::Request::builder()
        .uri(url)
        .header("Host", "hq.sinajs.cn")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", key)
        .header("Origin", ORIGIN)
        .header("User-Agent", USER_AGENT)
        .body(())?)
}

/// Returns the number of records successfully forwarded, or an error.
async fn read_loop(
    mut stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    tx: &Sender<String>,
    conn_id: usize,
) -> Result<u64> {
    let mut count: u64 = 0;

    // Send a ping every PING_INTERVAL to keep the connection alive.
    let mut ping_ticker = interval(PING_INTERVAL);
    ping_ticker.tick().await; // consume the first immediate tick

    // Resettable idle-timeout: reset on every received message (data or Pong).
    let mut idle = Box::pin(sleep(IDLE_TIMEOUT));

    loop {
        tokio::select! {
            _ = ping_ticker.tick() => {
                if let Err(e) = stream.send(Message::Ping(vec![])).await {
                    warn!("[conn {conn_id}] Ping failed: {e}");
                    return Ok(count);
                }
                debug!("[conn {conn_id}] Sent keepalive ping");
            }

            _ = &mut idle => {
                warn!(
                    "[conn {conn_id}] No data for {}s, reconnecting",
                    IDLE_TIMEOUT.as_secs()
                );
                return Ok(count);
            }

            msg_opt = stream.next() => {
                // Any incoming message (Pong, data, …) proves the connection is alive.
                idle.as_mut().reset(Instant::now() + IDLE_TIMEOUT);

                match msg_opt {
                    None => return Ok(count), // stream closed
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(msg)) => match msg {
                        Message::Text(text) => {
                            let text_str: &str = &text;
                            for token in text_str.split_ascii_whitespace() {
                                if token.contains('=') {
                                    if tx.send(token.to_string()).await.is_err() {
                                        return Ok(count); // channel closed = shutdown
                                    }
                                    count += 1;
                                }
                            }
                        }
                        Message::Close(_) => {
                            warn!("[conn {conn_id}] Server sent Close");
                            return Ok(count);
                        }
                        // tungstenite auto-replies to server Pings; Pong frames just reset idle.
                        _ => {}
                    },
                }
            }
        }
    }
}

