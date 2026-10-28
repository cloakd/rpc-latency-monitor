use std::net::IpAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use crate::rpc::methods::RpcMethod;
use crate::rpc::{CallResult, CallStatus};

const BUFFER: usize = 4096;
const FLUSH_ROWS: usize = 500;
const FLUSH_SECONDS: u64 = 30;
const MAX_BUFFERED: usize = 10000;

#[derive(Clone)]
pub struct SampleLogger {
    tx: mpsc::Sender<Sample>,
    infra: String,
    region: String,
}

#[derive(Serialize)]
struct Sample {
    ts: f64,
    provider: String,
    method: &'static str,
    infra: String,
    region: String,
    target: String,
    status: &'static str,
    error_kind: &'static str,
    latency_ms: f64,
    slot: Option<u64>,
    endpoint_ip: Option<IpAddr>,
}

impl SampleLogger {
    pub fn from_env(region: &str, metrics: Metrics) -> Option<Self> {
        let url = std::env::var("SAMPLE_LOG_URL")
            .ok()
            .filter(|v| !v.is_empty())?;
        let token = std::env::var("SAMPLE_LOG_TOKEN")
            .ok()
            .filter(|v| !v.is_empty())?;
        let infra = std::env::var("MONITOR_INFRA").unwrap_or_else(|_| "gcp".to_string());
        let (tx, rx) = mpsc::channel(BUFFER);
        tokio::spawn(flush_loop(url, token, rx, metrics));
        tracing::info!("sample logging enabled");
        Some(Self {
            tx,
            infra,
            region: region.to_string(),
        })
    }

    pub fn log(
        &self,
        provider: &str,
        method: RpcMethod,
        status: CallStatus,
        target: &str,
        result: &CallResult,
    ) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or_default();
        let _ = self.tx.try_send(Sample {
            ts,
            provider: provider.to_string(),
            method: method.label_for_target(target),
            infra: self.infra.clone(),
            region: self.region.clone(),
            target: target.to_string(),
            status: status.label(),
            error_kind: status.error_kind().unwrap_or("none"),
            latency_ms: result.latency.as_secs_f64() * 1000.0,
            slot: result.observed_slot,
            endpoint_ip: result.endpoint_ip,
        });
    }
}

async fn flush_loop(url: String, token: String, mut rx: mpsc::Receiver<Sample>, metrics: Metrics) {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
    else {
        return;
    };
    let mut buffer = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_secs(FLUSH_SECONDS));
    loop {
        tokio::select! {
            received = rx.recv() => match received {
                Some(sample) => {
                    buffer.push(sample);
                    if buffer.len() >= FLUSH_ROWS {
                        flush(&client, &url, &token, &mut buffer, &metrics).await;
                    }
                }
                None => {
                    flush(&client, &url, &token, &mut buffer, &metrics).await;
                    return;
                }
            },
            _ = tick.tick() => {
                if !buffer.is_empty() {
                    flush(&client, &url, &token, &mut buffer, &metrics).await;
                }
            }
        }
    }
}

async fn flush(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    buffer: &mut Vec<Sample>,
    metrics: &Metrics,
) {
    while !buffer.is_empty() {
        if !flush_chunk(client, url, token, buffer, metrics).await {
            return;
        }
    }
}

async fn flush_chunk(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    buffer: &mut Vec<Sample>,
    metrics: &Metrics,
) -> bool {
    let rows: Vec<Sample> = buffer.drain(..buffer.len().min(FLUSH_ROWS)).collect();
    let count = rows.len();
    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({ "rows": &rows }))
        .send()
        .await;
    let failure = match response {
        Ok(r) if r.status().is_success() => {
            metrics.record_sample_flush("ok", 1);
            return true;
        }
        Ok(r) => {
            metrics.record_sample_flush("rejected", 1);
            format!("rejected with {}", r.status())
        }
        Err(error) => {
            metrics.record_sample_flush("failed", 1);
            error.to_string()
        }
    };
    if count + buffer.len() <= MAX_BUFFERED {
        tracing::warn!(%failure, count, "sample flush failed, will retry");
        buffer.splice(0..0, rows);
    } else {
        metrics.record_sample_flush("dropped", 1);
        tracing::warn!(%failure, count, "sample flush failed, buffer full, dropping batch");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::ErrorKind;

    #[test]
    fn signature_samples_separate_recent_and_archival_outcomes() {
        let (tx, mut rx) = mpsc::channel(4);
        let logger = SampleLogger {
            tx,
            infra: "tsw".to_string(),
            region: "ams".to_string(),
        };
        for (target, status, expected_method) in [
            ("", CallStatus::Success, "getSignaturesForAddress_recent"),
            (
                "",
                CallStatus::Error(ErrorKind::Stale),
                "getSignaturesForAddress_recent",
            ),
            (
                "archival",
                CallStatus::Success,
                "getSignaturesForAddress_archival",
            ),
            (
                "archival",
                CallStatus::Error(ErrorKind::RpcError(-32017)),
                "getSignaturesForAddress_archival",
            ),
        ] {
            let result = CallResult {
                latency: Duration::from_millis(12),
                status,
                observed_slot: None,
                signature: None,
                archival_signature: None,
                accounts: Vec::new(),
                claim: None,
                endpoint_ip: None,
            };
            logger.log(
                "provider",
                RpcMethod::GetSignaturesForAddress,
                status,
                target,
                &result,
            );
            let sample = serde_json::to_value(rx.try_recv().unwrap()).unwrap();
            assert_eq!(sample["method"], expected_method);
            assert_eq!(sample["target"], target);
            assert_eq!(sample["status"], status.label());
            assert_eq!(sample["error_kind"], status.error_kind().unwrap_or("none"));
        }
    }
}
