use anyhow::{format_err, Result};
use chrono::{serde::ts_seconds, DateTime, Utc};
use serde_derive::Serialize;

#[derive(Serialize)]
pub struct MetricValue {
    value: u64,
    #[serde(with = "ts_seconds")]
    timestamp: DateTime<Utc>,
}

#[derive(Serialize)]
#[serde(tag = "metric")]
pub enum Metric {
    #[serde(rename(serialize = "apps.event.incoming_requests_total"))]
    IncomingQueueRequests(MetricValue),
    #[serde(rename(serialize = "apps.event.incoming_responses_total"))]
    IncomingQueueResponses(MetricValue),
    #[serde(rename(serialize = "apps.event.incoming_events_total"))]
    IncomingQueueEvents(MetricValue),
    #[serde(rename(serialize = "apps.event.outgoing_requests_total"))]
    OutgoingQueueRequests(MetricValue),
    #[serde(rename(serialize = "apps.event.outgoing_responses_total"))]
    OutgoingQueueResponses(MetricValue),
    #[serde(rename(serialize = "apps.event.outgoing_events_total"))]
    OutgoingQueueEvents(MetricValue),
}

pub fn get_metrics(
    qc_handle: svc_agent::queue_counter::QueueCounterHandle,
    payload: serde_json::Value,
) -> Result<Vec<Metric>> {
    let now = Utc::now();

    let metrics = {
        let x: Option<u64> = payload.get("duration").and_then(|v| v.as_u64());
        let stats = qc_handle
            .get_stats(x.unwrap_or(5))
            .map_err(|e| format_err!("Failed to get status, reason = {}", e))?;

        vec![
            Metric::IncomingQueueRequests(MetricValue {
                value: stats.incoming_requests,
                timestamp: now,
            }),
            Metric::IncomingQueueResponses(MetricValue {
                value: stats.incoming_responses,
                timestamp: now,
            }),
            Metric::IncomingQueueEvents(MetricValue {
                value: stats.incoming_events,
                timestamp: now,
            }),
            Metric::OutgoingQueueRequests(MetricValue {
                value: stats.outgoing_requests,
                timestamp: now,
            }),
            Metric::OutgoingQueueResponses(MetricValue {
                value: stats.outgoing_responses,
                timestamp: now,
            }),
            Metric::OutgoingQueueEvents(MetricValue {
                value: stats.outgoing_events,
                timestamp: now,
            }),
        ]
    };
    Ok(metrics)
}
