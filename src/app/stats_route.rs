use std::collections::HashMap;

use async_std::stream::StreamExt;
use async_std::sync::Sender;
use log::{error, warn};
use serde_derive::Deserialize;
use svc_agent::AgentId;
use svc_authn::Authenticable;

#[derive(Deserialize)]
struct ExternalMetric {
    pub value: Option<serde_json::Value>,
    #[serde(rename = "metric")]
    pub key: String,
}

#[derive(Clone)]
pub struct StatsRoute {
    tx: Sender<StatsRouteCommand>,
}

enum StatsRouteCommand {
    GetStats(Sender<String>),
    AppendStat(AgentId, ExternalMetric),
}

impl StatsRoute {
    pub fn start(http: crate::app::config::HttpConfig) -> Self {
        let (tx, mut rx) = async_std::sync::channel(1000);

        async_std::task::spawn(async move {
            let mut s = HashMap::new();

            loop {
                if let Some(x) = rx.next().await {
                    match x {
                        StatsRouteCommand::AppendStat(agent_id, metric) => {
                            s.entry((agent_id, metric.key)).or_insert(metric.value);
                        }
                        StatsRouteCommand::GetStats(chan) => {
                            chan.send(Self::collect_hashmap(&s)).await;
                        }
                    }
                }
            }
        });

        let route = Self { tx };
        let route_ = route.clone();

        std::thread::Builder::new()
            .name(String::from("tide-metrics"))
            .spawn(move || {
                warn!("StatsRoute listening on http://{}", http.metrics_addr);
                let route = route_;

                let mut app = tide::with_state(route);
                app.at("/metrics")
                    .get(|req: tide::Request<StatsRoute>| async move {
                        match req.state().get_stats().await {
                            Ok(text) => {
                                let mut res = tide::Response::new(200);
                                res.set_body(tide::Body::from_string(text));
                                Ok(res)
                            }
                            Err(e) => {
                                error!("Something went wrong: {:?}", e);
                                let mut res = tide::Response::new(500);
                                res.set_body(tide::Body::from_string(
                                    "Something went wrong".into(),
                                ));
                                Ok(res)
                            }
                        }
                    });

                if let Err(e) = async_std::task::block_on(app.listen(http.metrics_addr)) {
                    error!("Tide future completed with error = {:?}", e);
                }
            })
            .expect("Failed to spawn tide-metrics thread");

        route
    }

    async fn get_stats(&self) -> Result<String, async_std::sync::RecvError> {
        let (tx, rx) = async_std::sync::channel(1);
        self.tx.send(StatsRouteCommand::GetStats(tx)).await;

        rx.recv().await
    }

    pub async fn update_with_value(&self, json_payload: &serde_json::Value, sender: &AgentId) {
        let payload = json_payload.clone();
        match serde_json::from_value::<Vec<ExternalMetric>>(payload) {
            Ok(metrics) => {
                for metric in metrics {
                    // if not old format with apps.app_name. prefix
                    if metric.key.find('.').is_none() {
                        self.update(sender.clone(), metric).await;
                    }
                }
            }
            Err(e) => error!(
                "Failed to deserialize metrics from payload {}, reason = {:?}",
                json_payload, e
            ),
        }
    }

    async fn update(&self, agent_id: AgentId, metric: ExternalMetric) {
        self.tx
            .send(StatsRouteCommand::AppendStat(agent_id, metric))
            .await
    }

    fn collect_hashmap(map: &HashMap<(AgentId, String), Option<serde_json::Value>>) -> String {
        let mut acc = String::from("");
        for ((agent_id, key), val) in map {
            if let Some(v) = val {
                acc.push_str(&format!(
                    "{}{{agent_label=\"{}\",account_id=\"{}\"}} {}\n",
                    key,
                    agent_id.label(),
                    agent_id.as_account_id(),
                    v
                ));
            }
        }
        acc
    }
}
