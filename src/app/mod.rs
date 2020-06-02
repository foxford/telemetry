use anyhow::{format_err, Context, Result};
use async_std::{stream, stream::StreamExt, sync::channel, task};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use isahc::{config::Configurable, config::VersionNegotiation, HttpClient};
use log::{error, info, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use svc_agent::mqtt::{
    compat, Agent, AgentBuilder, ConnectionMode, QoS, ResponseStatus, SubscriptionTopic,
};
use svc_agent::{AccountId, AgentId, Authenticable, SharedGroup, Subscription};
use svc_authn::{jose::Algorithm, token::jws_compact};
use svc_error::extension::sentry;

type Error = std::io::Error;
type ErrorKind = std::io::ErrorKind;

pub(crate) const API_VERSION: &str = "v1";
const INTERNAL_MESSAGE_QUEUE_SIZE: usize = 1_000_000;
const MAX_HTTP_CONNECTION: usize = 256;
const MAX_ATTEMPTS: u8 = 3;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct IdTokenConfig {
    #[serde(deserialize_with = "svc_authn::serde::algorithm")]
    algorithm: Algorithm,
    #[serde(deserialize_with = "svc_authn::serde::file")]
    key: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TopMindConfig {
    uri: String,
    token: String,
    timeout: Option<u64>,
    retry: Option<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status")]
#[serde(rename_all = "lowercase")]
enum TopMindResponse {
    Success(TopMindResponseSuccess),
    Error(TopMindResponseError),
}

#[derive(Debug, Deserialize)]
struct TopMindResponseSuccess {
    op_id: String,
}

#[derive(Debug, Deserialize)]
struct TopMindResponseError {
    message: String,
    #[serde(rename = "reasonPhrase")]
    reason_phrase: String,
}

impl std::fmt::Display for TopMindResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reason = {}", &self.reason_phrase)
    }
}

impl std::error::Error for TopMindResponseError {}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize)]
struct AccountAddress {
    account_id: AccountId,
    version: String,
}

impl AccountAddress {
    fn new(account_id: AccountId, version: &str) -> Self {
        Self {
            account_id,
            version: version.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct AgentAddress {
    agent_id: AgentId,
    version: String,
}

impl AgentAddress {
    fn new(agent_id: AgentId, version: &str) -> Self {
        Self {
            agent_id,
            version: version.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
enum MessagingPattern {
    Broadcast(BroadcastMessagingPattern),
    Multicast(MulticastMessagingPattern),
    Unicast(UnicastMessagingPattern),
}

#[derive(Debug, Clone, Serialize)]
struct BroadcastMessagingPattern {
    from: AccountAddress,
    path: String,
}

impl BroadcastMessagingPattern {
    fn new(from: AccountAddress, path: &str) -> Self {
        Self {
            from,
            path: path.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct MulticastMessagingPattern {
    from: AgentId,
    to: AccountAddress,
}

impl MulticastMessagingPattern {
    fn new(from: AgentId, to: AccountAddress) -> Self {
        Self { from, to }
    }
}

#[derive(Debug, Clone, Serialize)]
struct UnicastMessagingPattern {
    from: AccountId,
    to: AgentAddress,
}

impl UnicastMessagingPattern {
    fn new(from: AccountId, to: AgentAddress) -> Self {
        Self { from, to }
    }
}

impl FromStr for MessagingPattern {
    type Err = anyhow::Error;

    fn from_str(topic: &str) -> Result<Self, Self::Err> {
        if topic.starts_with("apps") {
            let arr = topic.splitn(5, '/').collect::<Vec<&str>>();
            match &arr[..] {
                ["apps", from_account_id, "api", ref from_version, ref path] => {
                    let from_account_id = from_account_id
                        .parse()
                        .context("Error to parse account_id")?;
                    let address = AccountAddress::new(from_account_id, from_version);
                    let pattern =
                        MessagingPattern::Broadcast(BroadcastMessagingPattern::new(address, path));
                    Ok(pattern)
                }
                _ => Err(Error::new(
                    ErrorKind::Other,
                    format!("invalid value for the messaging pattern: {:?}", topic),
                )
                .into()),
            }
        } else {
            let arr = topic.splitn(6, '/').collect::<Vec<&str>>();
            match &arr[..] {
                ["agents", ref from_agent_id, "api", ref to_version, "out", ref to_account_id] => {
                    let from_agent_id = from_agent_id.parse().context("Error to parse agent_id")?;
                    let to_account_id =
                        to_account_id.parse().context("Error to parse account_id")?;
                    let address = AccountAddress::new(to_account_id, to_version);
                    let pattern = MessagingPattern::Multicast(MulticastMessagingPattern::new(
                        from_agent_id,
                        address,
                    ));
                    Ok(pattern)
                }
                ["agents", ref to_agent_id, "api", ref to_version, "in", ref from_account_id] => {
                    let from_account_id = from_account_id
                        .parse()
                        .context("Error to parse account_id")?;
                    let to_agent_id = to_agent_id.parse().context("Error to parse agent_id")?;
                    let address = AgentAddress::new(to_agent_id, to_version);
                    let pattern = MessagingPattern::Unicast(UnicastMessagingPattern::new(
                        from_account_id,
                        address,
                    ));
                    Ok(pattern)
                }
                _ => Err(Error::new(
                    ErrorKind::Other,
                    format!("invalid value for the messaging pattern: {:?}", topic),
                )
                .into()),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

fn json_flatten_prefix(key: &str, prefix: &str) -> String {
    if !prefix.is_empty() {
        [prefix, key].join(".")
    } else {
        key.to_owned()
    }
}

fn json_flatten(prefix: &str, json: &JsonValue, acc: &mut HashMap<String, JsonValue>) {
    if let Some(object) = json.as_object() {
        for (key, value) in object {
            if value.is_object() {
                json_flatten(&json_flatten_prefix(key, prefix), value, acc);
            } else {
                acc.insert(json_flatten_prefix(key, prefix), value.clone());
            }
        }
    }
}

fn json_flatten_one_level_deep(
    prefix: &str,
    json: &JsonValue,
    acc: &mut HashMap<String, JsonValue>,
) {
    if let Some(object) = json.as_object() {
        for (key, value) in object {
            if value.is_string() || value.is_number() || value.is_boolean() {
                acc.insert(json_flatten_prefix(key, prefix), value.clone());
            } else if value.is_object() && key == "tags" {
                json_flatten(&json_flatten_prefix(key, prefix), value, acc);
            }
        }
    }
}

fn adjust_request_properties(acc: &mut HashMap<String, JsonValue>) {
    acc.insert(String::from("properties.type"), json!("request"));
    adjust_properties(acc);
}

fn adjust_response_properties(acc: &mut HashMap<String, JsonValue>) {
    acc.insert(String::from("properties.type"), json!("response"));
    adjust_properties(acc);
}

fn adjust_event_properties(acc: &mut HashMap<String, JsonValue>) {
    acc.insert(String::from("properties.type"), json!("event"));
    adjust_properties(acc);
}

fn adjust_properties(acc: &mut HashMap<String, JsonValue>) {
    adjust_agent_id("properties.agent_id", acc);
    adjust_agent_id("properties.broker_agent_id", acc);
    adjust_tracking_id("properties.tracking_id", acc);
    replace_session_tracking_label("properties.session_tracking_label", acc);
    replace_integer("properties.status", acc);
    replace_integer("properties.broker_initial_processing_timestamp", acc);
    replace_integer("properties.broker_processing_timestamp", acc);
    replace_integer("properties.broker_timestamp", acc);
    replace_integer("properties.local_initial_timediff", acc);
    replace_integer("properties.initial_timestamp", acc);
    replace_integer("properties.timestamp", acc);
    replace_integer("properties.authorization_time", acc);
    replace_integer("properties.processing_time", acc);
    replace_integer("properties.cumulative_authorization_time", acc);
    replace_integer("properties.cumulative_processing_time", acc);
}

fn adjust_pattern(pattern: &MessagingPattern, acc: &mut HashMap<String, JsonValue>) {
    match pattern {
        MessagingPattern::Broadcast(_) => {
            adjust_account_id("pattern.from.account_id", acc);
        }
        MessagingPattern::Multicast(_) => {
            adjust_agent_id("pattern.from", acc);
            adjust_account_id("pattern.to.account_id", acc);
        }
        MessagingPattern::Unicast(_) => {
            adjust_account_id("pattern.from", acc);
            adjust_agent_id("pattern.to.agent_id", acc);
        }
    }
}

fn adjust_payload(acc: &mut HashMap<String, JsonValue>) {
    adjust_agent_id("payload.agent_id", acc);
    adjust_agent_id("payload.created_by", acc);
    adjust_useragent_tag("payload.tags.user_agent", acc);
}

fn adjust_useragent_tag(key: &str, acc: &mut HashMap<String, JsonValue>) {
    if let Some(JsonValue::String(ua_str)) = acc.get(key) {
        if let Ok(mut ua_json) = convert_ua_to_json(&ua_str) {
            ua_json.drain().for_each(|(ua_key, ua_val)| {
                let k = json_flatten_prefix(ua_key, key);
                acc.insert(k, ua_val);
            });
        }
    }
}

fn adjust_agent_id(key: &str, acc: &mut HashMap<String, JsonValue>) {
    if let Some(JsonValue::String(val)) = acc.get(key) {
        let arr = val.splitn(2, '.').collect::<Vec<&str>>();
        if let [ref label, ref account_id] = &arr[..] {
            let label = json!(label);
            let account_id = json!(account_id);
            let next = json_flatten_prefix("account_id", key);
            acc.insert(json_flatten_prefix("label", key), label);
            acc.insert(next.clone(), account_id);
            adjust_account_id(&next, acc);
        }
    }
}

fn adjust_account_id(key: &str, acc: &mut HashMap<String, JsonValue>) {
    if let Some(JsonValue::String(val)) = acc.get(key) {
        let arr = val.splitn(2, '.').collect::<Vec<&str>>();
        if let [ref label, ref audience] = &arr[..] {
            let label = json!(label);
            let audience = json!(audience);
            acc.insert(json_flatten_prefix("label", key), label);
            acc.insert(json_flatten_prefix("audience", key), audience);
        }
    }
}

fn adjust_tracking_id(key: &str, acc: &mut HashMap<String, JsonValue>) {
    if let Some(JsonValue::String(val)) = acc.get(key) {
        let arr = val.splitn(2, '.').collect::<Vec<&str>>();
        if let [ref label, ref session_id] = &arr[..] {
            let label = json!(label);
            let session_id = json!(session_id);
            let next = json_flatten_prefix("session_id", key);
            acc.insert(json_flatten_prefix("label", key), label);
            acc.insert(next.clone(), session_id);
            adjust_session_id(&next, acc);
        }
    }
}

fn adjust_session_id(key: &str, acc: &mut HashMap<String, JsonValue>) {
    if let Some(JsonValue::String(val)) = acc.get(key) {
        let arr = val.splitn(2, '.').collect::<Vec<&str>>();
        if let [ref agent_session_label, ref broker_session_label] = &arr[..] {
            let agent_session_label = json!(agent_session_label);
            let broker_session_label = json!(broker_session_label);
            acc.insert(
                json_flatten_prefix("agent_session_label", key),
                agent_session_label,
            );
            acc.insert(
                json_flatten_prefix("broker_session_label", key),
                broker_session_label,
            );
        }
    }
}

fn replace_session_tracking_label(key: &str, acc: &mut HashMap<String, JsonValue>) {
    if let Some(JsonValue::String(val)) = acc.get(key) {
        let arr = val.split(' ').collect::<Vec<&str>>();
        let arr = json!(arr);
        acc.insert(key.to_owned(), arr);
    }
}

fn replace_integer(key: &str, acc: &mut HashMap<String, JsonValue>) {
    if let Some(JsonValue::String(val)) = acc.get(key) {
        if let Ok(integer) = val.parse::<i64>() {
            let integer = json!(integer);
            acc.insert(key.to_owned(), integer);
        }
    }
}

fn subscribe(agent: &mut Agent) {
    let group = SharedGroup::new("loadbalancer", agent.id().as_account_id().clone());
    agent
        .subscribe(&"apps/+/api/+/#", QoS::AtMostOnce, Some(&group))
        .expect("Error subscribing to broadcast events");
    agent
        .subscribe(&"agents/+/api/+/out/+", QoS::AtMostOnce, Some(&group))
        .expect("Error subscribing to multicast requests and events");
    agent
        .subscribe(&"agents/+/api/+/in/+", QoS::AtMostOnce, Some(&group))
        .expect("Error subscribing to unicast requests and responses");
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn run() -> Result<()> {
    // Config
    let config = config::load().context("Failed to load config")?;
    info!("App config: {:?}", config);

    // Agent
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!("Agent id: {:?}", &agent_id);

    let token = jws_compact::TokenBuilder::new()
        .issuer(&agent_id.as_account_id().audience().to_string()) //?
        .subject(&agent_id)
        .key(config.id_token.algorithm, config.id_token.key.as_slice())
        .build()
        .context("Error creating an id token")?;

    let mut agent_config = config.mqtt.clone();
    agent_config.set_password(&token);

    let (mut agent, rx) = AgentBuilder::new(agent_id.clone(), API_VERSION)
        .connection_mode(ConnectionMode::Observer)
        .start(&agent_config)
        .context("Failed to create an agent")?;

    // Message loop for incoming messages of MQTT Agent
    let (mq_tx, mut mq_rx) = channel(INTERNAL_MESSAGE_QUEUE_SIZE);
    thread::spawn(move || {
        for message in rx {
            let mq_tx = mq_tx.clone();
            task::spawn(async move {
                mq_tx.send(message).await;
            });
        }
    });

    // Sentry
    if let Some(sentry_config) = config.sentry.as_ref() {
        svc_error::extension::sentry::init(sentry_config);
    }

    // Subscription
    subscribe(&mut agent);

    // Http client
    let topmind = Arc::new(config.topmind);
    let timeout = std::time::Duration::from_secs(topmind.timeout.unwrap_or(5));
    let client = Arc::new(
        HttpClient::builder()
            .version_negotiation(VersionNegotiation::http11())
            .max_connections(MAX_HTTP_CONNECTION)
            .timeout(timeout)
            .build()?,
    );

    // Throughput counters
    let incoming_throughput = Arc::new(AtomicUsize::new(0));
    let outgoing_throughput = Arc::new(AtomicUsize::new(0));
    task::spawn(reset(
        incoming_throughput.clone(),
        outgoing_throughput.clone(),
    ));

    while let Some(message) = mq_rx.next().await {
        incoming_throughput.fetch_add(1, Ordering::SeqCst);
        let outgoing_throughput = outgoing_throughput.clone();
        let client = client.clone();
        let mut agent = agent.clone();
        let agent_id = agent_id.clone();

        let topmind = topmind.clone();
        task::spawn(async move {
            match message {
                svc_agent::mqtt::Notification::Reconnection => {
                    subscribe(&mut agent);
                }
                svc_agent::mqtt::Notification::Publish(message) => {
                    let topic: &str = &message.topic_name;

                    let result = handle_message(
                        &client,
                        &agent_id,
                        topic,
                        message.payload.clone(),
                        topmind.clone(),
                    )
                    .await;

                    if let Err(err) = result {
                        error!(
                            "Error processing a message = '{text}' sent to the topic = '{topic}', {detail}",
                            text = String::from_utf8_lossy(message.payload.as_slice()),
                            topic = topic,
                            detail = err,
                        );

                        // Send to the Sentry
                        let svc_error = svc_error::Error::builder()
                            .kind(
                                "topmind.wapi",
                                "Error publishing a message to the TopMind W API",
                            )
                            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            .detail(&err.to_string())
                            .build();

                        sentry::send(svc_error)
                            .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
                    } else {
                        outgoing_throughput.fetch_add(1, Ordering::SeqCst);
                    }

                    // Log incoming messages
                    info!(
                        "Incoming message = '{}' sent to the topic = '{}', dup = '{}', pkid = '{:?}'",
                        String::from_utf8_lossy(message.payload.as_slice()), topic, message.dup, message.pkid,
                    );
                }
                _ => error!("An unsupported type of message = '{:?}'", message),
            }
        });
    }

    Ok(())
}

async fn reset(
    incoming: Arc<AtomicUsize>,
    outgoing: Arc<AtomicUsize>,
) -> Result<(), std::io::Error> {
    let mut interval = stream::interval(Duration::from_secs(1));
    while let Some(_) = interval.next().await {
        let now = chrono::offset::Utc::now();

        println!(
            "{} | throughput: {} => {}",
            now,
            incoming.swap(0, Ordering::SeqCst),
            outgoing.swap(0, Ordering::SeqCst)
        );
    }

    Ok(())
}

async fn handle_message(
    client: &HttpClient,
    agent_id: &AgentId,
    topic: &str,
    payload: Arc<Vec<u8>>,
    topmind: Arc<TopMindConfig>,
) -> Result<()> {
    let mut acc: HashMap<String, JsonValue> = HashMap::new();

    let pattern = topic
        .parse::<MessagingPattern>()
        .context("Failed to parse message pattern")?;
    let json_pattern =
        serde_json::to_value(pattern.clone()).context("Failed to serialize message pattern")?;
    json_flatten("pattern", &json_pattern, &mut acc);
    adjust_pattern(&pattern, &mut acc);

    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(payload.as_slice())
        .context("Failed to parse message envelope")?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref reqp) => {
            let json_properties =
                serde_json::to_value(reqp).context("Failed to serialize message properties")?;
            json_flatten("properties", &json_properties, &mut acc);
            adjust_request_properties(&mut acc);

            let json_payload = envelope
                .payload::<JsonValue>()
                .context("Failed to serialize message payload")?;
            // For any request: send only first level key/value pairs from the message payload.
            json_flatten_one_level_deep("payload", &json_payload, &mut acc);
            adjust_payload(&mut acc);

            let payload = serde_json::to_value(acc)?;
            try_send(&client, payload, topmind).await
        }
        compat::IncomingEnvelopeProperties::Response(ref resp) => {
            let json_properties =
                serde_json::to_value(resp).context("Failed to serialize message properties")?;
            json_flatten("properties", &json_properties, &mut acc);
            adjust_response_properties(&mut acc);

            let json_payload = envelope
                .payload::<JsonValue>()
                .context("Failed to serialize message payload")?;
            // For any response: send only first level key/value pairs from the message payload.
            json_flatten_one_level_deep("payload", &json_payload, &mut acc);
            adjust_payload(&mut acc);

            let payload = serde_json::to_value(acc)?;
            try_send(&client, payload, topmind).await
        }
        compat::IncomingEnvelopeProperties::Event(ref evp) => {
            let json_properties =
                serde_json::to_value(evp).context("Failed to serialize message properties")?;
            json_flatten("properties", &json_properties, &mut acc);
            adjust_event_properties(&mut acc);

            let json_payload = envelope
                .payload::<JsonValue>()
                .context("Failed to serialize message payload")?;

            let telemetry_topic = Subscription::multicast_requests_from(evp, Some(API_VERSION))
                .subscription_topic(agent_id, API_VERSION)
                .context("Error building telemetry subscription topic")?;
            // Telemetry only events: send entire payload.
            if topic == telemetry_topic {
                if let Some(json_payload_array) = json_payload.as_array() {
                    // Send multiple metrics.
                    for json_payload_object in json_payload_array {
                        let topmind = topmind.clone();
                        let mut acc2 = acc.clone();
                        json_flatten("payload", &json_payload_object, &mut acc2);
                        adjust_payload(&mut acc);

                        let payload = serde_json::to_value(acc2)
                            .context("Failed to serialize message payload")?;
                        try_send(&client, payload, topmind).await?
                    }
                } else {
                    // Send a single metric.
                    json_flatten("payload", &json_payload, &mut acc);
                    adjust_payload(&mut acc);

                    let payload =
                        serde_json::to_value(acc).context("Failed to serialize message payload")?;
                    try_send(&client, payload, topmind).await?
                }
            }
            // All the other events: send only first level key/value pairs from the message payload.
            else {
                json_flatten_one_level_deep("payload", &json_payload, &mut acc);
                adjust_payload(&mut acc);

                let payload =
                    serde_json::to_value(acc).context("Failed to serialize message payload")?;
                try_send(&client, payload, topmind).await?
            }

            Ok(())
        }
    }
}

async fn try_send(
    client: &HttpClient,
    payload: JsonValue,
    topmind: Arc<TopMindConfig>,
) -> Result<()> {
    let retry = topmind.retry.unwrap_or(MAX_ATTEMPTS);
    let mut errors = vec![];
    for _ in 0..retry {
        let payload = payload.clone();
        let topmind = topmind.clone();

        match send(client, payload, topmind).await {
            ok @ Ok(_) => return ok,
            Err(err) => errors.push(err.to_string()),
        }
    }

    errors.dedup();
    Err(Error::new(ErrorKind::Other, errors.join(", ")).into())
}

async fn send(client: &HttpClient, payload: JsonValue, topmind: Arc<TopMindConfig>) -> Result<()> {
    use isahc::prelude::*;

    let body = serde_json::to_string(&payload).context("Failed to build TopMind request")?;
    let req = Request::post(&topmind.uri)
        .header("authorization", format!("Bearer {}", topmind.token))
        .header("content-type", "application/json")
        // Must not be used with HTTP/2.
        .header("connection", "keep-alive")
        .header("user-agent", "telemetry")
        .body(body)?;

    let mut resp = client
        .send_async(req)
        .await
        .context("Error sending the TopMind request")?;
    let data = resp
        .text_async()
        .await
        .context("Invalid format of the TopMind response, received data isn't even a string")?;
    let object = serde_json::from_str::<TopMindResponse>(&data).with_context(|| {
        format!(
            "Invalid format of the TopMind response, received data = '{}'",
            data
        )
    })?;
    if let TopMindResponse::Error(data) = object {
        return Err(anyhow::Error::from(data).context("TopMind responded with the error"));
    }

    Ok(())
}

fn convert_ua_to_json(ua_str: &str) -> Result<HashMap<&'static str, JsonValue>, anyhow::Error> {
    let result = woothee::parser::Parser::new().parse(ua_str);

    result
        .as_ref()
        .ok_or_else(|| format_err!("Failed to parse user agent"))
        .and_then(|ua| {
            let mut ua_map: HashMap<&'static str, JsonValue> = HashMap::new();
            ua_map.insert("name", serde_json::to_value(ua.name)?);
            ua_map.insert("category", serde_json::to_value(ua.category)?);
            ua_map.insert("os", serde_json::to_value(ua.os)?);
            ua_map.insert("os_version", serde_json::to_value(ua.os_version.as_ref())?);
            ua_map.insert("browser_type", serde_json::to_value(ua.browser_type)?);
            ua_map.insert("version", serde_json::to_value(ua.version)?);
            ua_map.insert("vendor", serde_json::to_value(ua.vendor)?);

            Ok(ua_map)
        })
}

////////////////////////////////////////////////////////////////////////////////

mod config;
