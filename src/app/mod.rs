use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;

use failure::{err_msg, format_err, Error};
use futures::{
    executor::ThreadPoolBuilder,
    future::{self, Either},
    task::SpawnExt,
    StreamExt,
};
use futures_timer::Delay;
use log::{error, info, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{compat, AgentBuilder, ConnectionMode, Notification, QoS, ResponseStatus};
use svc_agent::{AccountId, AgentId, Authenticable, SharedGroup, Subscription};
use svc_authn::{jose::Algorithm, token::jws_compact};
use svc_error::extension::sentry;

pub(crate) const API_VERSION: &str = "v1";

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

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
enum MessagingPattern {
    Broadcast(BroadcastMessagingPattern),
    Multicast(MulticastMessagingPattern),
    Unicast(UnicastMessagingPattern),
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
struct MulticastMessagingPattern {
    from: AgentId,
    to: AccountAddress,
}

impl MulticastMessagingPattern {
    fn new(from: AgentId, to: AccountAddress) -> Self {
        Self { from, to }
    }
}

#[derive(Debug, Serialize)]
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
    type Err = Error;

    fn from_str(topic: &str) -> Result<Self, Self::Err> {
        if topic.starts_with("apps") {
            let arr = topic.splitn(5, '/').collect::<Vec<&str>>();
            match &arr[..] {
                ["apps", from_account_id, "api", ref from_version, ref path] => {
                    let from_account_id = from_account_id.parse().map_err(|err| {
                        format_err!("error deserializing account_id from a string, {}", &err)
                    })?;
                    let address = AccountAddress::new(from_account_id, from_version);
                    let pattern =
                        MessagingPattern::Broadcast(BroadcastMessagingPattern::new(address, path));
                    Ok(pattern)
                }
                _ => Err(format_err!(
                    "invalid value for the messaging pattern: {:?}",
                    topic
                )),
            }
        } else {
            let arr = topic.splitn(6, '/').collect::<Vec<&str>>();
            match &arr[..] {
                ["agents", ref from_agent_id, "api", ref to_version, "out", ref to_account_id] => {
                    let from_agent_id = from_agent_id.parse().map_err(|err| {
                        format_err!("error deserializing account_id from a string, {}", &err)
                    })?;
                    let to_account_id = to_account_id.parse().map_err(|err| {
                        format_err!("error deserializing account_id from a string, {}", &err)
                    })?;
                    let address = AccountAddress::new(to_account_id, to_version);
                    let pattern = MessagingPattern::Multicast(MulticastMessagingPattern::new(
                        from_agent_id,
                        address,
                    ));
                    Ok(pattern)
                }
                ["agents", ref to_agent_id, "api", ref to_version, "in", ref from_account_id] => {
                    let from_account_id = from_account_id.parse().map_err(|err| {
                        format_err!("error deserializing account_id from a string, {}", &err)
                    })?;
                    let to_agent_id = to_agent_id.parse().map_err(|err| {
                        format_err!("error deserializing agent_id from a string, {}", &err)
                    })?;
                    let address = AgentAddress::new(to_agent_id, to_version);
                    let pattern = MessagingPattern::Unicast(UnicastMessagingPattern::new(
                        from_account_id,
                        address,
                    ));
                    Ok(pattern)
                }
                _ => Err(format_err!(
                    "invalid value for the messaging pattern: {:?}",
                    topic
                )),
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

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn run() -> Result<(), Error> {
    // Config
    let config = config::load().map_err(|err| format_err!("Failed to load config: {}", err))?;
    info!("App config: {:?}", config);

    // Agent
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!("Agent id: {:?}", &agent_id);

    let token = jws_compact::TokenBuilder::new()
        .issuer(&agent_id.as_account_id().audience().to_string()) //?
        .subject(&agent_id)
        .key(config.id_token.algorithm, config.id_token.key.as_slice())
        .build()
        .map_err(|err| format_err!("Error creating an id token: {}", err))?;

    let mut agent_config = config.mqtt.clone();
    agent_config.set_password(&token);

    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());
    let (mut agent, rx) = AgentBuilder::new(agent_id.clone(), API_VERSION)
        .connection_mode(ConnectionMode::Observer)
        .start(&agent_config)
        .map_err(|err| format_err!("Failed to create an agent: {}", err))?;

    // Message loop for incoming messages of MQTT Agent
    let (mq_tx, mut mq_rx) = futures_channel::mpsc::unbounded::<Notification>();

    thread::spawn(move || {
        for message in rx {
            if let Err(_) = mq_tx.unbounded_send(message) {
                error!("Error sending message to the internal channel");
            }
        }
    });

    // Sentry
    if let Some(sentry_config) = config.sentry.as_ref() {
        svc_error::extension::sentry::init(sentry_config);
    }

    // Subscription
    agent
        .subscribe(
            &Subscription::multicast_requests(Some(API_VERSION)),
            QoS::AtMostOnce,
            Some(&group),
        )
        .expect("Error subscribing to everyone's output messages");

    // Thread Pool
    let thread_pool = ThreadPoolBuilder::new().create()?;

    let topmind = Arc::new(config.topmind);
    while let Some(message) = mq_rx.next().await {
        let topmind = topmind.clone();
        thread_pool.spawn(async move {
            match message {
                svc_agent::mqtt::Notification::Publish(message) => {
                    let topic: &str = &message.topic_name;

                    let result = handle_message(topic, message.payload.clone(), topmind.clone()).await;
                    if let Err(err) = result {
                        error!(
                            "Error processing a message = '{text}' sent to the topic = '{topic}', {detail}",
                            text = String::from_utf8_lossy(message.payload.as_slice()),
                            topic = topic,
                            detail = err,
                        );

                        // Send to the Sentry
                        let svc_error = svc_error::Error::builder()
                            .kind("topmind.wapi", "Error publishing a message to the TopMind W API")
                            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                            .detail(&err.to_string())
                            .build();

                        sentry::send(svc_error)
                            .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
                    }

                    // Log incoming messages
                    info!(
                        "Incoming message = '{}' sent to the topic = '{}', dup = '{}', pkid = '{:?}'",
                        String::from_utf8_lossy(message.payload.as_slice()), topic, message.dup, message.pkid,
                    );
                }
                _ => error!("An unsupported type of message = '{:?}'", message),
            }

        }).unwrap();
    }

    Ok(())
}

async fn handle_message(
    topic: &str,
    payload: Arc<Vec<u8>>,
    topmind: Arc<TopMindConfig>,
) -> Result<(), Error> {
    let mut acc: HashMap<String, JsonValue> = HashMap::new();

    let pattern = topic.parse::<MessagingPattern>()?;
    json_flatten("pattern", &serde_json::to_value(pattern)?, &mut acc);

    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(payload.as_slice())?;
    json_flatten("payload", &envelope.payload::<JsonValue>()?, &mut acc);

    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref reqp) => {
            json_flatten("properties", &serde_json::to_value(reqp)?, &mut acc);
            let payload = serde_json::to_value(acc)?;
            send(payload, topmind).await
        }
        compat::IncomingEnvelopeProperties::Response(ref resp) => {
            json_flatten("properties", &serde_json::to_value(resp)?, &mut acc);
            let payload = serde_json::to_value(acc)?;
            send(payload, topmind).await
        }
        compat::IncomingEnvelopeProperties::Event(ref evp) => {
            json_flatten("properties", &serde_json::to_value(evp)?, &mut acc);
            let payload = serde_json::to_value(acc)?;
            send(payload, topmind).await
        }
    }
}

async fn send(payload: JsonValue, topmind: Arc<TopMindConfig>) -> Result<(), Error> {
    let timeout = std::time::Duration::from_secs(topmind.timeout.unwrap_or(5));
    let request = surf::post(&topmind.uri)
        .set_header("authorization", format!("Bearer {}", topmind.token))
        .body_json(&payload);

    match request {
        Ok(req) => match future::select(req, Delay::new(timeout)).await {
            Either::Left((Ok(mut resp), _)) => match resp.body_json::<TopMindResponse>().await {
                Ok(TopMindResponse::Success(data)) => {
                    info!("reponse = {:#?}", &data);
                    Ok(())
                }
                Ok(TopMindResponse::Error(data)) => Err(format_err!(
                    "TopMind responded with the error, reason = {}",
                    &data.reason_phrase
                )),
                Err(_) => Err(err_msg("invalid format of the TopMind response")),
            },
            Either::Left((Err(err), _)) => {
                Err(format_err!("error sending the TopMind request, {}", &err))
            }
            Either::Right((_, _)) => Err(err_msg("timed out sending the TopMind request")),
        },
        Err(err) => Err(format_err!("failed to build TopMind request, {}", &err)),
    }
}

////////////////////////////////////////////////////////////////////////////////

mod config;
