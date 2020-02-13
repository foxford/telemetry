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
use log::{error, info};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_agent::mqtt::{compat, AgentBuilder, ConnectionMode, Notification, QoS};
use svc_agent::{AgentId, Authenticable, SharedGroup, Subscription};
use svc_authn::{jose::Algorithm, token::jws_compact};

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

#[derive(Debug, Serialize)]
struct TopMindRequest {
    properties: JsonValue,
    payload: JsonValue,
}

impl TopMindRequest {
    fn new(properties: JsonValue, payload: JsonValue) -> Self {
        Self {
            properties,
            payload,
        }
    }
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

                    let result = handle_message(message.payload.clone(), topmind.clone()).await;
                    if let Err(err) = result {
                        error!(
                            "Error processing a message = '{text}' sent to the topic = '{topic}', {detail}",
                            text = String::from_utf8_lossy(message.payload.as_slice()),
                            topic = topic,
                            detail = err,
                        )
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

async fn handle_message(payload: Arc<Vec<u8>>, topmind: Arc<TopMindConfig>) -> Result<(), Error> {
    let envelope = serde_json::from_slice::<compat::IncomingEnvelope>(payload.as_slice())?;
    let payload = envelope.payload::<JsonValue>()?;
    match envelope.properties() {
        compat::IncomingEnvelopeProperties::Request(ref reqp) => {
            let props = serde_json::to_value(reqp)?;
            send(TopMindRequest::new(props, payload), topmind).await
        }
        compat::IncomingEnvelopeProperties::Response(ref resp) => {
            let props = serde_json::to_value(resp)?;
            send(TopMindRequest::new(props, payload), topmind).await
        }
        compat::IncomingEnvelopeProperties::Event(ref evp) => {
            let props = serde_json::to_value(evp)?;
            send(TopMindRequest::new(props, payload), topmind).await
        }
    }
}

async fn send(payload: TopMindRequest, topmind: Arc<TopMindConfig>) -> Result<(), Error> {
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
