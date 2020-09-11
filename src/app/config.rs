use serde_derive::Deserialize;
use svc_authn::jose::Algorithm;

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) id: svc_agent::AccountId,
    pub(crate) id_token: IdTokenConfig,
    pub(crate) topmind: TopMindConfig,
    pub(crate) agent_label: String,
    pub(crate) broker_id: svc_agent::AccountId,
    pub(crate) mqtt: svc_agent::mqtt::AgentConfig,
    pub(crate) sentry: Option<svc_error::extension::sentry::Config>,
    pub(crate) metrics_addr: Option<std::net::SocketAddr>,
}

pub(crate) fn load() -> Result<Config, config::ConfigError> {
    let mut parser = config::Config::default();
    parser.merge(config::File::with_name("App"))?;
    parser.merge(config::Environment::with_prefix("APP").separator("__"))?;
    parser.try_into::<Config>()
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct IdTokenConfig {
    #[serde(deserialize_with = "svc_authn::serde::algorithm")]
    pub algorithm: Algorithm,
    #[serde(deserialize_with = "svc_authn::serde::file")]
    pub key: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TopMindConfig {
    pub uri: String,
    pub token: String,
    pub timeout: Option<u64>,
    pub retry: Option<u8>,
}
