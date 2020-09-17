use serde_derive::Deserialize;
use svc_authn::jose::Algorithm;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub id: svc_agent::AccountId,
    pub id_token: IdTokenConfig,
    pub topmind: TopMindConfig,
    pub agent_label: String,
    pub broker_id: svc_agent::AccountId,
    pub mqtt: svc_agent::mqtt::AgentConfig,
    pub sentry: Option<svc_error::extension::sentry::Config>,
    pub http: Option<HttpConfig>,
}

pub fn load() -> Result<Config, config::ConfigError> {
    let mut parser = config::Config::default();
    parser.merge(config::File::with_name("App"))?;
    parser.merge(config::Environment::with_prefix("APP").separator("__"))?;
    parser.try_into::<Config>()
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub struct IdTokenConfig {
    #[serde(deserialize_with = "svc_authn::serde::algorithm")]
    pub algorithm: Algorithm,
    #[serde(deserialize_with = "svc_authn::serde::file")]
    pub key: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub struct TopMindConfig {
    pub uri: String,
    pub token: String,
    pub timeout: Option<u64>,
    pub retry: Option<u8>,
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    pub metrics_addr: std::net::SocketAddr,
}
