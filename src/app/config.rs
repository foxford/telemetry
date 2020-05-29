use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) id: svc_agent::AccountId,
    pub(crate) id_token: crate::app::IdTokenConfig,
    pub(crate) topmind: crate::app::TopMindConfig,
    pub(crate) agent_label: String,
    pub(crate) broker_id: svc_agent::AccountId,
    pub(crate) mqtt: svc_agent::mqtt::AgentConfig,
    pub(crate) sentry: Option<svc_error::extension::sentry::Config>,
}

pub(crate) fn load() -> Result<Config, config::ConfigError> {
    let mut parser = config::Config::default();
    parser.merge(config::File::with_name("App"))?;
    parser.merge(config::Environment::with_prefix("APP").separator("__"))?;
    parser.try_into::<Config>()
}
