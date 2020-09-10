use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "status")]
#[serde(rename_all = "lowercase")]
pub enum TopMindResponse {
    Success(TopMindResponseSuccess),
    Error(TopMindResponseError),
}

#[derive(Debug, Deserialize)]
pub struct TopMindResponseSuccess {
    op_id: String,
}

#[derive(Debug, Deserialize)]
pub struct TopMindResponseError {
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
