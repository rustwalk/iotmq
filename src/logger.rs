use serde::Deserialize;

#[derive(Deserialize, Debug, Default, Clone)]
pub struct Log {
    pub level: String,
    pub format: String,
    pub dir: String,
    pub file: String,
}
