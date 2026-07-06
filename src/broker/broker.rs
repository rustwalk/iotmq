use crate::Context;
use anyhow::Result;
use tracing::info;

pub struct Broker;

impl Broker {
    pub async fn run(ctx: Context) -> Result<()> {
        info!("Starting mqtt server");
        Ok(())
    }
}
