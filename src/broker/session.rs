use super::Stream;
use crate::Context;
use crate::mqtt::Connect;
use anyhow::Result;

pub struct Session {
    pub ctx: Context,
    pub stream: Stream,
    pub connect: Connect,
}

impl Session {
    pub(crate) fn new(ctx: Context, stream: Stream, connect: Connect) -> Self {
        Self { ctx, stream, connect }
    }

    pub async fn run(self) -> Result<()> {
        loop {}
        Ok(())
    }
}
