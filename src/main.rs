use anyhow::Result;

#[async_std::main]
async fn main() -> Result<()> {
    env_logger::init();

    app::run().await
}

mod app;
