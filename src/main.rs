use std::io::{Error, ErrorKind};

#[async_std::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    app::run()
        .await
        .map_err(|err| Error::new(ErrorKind::Other, err))
}

mod app;
