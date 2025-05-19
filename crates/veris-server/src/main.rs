use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
};

use clap::Parser;
use clap_serde_derive::ClapSerde;
use serde::{Deserialize, Serialize};
use tokio::io;

pub mod server;

#[derive(Debug, ClapSerde, Serialize, Deserialize)]
#[command(author, version, about)]
pub struct Config {
    db_path: PathBuf,

    #[default(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234)))]
    addr: SocketAddr,
}

#[derive(Parser)]
pub struct Cli {
    #[arg(long, default_value = "veris.toml")]
    config: PathBuf,

    #[command(flatten)]
    overrides: <Config as ClapSerde>::Opt,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .filter_module("veris_server", log::LevelFilter::Debug)
        .parse_env("VERIS_LOG")
        .init();

    let mut cli = Cli::parse();
    let config = match tokio::fs::read_to_string(&cli.config).await {
        Ok(s) => {
            let config: <Config as ClapSerde>::Opt = toml::from_str(&s)?;
            Config::from(config)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Config::default(),
        Err(e) => return Err(e.into()),
    };

    let config = config.merge(&mut cli.overrides);

    let server = server::Server::new(config);

    server.serve().await?;

    Ok(())
}
