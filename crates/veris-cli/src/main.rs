#![allow(clippy::derivable_impls, clippy::uninlined_format_args)]

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
};

use clap::Parser;
use clap_serde_derive::ClapSerde;
use client::Client;
use serde::{Deserialize, Serialize};
use std::io;

pub mod client;

#[derive(Debug, Clone, ClapSerde, Serialize, Deserialize)]
#[command(author, version, about)]
pub struct Config {
    #[arg(long)]
    #[default(PathBuf::from(".veris_history"))]
    repl_history: PathBuf,

    #[arg(long)]
    #[default(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234)))]
    addr: SocketAddr,
}

#[derive(Parser)]
pub struct Cli {
    #[arg(long, default_value = "veris-cli.toml")]
    config: PathBuf,

    #[command(flatten)]
    overrides: <Config as ClapSerde>::Opt,
}

fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_env("VERIS_LOG")
        .init();

    let mut cli = Cli::parse();
    let config = match std::fs::read_to_string(&cli.config) {
        Ok(s) => {
            let config: <Config as ClapSerde>::Opt = toml::from_str(&s)?;
            Config::from(config)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Config::default(),
        Err(e) => return Err(e.into()),
    };

    let config = config.merge(&mut cli.overrides);
    let client = Client::new(config);

    client.connect()?;

    Ok(())
}
