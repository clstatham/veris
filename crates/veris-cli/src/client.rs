use std::{
    io::{self, BufRead, BufReader, Write},
    net::TcpStream,
};

use rustyline::{Editor, error::ReadlineError, history::FileHistory};
use sqlparser::parser::ParserError;
use thiserror::Error;
use veris_net::request::{Request, Response};

use crate::Config;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    SqlParser(#[from] ParserError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

pub struct Client {
    config: Config,
}

impl Client {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn connect(&self) -> anyhow::Result<()> {
        let socket = loop {
            match TcpStream::connect_timeout(&self.config.addr, std::time::Duration::from_secs(5)) {
                Ok(socket) => break socket,
                Err(e) => {
                    log::warn!("Failed to connect to server: {e}");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        };
        socket.set_nodelay(true)?;
        log::info!("Connected to server at {}", self.config.addr);

        self.launch_repl(socket)?;

        Ok(())
    }

    fn launch_repl(&self, mut socket: TcpStream) -> anyhow::Result<()> {
        let mut rl = Editor::<(), FileHistory>::new()?;
        rl.load_history(&self.config.repl_history).ok();

        let mut rx = BufReader::new(socket.try_clone()?);

        'repl: loop {
            let readline = rl.readline(">>> ");
            match readline {
                Ok(line) => {
                    let line = line.trim();
                    rl.add_history_entry(line)?;

                    if let Err(e) = self.handle_line(line, &mut socket, &mut rx) {
                        if let ClientError::Serialization(e) = &e {
                            if let Some(kind) = e.io_error_kind() {
                                if matches!(
                                    kind,
                                    io::ErrorKind::UnexpectedEof
                                        | io::ErrorKind::ConnectionReset
                                        | io::ErrorKind::ConnectionAborted
                                        | io::ErrorKind::BrokenPipe
                                ) {
                                    log::warn!("Server closed connection");
                                    break 'repl;
                                }
                            }
                        }
                        log::error!("Error: {e}");
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    log::warn!("Interrupted")
                }
                Err(ReadlineError::Eof) => {
                    break 'repl;
                }
                Err(e) => {
                    log::error!("Error: {e}");
                    break 'repl;
                }
            }
        }
        rl.save_history(&self.config.repl_history)?;

        socket.shutdown(std::net::Shutdown::Both).ok();

        Ok(())
    }

    pub fn handle_line(
        &self,
        line: &str,
        tx: &mut impl Write,
        rx: &mut BufReader<TcpStream>,
    ) -> Result<(), ClientError> {
        let req = if line.split_whitespace().next() == Some("?") {
            let line: String = line.split_whitespace().skip(1).collect();
            Request::Debug(line.lines().collect())
        } else {
            Request::Execute(line.lines().collect())
        };
        let req = serde_json::to_string(&req)?;
        writeln!(tx, "{}", req)?;

        let mut resp = String::new();
        rx.read_line(&mut resp)?;
        let resp: Response = serde_json::from_str(&resp)?;
        match resp {
            Response::Execute(()) => {}
            Response::Debug(result) => {
                println!("{}", result);
            }
            Response::Error(error) => {
                log::error!("Error: {}", error);
            }
        }

        Ok(())
    }
}
