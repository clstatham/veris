use sqlparser::{dialect::GenericDialect, parser::Parser};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use veris_db::{
    engine::local::Local,
    exec::session::{Session, StatementResult},
    storage::bitcask::Bitcask,
};
use veris_net::request::{Request, Response};

use crate::Config;

pub type Engine = Bitcask;

pub struct Server {
    config: Config,
    engine: Local<Engine>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            engine: Local::new(Engine::new("./data/db1").unwrap()),
        }
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        let sql_listener = TcpListener::bind(self.config.addr).await?;
        log::info!("Listening on {}", self.config.addr);

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                log::info!("Received Ctrl-C, shutting down");
            }

            res = Self::sql_accept(sql_listener, &self.engine) => {
                if let Err(e) = res {
                    log::error!("Error in SQL connection: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn sql_accept(listener: TcpListener, engine: &Local<Engine>) -> anyhow::Result<()> {
        loop {
            let (mut socket, _) = listener.accept().await?;
            log::info!("Accepted SQL connection from {}", socket.peer_addr()?);
            socket.set_nodelay(true)?;

            if let Err(e) = Self::sql_session(&mut socket, Session::new(engine)).await {
                log::error!("Error in SQL session: {}", e);
            }
            log::info!("Closing SQL connection to {}", socket.peer_addr().unwrap());
            socket.shutdown().await.ok();
        }
    }

    async fn sql_session(
        socket: &mut TcpStream,
        mut session: Session<'_, Local<Engine>>,
    ) -> anyhow::Result<()> {
        let (rx, mut tx) = socket.split();
        let rx = BufReader::new(rx);

        let mut lines = rx.lines();

        while let Some(line) = lines.next_line().await? {
            log::info!("Got line: {line}");
            let req = match serde_json::from_str(&line) {
                Ok(req) => req,
                Err(e) => {
                    log::error!("Failed to deserialize request: {}", e);
                    continue;
                }
            };

            let resp = Self::process_request(&mut session, &req);

            let resp = format!("{}\n", serde_json::to_string(&resp)?);
            tx.write_all(resp.as_bytes()).await?;
        }

        Ok(())
    }

    fn process_request(session: &mut Session<'_, Local<Engine>>, request: &Request) -> Response {
        match request {
            Request::Debug(sql) => {
                let ast = match Parser::parse_sql(&GenericDialect {}, sql) {
                    Ok(ast) => ast,
                    Err(e) => {
                        log::error!("Failed to parse SQL: {}", e);
                        return Response::Error(e.to_string());
                    }
                };
                Response::Debug(format!("{ast:#?}"))
            }
            Request::Execute(sql) => {
                let ast = match Parser::parse_sql(&GenericDialect {}, sql) {
                    Ok(ast) => ast,
                    Err(e) => {
                        log::error!("Failed to parse SQL: {}", e);
                        return Response::Error(e.to_string());
                    }
                };

                let mut result = StatementResult::Null;
                for statement in &ast {
                    match session.exec(statement) {
                        Ok(val) => {
                            result = val;
                        }
                        Err(e) => {
                            log::error!("Failed to execute SQL: {}", e);
                            return Response::Error(e.to_string());
                        }
                    }
                }

                Response::Execute(result)
            }
        }
    }
}
