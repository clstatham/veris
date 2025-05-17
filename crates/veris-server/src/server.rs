use sqlparser::{dialect::GenericDialect, parser::Parser};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use veris_db::{engine::debug::DebugEngine, exec::session::Session};
use veris_net::request::{Request, Response};

use crate::Config;

pub struct Server {
    config: Config,
}

impl Server {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        let sql_listener = TcpListener::bind(self.config.addr).await?;
        log::info!("Listening on {}", self.config.addr);

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                log::info!("Received Ctrl-C, shutting down");
            }

            res = Self::sql_accept(sql_listener) => {
                if let Err(e) = res {
                    log::error!("Error in SQL connection: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn sql_accept(listener: TcpListener) -> anyhow::Result<()> {
        loop {
            let (mut socket, _) = listener.accept().await?;
            log::info!("Accepted SQL connection from {}", socket.peer_addr()?);
            socket.set_nodelay(true)?;
            // Handle the connection in a separate task
            tokio::spawn(async move {
                if let Err(e) = Self::sql_session(&mut socket, Session::new(&DebugEngine)).await {
                    log::error!("Error in SQL session: {}", e);
                }
                socket.shutdown().await.ok();
            });
        }
    }

    async fn sql_session(
        socket: &mut TcpStream,
        mut session: Session<'_, DebugEngine>,
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
            log::info!("Received request: {:?}", req);

            let resp = Self::process_request(&mut session, &req).await;

            log::info!("Sending response: {:?}", resp);
            let resp = format!("{}\n", serde_json::to_string(&resp)?);
            tx.write_all(resp.as_bytes()).await?;
        }

        Ok(())
    }

    async fn process_request(
        session: &mut Session<'_, DebugEngine>,
        request: &Request,
    ) -> Response {
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

                for statement in &ast {
                    match session.exec(statement) {
                        Ok(()) => {}
                        Err(e) => {
                            log::error!("Failed to execute SQL: {}", e);
                            return Response::Error(e.to_string());
                        }
                    }
                }

                Response::Execute(())
            }
        }
    }
}
