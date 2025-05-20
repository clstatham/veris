use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::TcpStream,
};

use ascii_table::{Align, AsciiTable};
use rustyline::{Editor, error::ReadlineError, history::FileHistory};
use sqlparser::parser::ParserError;
use thiserror::Error;
use veris_db::exec::session::StatementResult;
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

#[derive(Debug)]
pub enum ControlFlow {
    Exit,
    Continue,
    Response(Response),
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

        println!("Type .q or press Ctrl-D to exit.");

        'repl: loop {
            let readline = rl.readline(">>> ");
            match readline {
                Ok(line) => {
                    let line = line.trim();
                    rl.add_history_entry(line)?;

                    match self.handle_line(line, &mut socket, &mut rx) {
                        Ok(cf) => match cf {
                            ControlFlow::Exit => {
                                log::info!("Exiting REPL");
                                break 'repl;
                            }
                            ControlFlow::Continue => {}
                            ControlFlow::Response(resp) => self.handle_response(resp)?,
                        },
                        Err(e) => {
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
                }
                Err(ReadlineError::Interrupted) => {
                    log::warn!("Interrupted")
                }
                Err(ReadlineError::Eof) => {
                    println!("Exiting REPL");
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
    ) -> Result<ControlFlow, ClientError> {
        let Some(first) = line.split_whitespace().next() else {
            return Ok(ControlFlow::Continue); // empty line
        };
        let req = match first {
            ".q" => return Ok(ControlFlow::Exit),
            ".x" => self.load_sql(line[3..].trim())?,
            ".?" => Request::Debug(line[3..].trim().to_string()),
            _ => Request::Execute(line.to_string()),
        };
        let req = serde_json::to_string(&req)?;
        writeln!(tx, "{}", req)?;

        let mut resp = String::new();
        rx.read_line(&mut resp)?;
        let resp: Response = serde_json::from_str(&resp)?;

        Ok(ControlFlow::Response(resp))
    }

    pub fn load_sql(&self, path: &str) -> Result<Request, ClientError> {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut sql = String::new();
        reader.read_to_string(&mut sql)?;
        Ok(Request::Execute(sql))
    }

    pub fn handle_response(&self, resp: Response) -> Result<(), ClientError> {
        match resp {
            Response::Execute(resps) => {
                for resp in resps {
                    match resp {
                        StatementResult::Error(e) => {
                            println!("Error: {e}");
                        }
                        StatementResult::ShowTables { tables } => {
                            for table in tables {
                                let mut ascii_table = AsciiTable::default();
                                let mut data = Vec::new();
                                for (i, column) in table.columns.iter().enumerate() {
                                    ascii_table
                                        .column(i)
                                        .set_header(&*column.name)
                                        .set_align(Align::Right);
                                    data.push(format!("{}", &column.data_type));
                                }
                                println!("Table: {}", table.name);
                                ascii_table.print(vec![data]);
                            }
                        }
                        StatementResult::Select { rows, columns } => {
                            let mut ascii_table = AsciiTable::default();
                            for (i, column) in columns.iter().enumerate() {
                                ascii_table
                                    .column(i)
                                    .set_header(column.to_string())
                                    .set_align(Align::Right);
                            }
                            let mut data = Vec::new();
                            for row in rows {
                                let mut inner = Vec::new();
                                for item in row {
                                    inner.push(item);
                                }
                                data.push(inner);
                            }
                            ascii_table.print(data);
                        }
                        StatementResult::Insert(count) => {
                            println!("Inserted {count} rows");
                        }
                        StatementResult::Delete(count) => {
                            println!("Deleted {count} rows");
                        }
                        StatementResult::Begin => {
                            println!("Transaction started");
                        }
                        StatementResult::Commit => {
                            println!("Transaction committed");
                        }
                        StatementResult::Rollback => {
                            println!("Transaction rolled back");
                        }
                        StatementResult::CreateTable(name) => {
                            println!("Created table {name}");
                        }
                        StatementResult::DropTable(name) => {
                            println!("Dropped table {name}");
                        }
                        StatementResult::Null => {}
                    }
                }
            }
            Response::Error(resp) => {
                println!("Error: {resp}")
            }
            Response::Debug(resp) => {
                println!("{resp}")
            }
        }
        Ok(())
    }
}
