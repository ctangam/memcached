use std::time::Instant;

use anyhow::{Error, Result};
use dashmap::DashMap;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

#[derive(Debug, PartialEq)]
pub enum Req {
    Set(Set),
    Get(Get),
}

impl Req {

    pub fn from(data: &str) -> Result<Req, Error> {
        let name = data.split_once(" ").unwrap().0;
        println!("Name: [{}]", name);

        match name {
            "set" => Ok(Req::Set(Set::from(data)?)),
            "get" => Ok(Req::Get(Get::from(data)?)),
            _ => Err(Error::msg(format!("Invalid command {}", name))),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Set {
    pub name: String,
    pub key: String,
    pub flags: u16,
    pub exptime: u64,
    pub byte_count: usize,
    pub no_reply: bool,
    pub data_block: Vec<u8>,
}

impl Set {
    pub fn from(data: &str) -> Result<Set, Error> {
        let parts = data.split(" ").collect::<Vec<&str>>();
        println!("Command: {:?}", parts);
        if parts.len() < 5 {
            return Err(Error::msg(format!("Invalid set format {}", data)));
        }

        let name = parts[0].to_string();
        let key = parts[1].to_string();
        let flags = parts[2].parse::<u16>()?;
        let exptime = parts[3].parse::<u64>()?;
        let byte_count = parts[4].parse::<usize>()?;

        let (no_reply, data_block) = parts[5].split_once("\r\n").unwrap();

        let no_reply = no_reply != "";
        let data_block = data_block.as_bytes()[..byte_count].to_vec();

        Ok(Set {
            name,
            key,
            flags,
            exptime,
            byte_count,
            no_reply,
            data_block,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct Get {
    pub name: String,
    pub key: String,
}

impl Get {
    pub fn from(data: &str) -> Result<Get, Error> {
        let parts = data.split_whitespace().collect::<Vec<&str>>();

        let name = parts[0].to_string();
        let key = parts[1].to_string();

        Ok(Get {
            name,
            key,
        })
    }
}

pub enum Resp {
    Stored,
    End,
    Value(Value),
}

impl Resp {
    pub fn to_string(&self) -> String {
        match self {
            Resp::Stored => "STORED\r\n".to_string(),
            Resp::End => "END\r\n".to_string(),
            Resp::Value(value) => value.to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Entry {
    pub data: Value,
    pub expires_at: Option<Instant>
}

#[derive(Debug, PartialEq, Clone)]
pub struct Value {
    pub name: String,
    pub flags: u16,
    pub byte_count: usize,
    pub data_block: Vec<u8>,
}

impl Value {
    pub fn to_string(&self) -> String {
        format!("VALUE {} {} {}\r\n{}\r\n", self.name, self.flags, self.byte_count, String::from_utf8_lossy(&self.data_block))
    }
}

#[tokio::main]
async fn main() {
    let server = TcpListener::bind("127.0.0.1:11211").await.unwrap();
    let storage: DashMap<String, Entry> = DashMap::new();

    loop {
        let (socket, _) = server.accept().await.unwrap();
        let storage = storage.clone();
        tokio::spawn(async move {
            let (mut reader, mut writer) = socket.into_split();
            let mut buf = vec![0; 1024];

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => {
                        let data = String::from_utf8_lossy(&buf[..n]);
                        println!("Received:[{}]", data);
        
                        if let Err(_) = Req::from(&data) {
                            continue;
                        }

                        let command = Req::from(&data).unwrap();
        
                        let resp = match command {
                            Req::Set(set) => {
                                let value = Value {
                                    name: set.key.clone(),
                                    data_block: set.data_block.clone(),
                                    flags: set.flags,
                                    byte_count: set.byte_count,
                                };
                                let expires_at = if set.exptime == 0 {
                                    None
                                } else {
                                    Some(Instant::now() + std::time::Duration::from_secs(set.exptime))
                                };
                                let entry = Entry {
                                    data: value.clone(),
                                    expires_at
                                };
 
                                storage.insert(set.key, entry);

                                if !set.no_reply {
                                    Some(Resp::Stored)
                                } else {
                                    None
                                }
                            }
                            Req::Get(get) => {
                                if let Some(entry) = storage.get(&get.key) {
                                    Some(Resp::Value(entry.data.clone()))
                                } else {
                                    Some(Resp::End)
                                }
                            }
                        };

                        if let Some(resp) = resp {
                            writer.write_all(resp.to_string().as_bytes()).await.unwrap();
                        }
                    }
                    Err(_) => return,
                }



            }


        });
    }
}


#[test]
fn test_set() {
    let command = Set::from("set hello 0 0 5 \r\nhello\r\n").unwrap();

    assert_eq!(command.name, "set");
    assert_eq!(command.key, "hello");
    assert_eq!(command.flags, 0);
    assert_eq!(command.exptime, 0);
    assert_eq!(command.byte_count, 5);
    assert_eq!(command.no_reply, false);
    assert_eq!(command.data_block, "hello".as_bytes());
}

#[test]
fn test_empty_data_set() {
    let command = Set::from("set hello 0 0 0 \r\n\r\n").unwrap();

    assert_eq!(command.name, "set");
    assert_eq!(command.key, "hello");
    assert_eq!(command.flags, 0);
    assert_eq!(command.exptime, 0);
    assert_eq!(command.byte_count, 0);
    assert_eq!(command.no_reply, false);
    assert_eq!(command.data_block, "".as_bytes());
}


#[test]
fn test_noreply_set() {
    let command = Set::from("set hello 0 0 5 noreply\r\nhello\r\n").unwrap();

    assert_eq!(command.name, "set");
    assert_eq!(command.key, "hello");
    assert_eq!(command.flags, 0);
    assert_eq!(command.exptime, 0);
    assert_eq!(command.byte_count, 5);
    assert_eq!(command.no_reply, true);
    assert_eq!(command.data_block, "hello".as_bytes());
}

#[test]
fn test_noreply_empty_data_set() {
    let command = Set::from("set hello 0 0 0 noreply\r\n\r\n").unwrap();

    assert_eq!(command.name, "set");
    assert_eq!(command.key, "hello");
    assert_eq!(command.flags, 0);
    assert_eq!(command.exptime, 0);
    assert_eq!(command.byte_count, 0);
    assert_eq!(command.no_reply, true);
    assert_eq!(command.data_block, "".as_bytes());
}

#[test]
fn test_get() {
    let command = Get::from("get hello\r\n").unwrap();

    assert_eq!(command.name, "get");
    assert_eq!(command.key, "hello");
}

#[test]
fn test_req() {
    let command = Req::from("set hello 0 0 5 \r\nhello\r\n").unwrap();

    assert_eq!(command, Req::Set(Set::from("set hello 0 0 5 \r\nhello\r\n").unwrap()));

    let command = Req::from("get hello\r\n").unwrap();

    assert_eq!(command, Req::Get(Get::from("get hello\r\n").unwrap()));
}