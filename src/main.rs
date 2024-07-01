use std::{io::Read, time::Instant};

use anyhow::{Error, Result};
use dashmap::DashMap;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

#[derive(Debug, PartialEq)]
pub enum Req {
    Set(Common),
    Get(Get),
    Add(Common),
    Replace(Common),
    Append(Common),
    Prepend(Common),
}

impl Req {

    pub fn from(data: &str) -> Result<Req, Error> {
        let name = data.split_once(" ").unwrap().0;
        println!("Name: [{}]", name);

        match name {
            "set" => Ok(Req::Set(Common::from(data)?)),
            "get" => Ok(Req::Get(Get::from(data)?)),
            "add" => Ok(Req::Add(Common::from(data)?)),
            "replace" => Ok(Req::Replace(Common::from(data)?)),
            _ => Err(Error::msg(format!("Invalid command {}", name))),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Common {
    pub name: String,
    pub key: String,
    pub flags: u16,
    pub exptime: u64,
    pub byte_count: usize,
    pub no_reply: bool,
    pub data_block: Vec<u8>,
}

impl Common {
    pub fn from(data: &str) -> Result<Common, Error> {
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

        Ok(Common {
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
    NotStored,
    End,
    Value(Value),
}

impl Resp {
    pub fn to_string(&self) -> String {
        match self {
            Resp::Stored => "STORED\r\n".to_string(),
            Resp::NotStored => "NOT_STORED\r\n".to_string(),
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
                            Req::Set(common) => store(&storage, common),
                            Req::Get(get) => {
                                if let Some(entry) = storage.get(&get.key) {
                                    if entry.expires_at.is_some_and(|e| e <= Instant::now()) {
                                        storage.remove(&get.key);
                                        Some(Resp::End)
                                    } else {
                                        Some(Resp::Value(entry.data.clone()))
                                    }
                                } else {
                                    Some(Resp::End)
                                }
                            },
                            Req::Add(common) => {
                                if let None = storage.get(&common.key) {
                                    store(&storage, common)
                                } else {
                                    Some(Resp::NotStored)
                                }
                            },
                            Req::Replace(common) => {
                                if storage.contains_key(&common.key) {
                                    store(&storage, common)
                                } else {
                                    Some(Resp::NotStored)
                                }
                            },
                            Req::Append(mut common) => {
                                if let Some(entry) = storage.get(&common.key) {
                                    let mut bytes = Vec::new();
                                    bytes.extend_from_slice(&entry.data.data_block);
                                    bytes.extend_from_slice(&common.data_block);
                                    common.data_block = bytes;

                                    store(&storage, common)
                                } else {
                                    Some(Resp::NotStored)
                                }
                            },
                            Req::Prepend(mut common) => {
                                if let Some(entry) = storage.get(&common.key) {
                                    let mut bytes = Vec::new();
                                    bytes.extend_from_slice(&common.data_block);
                                    bytes.extend_from_slice(&entry.data.data_block);
                                    common.data_block = bytes;
                                    
                                    store(&storage, common)
                                } else {
                                    Some(Resp::NotStored)
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

fn store(storage: &DashMap<String, Entry>, common: Common) -> Option<Resp> {
    let value = Value {
        name: common.key.clone(),
        data_block: common.data_block.clone(),
        flags: common.flags,
        byte_count: common.byte_count,
    };
    let expires_at = if common.exptime == 0 {
        None
    } else {
        Some(Instant::now() + std::time::Duration::from_secs(common.exptime))
    };
    let entry = Entry {
        data: value.clone(),
        expires_at
    };
    if common.exptime > 0 {
        storage.insert(common.key, entry);
    }

    if !common.no_reply {
        Some(Resp::Stored)
    } else {
        None
    }
}

#[test]
fn test_set() {
    let command = Common::from("set hello 0 0 5 \r\nhello\r\n").unwrap();

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
    let command = Common::from("set hello 0 0 0 \r\n\r\n").unwrap();

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
    let command = Common::from("set hello 0 0 5 noreply\r\nhello\r\n").unwrap();

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
    let command = Common::from("set hello 0 0 0 noreply\r\n\r\n").unwrap();

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

    assert_eq!(command, Req::Set(Common::from("set hello 0 0 5 \r\nhello\r\n").unwrap()));

    let command = Req::from("get hello\r\n").unwrap();

    assert_eq!(command, Req::Get(Get::from("get hello\r\n").unwrap()));
}