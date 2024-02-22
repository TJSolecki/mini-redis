use bytes::Bytes;
use mini_redis::client;
use regex::Regex;
use std::io::{self, BufRead};
use tokio::sync::{mpsc, oneshot};

/// Multiple different commands are multiplexed over a single channel.
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}

/// Provided by the requester and used by the manager task to send the command
/// response back to the requester.
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(32);

    let manager = tokio::spawn(async move {
        // Open a connection to the mini-redis address.
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key, resp } => {
                    let res = client.get(&key).await;
                    // Ignore errors
                    let _ = resp.send(res);
                }
                Command::Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    // Ignore errors
                    let _ = resp.send(res);
                }
            }
        }
    });
    let set_regex = Regex::new(r"^set\s+(?<key>\w+)\s+(?<value>\w+)$").unwrap();
    let get_regex = Regex::new(r"^get\s+(?<key>\w+)$").unwrap();

    // Create a new stdin reader
    let stdin = io::stdin();
    let handle = stdin.lock();

    for line in handle.lines() {
        match line {
            Ok(line) => {
                if let Some(set_match) = set_regex.captures(&line.to_lowercase()) {
                    let key = set_match["key"].to_string();
                    let value = set_match["value"].to_string();
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let cmd = Command::Set {
                        key,
                        val: value.into(),
                        resp: resp_tx,
                    };

                    if tx.send(cmd).await.is_err() {
                        eprintln!("connection with server failed");
                        return;
                    }

                    let res = resp_rx.await;
                    println!("From server: {:?}", res);
                } else if let Some(get_match) = get_regex.captures(&line.to_lowercase()) {
                    let key = get_match["key"].to_string();
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let cmd = Command::Get { key, resp: resp_tx };

                    if tx.send(cmd).await.is_err() {
                        eprintln!("connection with server failed");
                        return;
                    }

                    let res = resp_rx.await;
                    println!("From server: {:?}", res);
                } else {
                    println!("Error parsing command, please try again");
                }
            }
            Err(_) => break,
        };
    }

    manager.await.unwrap();
}
