//! Command-line version of HSFT, also handy for testing and measuring speed.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use hsft_core::{Event, FriendInfo, Network, Node};

const USAGE: &str = "\
Usage:
  hsft-cli id                     show your id
  hsft-cli invite                 create an invite code and wait for a friend to use it
  hsft-cli join <code>            become friends using a friend's invite code
  hsft-cli friends                list friends
  hsft-cli send <friend> <path>...  send files or folders to a friend
  hsft-cli receive <folder>       accept everything friends send, into a folder
  hsft-cli bench <path>           measure speed by sending to a second peer on this PC
";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_env("HSFT_LOG"))
        .with_writer(std::io::stderr)
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["bench", path] => bench(Path::new(path)).await,
        ["id"] => {
            let (node, _) = start().await?;
            println!("{}", node.id());
            node.shutdown().await;
            Ok(())
        }
        ["friends"] => {
            let (node, _) = start().await?;
            for friend in node.friends() {
                println!("{}  {}", friend.name, friend.id);
            }
            node.shutdown().await;
            Ok(())
        }
        ["invite"] => invite().await,
        ["join", code] => {
            let (node, _) = start().await?;
            let joined = node.join(code).await;
            node.shutdown().await;
            println!("You and {} are now friends.", joined?.name);
            Ok(())
        }
        ["receive", dest] => receive(PathBuf::from(dest)).await,
        ["send", friend, paths @ ..] if !paths.is_empty() => {
            send(friend, paths.iter().map(PathBuf::from).collect()).await
        }
        _ => {
            print!("{USAGE}");
            Ok(())
        }
    }
}

fn data_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("HSFT_DATA") {
        return Ok(dir.into());
    }
    Ok(dirs::data_dir().context("no data folder")?.join("HSFT"))
}

async fn start() -> Result<(Node, tokio::sync::mpsc::UnboundedReceiver<Event>)> {
    let network = match std::env::var_os("HSFT_RELAY_ONLY") {
        Some(_) => Network::RelayOnly,
        None => Network::Internet,
    };
    Node::start(&data_dir()?, network).await
}

async fn invite() -> Result<()> {
    let (node, mut events) = start().await?;
    println!("{}", node.create_invite().await?);
    eprintln!("Waiting for your friend to use this code...");
    while let Some(event) = events.recv().await {
        if let Event::FriendAdded { friend } = event {
            println!("You and {} are now friends.", friend.name);
            break;
        }
    }
    node.shutdown().await;
    Ok(())
}

fn find_friend(node: &Node, query: &str) -> Result<FriendInfo> {
    let query = query.to_lowercase();
    let matches: Vec<_> = node
        .friends()
        .into_iter()
        .filter(|f| f.name.to_lowercase() == query || f.id.starts_with(&query))
        .collect();
    match <[FriendInfo; 1]>::try_from(matches) {
        Ok([friend]) => Ok(friend),
        Err(matches) if matches.is_empty() => bail!("no friend called {query}"),
        Err(_) => bail!("more than one friend matches {query}"),
    }
}

async fn send(friend: &str, paths: Vec<PathBuf>) -> Result<()> {
    let (node, mut events) = start().await?;
    let friend = find_friend(&node, friend)?;
    node.send(&friend.id, paths)?;
    let mut failure = None;
    while let Some(event) = events.recv().await {
        match event {
            Event::Progress { .. } => print_progress(&event),
            Event::Finished { .. } => {
                println!("\nDelivered to {}.", friend.name);
                break;
            }
            Event::Failed { error, .. } => {
                failure = Some(error);
                break;
            }
            _ => {}
        }
    }
    node.shutdown().await;
    failure.map_or(Ok(()), |error| bail!("{error}"))
}

async fn receive(dest: PathBuf) -> Result<()> {
    let (node, mut events) = start().await?;
    println!("Waiting for friends to send something...");
    while let Some(event) = events.recv().await {
        match event {
            Event::Offer { transfer, from_name, title, bytes, .. } => {
                println!("{from_name} is sending {title} ({})", size(bytes));
                node.respond(transfer, Some(dest.clone()));
            }
            Event::Progress { .. } => print_progress(&event),
            Event::Finished { saved_to, .. } => {
                println!("\nSaved to {}", saved_to.unwrap_or_default());
            }
            Event::Failed { error, .. } => println!("\nFailed: {error}"),
            _ => {}
        }
    }
    Ok(())
}

fn print_progress(event: &Event) {
    if let Event::Progress { done, total, bytes_per_sec, route, .. } = event {
        print!("\r{} of {}  {}/s  ({route})     ", size(*done), size(*total), size(*bytes_per_sec));
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Sends through a real connection between two peers in this process. Disk and CPU are the limit
/// here, so this measures what the software can do, not a real internet route.
async fn bench(path: &Path) -> Result<()> {
    let temp = std::env::temp_dir().join(format!("hsft-bench-{}", std::process::id()));
    let (a_dir, b_dir, dest) = (temp.join("a"), temp.join("b"), temp.join("received"));
    let (a, mut a_events) = Node::start(&a_dir, Network::LocalOnly).await?;
    let (b, mut b_events) = Node::start(&b_dir, Network::LocalOnly).await?;
    b.join(&a.create_invite().await?).await?;

    let started = Instant::now();
    b.send(&a.id(), vec![path.to_path_buf()])?;
    let mut bytes = 0;
    let mut outcome = None;
    while outcome.is_none() {
        tokio::select! {
            Some(event) = a_events.recv() => match event {
                Event::Offer { transfer, bytes: total, .. } => {
                    bytes = total;
                    a.respond(transfer, Some(dest.clone()));
                }
                Event::Finished { .. } => outcome = Some(Ok(())),
                Event::Failed { error, .. } => outcome = Some(Err(error)),
                _ => {}
            },
            Some(event) = b_events.recv() => {
                if let Event::Failed { error, .. } = event {
                    outcome = Some(Err(error));
                }
            }
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    a.shutdown().await;
    b.shutdown().await;
    let _ = std::fs::remove_dir_all(&temp);
    outcome.unwrap().map_err(anyhow::Error::msg)?;
    println!(
        "{} in {seconds:.2}s = {}/s",
        size(bytes),
        size((bytes as f64 / seconds) as u64)
    );
    Ok(())
}
