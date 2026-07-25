use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use rns_identity::identity::Identity;
use rns_runtime::lifecycle::ShutdownSignal;
use rs_rrc_client::{Event, MessageKind, RrcClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    let destination = parse_hash(
        &arguments
            .next()
            .ok_or("usage: bot <hub hash> <rns config dir> <room>")?,
    )?;
    let config = arguments
        .next()
        .ok_or("usage: bot <hub hash> <rns config dir> <room>")?;
    let room = arguments
        .next()
        .ok_or("usage: bot <hub hash> <rns config dir> <room>")?;

    let shutdown = ShutdownSignal::new();
    let runtime = rns_runtime::reticulum::init(
        Some(&config),
        None,
        shutdown.clone(),
        Arc::new(AtomicBool::new(true)),
    )
    .await?;
    let client = RrcClient::new(runtime, Identity::new());
    let mut events = client.subscribe();

    client.connect(destination, Some("rust-bot")).await?;
    client
        .wait_until_connected(destination, Duration::from_secs(30))
        .await?;
    client.join(destination, &room, None).await?;
    client
        .wait_until_joined(destination, &room, Duration::from_secs(30))
        .await?;
    println!("rust-bot joined {room}; commands: !ping, !who");

    while let Ok(event) = events.recv().await {
        match event {
            Event::Message(message)
                if message.hub == destination
                    && message.room.as_deref() == Some(room.as_str())
                    && message.kind == MessageKind::Message
                    && message.nick.as_deref() != Some("rust-bot") =>
            {
                match message.body.trim() {
                    "!ping" => {
                        client.send_message(destination, &room, "pong").await?;
                    }
                    "!who" => {
                        let users = client
                            .list_users(destination, &room, Duration::from_secs(30))
                            .await?;
                        client
                            .send_message(
                                destination,
                                &room,
                                &format!("{} user(s) in {room}", users.len()),
                            )
                            .await?;
                    }
                    _ => {}
                }
            }
            Event::HubChanged(hub) if hub.destination_hash == destination && !hub.connected => {
                eprintln!("hub disconnected: {}", hub.detail);
            }
            _ => {}
        }
    }

    client.shutdown().await?;
    shutdown.trigger();
    Ok(())
}

fn parse_hash(value: &str) -> Result<[u8; 16], Box<dyn Error>> {
    if value.len() != 32 {
        return Err("destination hash must contain 32 hexadecimal characters".into());
    }
    let mut output = [0u8; 16];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(output)
}
