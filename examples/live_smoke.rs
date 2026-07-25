use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use rns_identity::identity::Identity;
use rns_runtime::lifecycle::ShutdownSignal;
use rs_rrc_client::{Event, RrcClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    let destination = parse_hash(
        &arguments
            .next()
            .ok_or("usage: live_smoke <hub hash> <rns config dir> [room]")?,
    )?;
    let config = arguments
        .next()
        .ok_or("usage: live_smoke <hub hash> <rns config dir> [room]")?;
    let room = arguments
        .next()
        .unwrap_or_else(|| "rrc-client-smoke".into());

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
    client.connect(destination, Some("rs-rrc-smoke")).await?;
    client
        .wait_until_connected(destination, Duration::from_secs(30))
        .await?;
    let round_trip = client.ping(destination, Duration::from_secs(30)).await?;
    let hub = client.set_nick(destination, "rs-rrc-smoke-renamed").await?;
    if hub.nick.as_deref() != Some("rs-rrc-smoke-renamed") {
        return Err("hub nickname was not updated".into());
    }
    let rooms = client
        .list_rooms(destination, Duration::from_secs(30))
        .await?;
    if !rooms.iter().any(|room| room.name == "e2e") {
        return Err("registered e2e room was not returned by LIST".into());
    }
    client.join(destination, &room, None).await?;
    client
        .wait_until_joined(destination, &room, Duration::from_secs(30))
        .await?;
    let users = client
        .list_users(destination, &room, Duration::from_secs(30))
        .await?;
    if !users
        .iter()
        .any(|user| user.nick.as_deref() == Some("rs-rrc-smoke-renamed"))
    {
        return Err("smoke user was not returned by WHO".into());
    }

    let body = format!("resource round trip {}", "R".repeat(600));
    client.send_message(destination, &room, &body).await?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(Event::Message(message)) = events.recv().await
                && message.hub == destination
                && message.room.as_deref() == Some(room.as_str())
                && message.body == body
            {
                break;
            }
        }
    })
    .await?;

    client.disconnect(destination).await?;
    client.shutdown().await?;
    shutdown.trigger();
    println!(
        "RRC CLIENT SMOKE OK: hub={} room={room} ping={}ms",
        hub.name.unwrap_or_else(|| encode_hash(destination)),
        round_trip.as_millis(),
    );
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

fn encode_hash(value: [u8; 16]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}
