use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use rns_identity::identity::Identity;
use rns_runtime::lifecycle::ShutdownSignal;
use rs_rrc_client::{Error as ClientError, Event, RrcClient};

const ROOM_A: &str = "reconnect-a";
const ROOM_B: &str = "reconnect-b";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    let destination = parse_hash(
        &arguments
            .next()
            .ok_or("usage: live_reconnect <hub hash> <rns config dir>")?,
    )?;
    let config = arguments
        .next()
        .ok_or("usage: live_reconnect <hub hash> <rns config dir> [cycles]")?;
    let cycles = arguments
        .next()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(1);
    if cycles == 0 {
        return Err("reconnect cycle count must be positive".into());
    }

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
    client.connect(destination, Some("reconnect-test")).await?;
    client
        .wait_until_connected(destination, Duration::from_secs(30))
        .await?;
    for room in [ROOM_A, ROOM_B] {
        client.join(destination, room, None).await?;
        client
            .wait_until_joined(destination, room, Duration::from_secs(30))
            .await?;
    }
    for cycle in 1..=cycles {
        println!("RECONNECT READY {cycle}");
        std::io::stdout().flush()?;
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                if let Ok(Event::HubChanged(hub)) = events.recv().await
                    && hub.destination_hash == destination
                    && !hub.connected
                {
                    break;
                }
            }
        })
        .await?;
        client
            .wait_until_connected(destination, Duration::from_secs(60))
            .await?;
        for room in [ROOM_A, ROOM_B] {
            client
                .wait_until_joined(destination, room, Duration::from_secs(60))
                .await?;
        }
        println!("RECONNECT CYCLE {cycle}");
        std::io::stdout().flush()?;
    }

    let timed_out = client.list_rooms(destination, Duration::ZERO).await;
    if !matches!(timed_out, Err(ClientError::Timeout)) {
        return Err("zero-timeout LIST unexpectedly completed".into());
    }

    let (rooms_a, rooms_b, users_a, users_b, ping_a, ping_b) = tokio::join!(
        client.list_rooms(destination, Duration::from_secs(30)),
        client.list_rooms(destination, Duration::from_secs(30)),
        client.list_users(destination, ROOM_A, Duration::from_secs(30)),
        client.list_users(destination, ROOM_B, Duration::from_secs(30)),
        client.ping(destination, Duration::from_secs(30)),
        client.ping(destination, Duration::from_secs(30)),
    );
    let rooms_a = rooms_a?;
    let rooms_b = rooms_b?;
    let users_a = users_a?;
    let users_b = users_b?;
    let ping_a = ping_a?;
    let ping_b = ping_b?;
    if rooms_a != rooms_b {
        return Err("parallel LIST results differ".into());
    }
    if !users_a
        .iter()
        .any(|user| user.nick.as_deref() == Some("reconnect-test"))
        || !users_b
            .iter()
            .any(|user| user.nick.as_deref() == Some("reconnect-test"))
    {
        return Err("restored room WHO does not contain reconnect-test".into());
    }

    let body = format!("post-reconnect resource {}", "R".repeat(600));
    client.send_message(destination, ROOM_A, &body).await?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(Event::Message(message)) = events.recv().await
                && message.hub == destination
                && message.room.as_deref() == Some(ROOM_A)
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
        "RECONNECT OK: cycles={cycles} rooms=2 list={} ping={}ms/{}ms resource={}B",
        rooms_a.len(),
        ping_a.as_millis(),
        ping_b.as_millis(),
        body.len()
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
