# rsRRC-client

Reusable asynchronous RRC client built on rsReticulum. It is intended for
interactive clients, services, and bots.

The client supports multiple hubs, room join/part, messages, actions, commands,
automatic PING/PONG handling, connection-state events, verified Reticulum
Resources for large notices, and access to every received protocol envelope.
Each connected `Hub` exposes the parsed WELCOME version, capabilities and
limits advertised by the server. Its `room_states` map tracks structured room
registration, modes, and topics when the hub advertises the optional rsRRC
room-state extension.

Room and member discovery is available without parsing notices manually:

```rust,no_run
# async fn discover(
#   client: rs_rrc_client::RrcClient,
#   hub: [u8; 16],
# ) -> Result<(), rs_rrc_client::Error> {
let rooms = client
    .list_rooms(hub, std::time::Duration::from_secs(30))
    .await?;
let users = client
    .list_users(hub, "bots", std::time::Duration::from_secs(30))
    .await?;
println!("{} rooms, {} users", rooms.len(), users.len());
# Ok(())
# }
```

Every successfully parsed LIST or WHO reply is also published as
`Event::RoomList` or `Event::UserList`. This is useful for long-running bots
that need to react to directory refreshes while another component owns the
request. The latest results are retained in `Hub::public_rooms` and
`Hub::room_users` and can be read later through `RrcClient::hub`.

Room administration has typed helpers for registration, topics, room modes
and keys, operator and voice roles, kicks, invites, and bans. These helpers
validate command arguments before sending them; `send_command` remains
available for server-specific extensions.

UTF-8 resources with an RRC text kind are emitted both as `Event::Resource`
and as a normal `Event::Message`. Binary and application-specific resources
remain available through `Event::Resource`.

Long messages and actions are automatically sent as an RRC Resource. Bots can
also send arbitrary resources explicitly:

```rust,no_run
# async fn send(
#   client: rs_rrc_client::RrcClient,
#   hub: [u8; 16],
# ) -> Result<(), rs_rrc_client::Error> {
client
    .send_resource(
        hub,
        Some("bots"),
        "blob",
        vec![0x42; 4096],
        None,
    )
    .await?;
# Ok(())
# }
```

```rust,no_run
use rs_rrc_client::{Event, RrcClient};

# async fn example(
#   runtime: rns_runtime::reticulum::ReticulumHandle,
#   identity: rns_identity::identity::Identity,
#   hub: [u8; 16],
# ) -> Result<(), rs_rrc_client::Error> {
let client = RrcClient::new(runtime, identity);
let mut events = client.subscribe();

client.connect(hub, Some("my-bot")).await?;
client.wait_until_connected(hub, std::time::Duration::from_secs(30)).await?;
client.join(hub, "bots", None).await?;
client.send_message(hub, "bots", "Hello from Rust").await?;

let message = client
    .wait_for_message(
        Some(hub),
        Some("bots"),
        std::time::Duration::from_secs(60),
    )
    .await?;
println!("{}: {}", message.nick.as_deref().unwrap_or("?"), message.body);

while let Ok(event) = events.recv().await {
    if let Event::Message(message) = event {
        println!("event: {}", message.body);
    }
}
# Ok(())
# }
```

## Live compatibility smoke test

With a reachable rsRRCD destination:

```text
cargo run --example live_smoke -- <hub-destination-hash> <rsReticulum-config-dir>
```

The smoke client connects, waits for WELCOME, joins a room, sends a Resource
backed message larger than one packet, waits for the echo, and disconnects.
