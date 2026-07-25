use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rns_identity::identity::Identity;
use rns_runtime::link_client::{LinkSession, LinkSessionHandle};
use rns_runtime::reticulum::ReticulumHandle;
use rns_transport::messages::{TransportMessage, TransportQuery, TransportQueryResponse};
use rs_rrc::*;
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, mpsc, oneshot};

const CHANNEL_CAPACITY: usize = 256;
const TIMEOUT: Duration = Duration::from_secs(30);
const RESOURCE_TEXT_THRESHOLD: usize = 300;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("RRC client has stopped")]
    Stopped,
    #[error("hub is not connected")]
    NotConnected,
    #[error("hub identity is not known")]
    UnknownIdentity,
    #[error("invalid room name")]
    InvalidRoom,
    #[error("invalid RRC command target")]
    InvalidTarget,
    #[error("invalid RRC message")]
    InvalidMessage,
    #[error("invalid RRC resource")]
    InvalidResource,
    #[error("RRC hub does not support resource envelopes")]
    ResourcesUnsupported,
    #[error("RRC hub does not support actions")]
    ActionsUnsupported,
    #[error("RRC hub does not support direct notices")]
    DirectNoticesUnsupported,
    #[error("timed out waiting for an RRC event")]
    Timeout,
    #[error("RRC event receiver lagged by {0} events")]
    EventLagged(u64),
    #[error("{0}")]
    Transport(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hub {
    pub destination_hash: [u8; 16],
    pub name: Option<String>,
    pub nick: Option<String>,
    pub welcome: Option<Welcome>,
    pub connected: bool,
    pub rooms: Vec<String>,
    pub public_rooms: Vec<RoomInfo>,
    pub room_states: BTreeMap<String, RoomState>,
    pub room_users: BTreeMap<String, Vec<UserInfo>>,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageKind {
    Message,
    Notice,
    Action,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoomMode {
    Moderated,
    InviteOnly,
    TopicOperatorsOnly,
    NoOutsideMessages,
    Private,
}

impl RoomMode {
    fn flag(self) -> char {
        match self {
            Self::Moderated => 'm',
            Self::InviteOnly => 'i',
            Self::TopicOperatorsOnly => 't',
            Self::NoOutsideMessages => 'n',
            Self::Private => 'p',
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub hub: [u8; 16],
    pub room: Option<String>,
    pub source: Option<[u8; 16]>,
    pub destination: Option<[u8; 16]>,
    pub nick: Option<String>,
    pub body: String,
    pub timestamp_ms: u64,
    pub kind: MessageKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Resource {
    pub hub: [u8; 16],
    pub room: Option<String>,
    pub source: Option<[u8; 16]>,
    pub nick: Option<String>,
    pub timestamp_ms: u64,
    pub descriptor: ResourceDescriptor,
    pub data: Vec<u8>,
    pub resource_hash: [u8; 32],
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    HubChanged(Hub),
    Message(Message),
    RoomList {
        hub: [u8; 16],
        rooms: Vec<RoomInfo>,
    },
    UserList {
        hub: [u8; 16],
        room: String,
        users: Vec<UserInfo>,
    },
    Envelope {
        hub: [u8; 16],
        envelope: Envelope,
    },
    Resource(Resource),
    InvalidEnvelope {
        hub: [u8; 16],
        error: String,
    },
}

#[derive(Clone)]
pub struct RrcClient {
    commands: mpsc::Sender<Command>,
    events: broadcast::Sender<Event>,
    source: [u8; 16],
    query_ids: Arc<AtomicU64>,
}

enum Command {
    Connect {
        destination: [u8; 16],
        nick: Option<String>,
        response: oneshot::Sender<Result<Hub, Error>>,
    },
    Send {
        destination: [u8; 16],
        envelope: Envelope,
        response: oneshot::Sender<Result<(), Error>>,
    },
    SendResource {
        destination: [u8; 16],
        envelope: Envelope,
        data: Vec<u8>,
        response: oneshot::Sender<Result<(), Error>>,
    },
    Disconnect {
        destination: [u8; 16],
        response: oneshot::Sender<Result<(), Error>>,
    },
    SetNick {
        destination: [u8; 16],
        nick: String,
        response: oneshot::Sender<Result<Hub, Error>>,
    },
    Hubs {
        response: oneshot::Sender<Vec<Hub>>,
    },
    ListRooms {
        destination: [u8; 16],
        query_id: u64,
        response: oneshot::Sender<Result<Vec<RoomInfo>, Error>>,
    },
    ListUsers {
        destination: [u8; 16],
        room: String,
        query_id: u64,
        response: oneshot::Sender<Result<Vec<UserInfo>, Error>>,
    },
    Ping {
        destination: [u8; 16],
        query_id: u64,
        response: oneshot::Sender<Result<Duration, Error>>,
    },
    CancelQuery {
        query_id: u64,
    },
    Reconnect {
        destination: [u8; 16],
        session_id: u64,
    },
    Shutdown,
}

type QuerySender<T> = oneshot::Sender<Result<Vec<T>, Error>>;
type PendingQuery<T> = (u64, QuerySender<T>);

struct Session {
    id: u64,
    handle: LinkSessionHandle,
    hub: Hub,
    nick: Option<String>,
    desired_rooms: BTreeMap<String, Option<String>>,
    reconnect_attempt: u32,
    pending_room_queries: VecDeque<PendingQuery<RoomInfo>>,
    pending_user_queries: BTreeMap<String, VecDeque<PendingQuery<UserInfo>>>,
    pending_pings: BTreeMap<u64, (Instant, oneshot::Sender<Result<Duration, Error>>)>,
    restore_rooms_on_welcome: bool,
}

struct Inbound {
    hub: [u8; 16],
    session_id: u64,
    result: Result<Vec<u8>, String>,
}

struct ActorChannels {
    commands: mpsc::Sender<Command>,
    inbound: mpsc::Sender<Inbound>,
    events: broadcast::Sender<Event>,
}

impl RrcClient {
    pub fn new(runtime: ReticulumHandle, identity: Identity) -> Self {
        let source = identity.hash;
        let (commands, command_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (events, _) = broadcast::channel(CHANNEL_CAPACITY);
        tokio::spawn(run(
            runtime,
            identity,
            commands.clone(),
            command_rx,
            events.clone(),
        ));
        Self {
            commands,
            events,
            source,
            query_ids: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    pub async fn connect(&self, destination: [u8; 16], nick: Option<&str>) -> Result<Hub, Error> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::Connect {
                destination,
                nick: nick.map(str::to_string),
                response,
            })
            .await
            .map_err(|_| Error::Stopped)?;
        receiver.await.map_err(|_| Error::Stopped)?
    }

    pub async fn disconnect(&self, destination: [u8; 16]) -> Result<(), Error> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::Disconnect {
                destination,
                response,
            })
            .await
            .map_err(|_| Error::Stopped)?;
        receiver.await.map_err(|_| Error::Stopped)?
    }

    pub async fn set_nick(&self, destination: [u8; 16], nick: &str) -> Result<Hub, Error> {
        let nick = normalize_nick(nick, 32).ok_or(Error::InvalidMessage)?;
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::SetNick {
                destination,
                nick,
                response,
            })
            .await
            .map_err(|_| Error::Stopped)?;
        receiver.await.map_err(|_| Error::Stopped)?
    }

    pub async fn hubs(&self) -> Result<Vec<Hub>, Error> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::Hubs { response })
            .await
            .map_err(|_| Error::Stopped)?;
        receiver.await.map_err(|_| Error::Stopped)
    }

    pub async fn hub(&self, destination: [u8; 16]) -> Result<Option<Hub>, Error> {
        Ok(self
            .hubs()
            .await?
            .into_iter()
            .find(|hub| hub.destination_hash == destination))
    }

    pub async fn wait_until_connected(
        &self,
        destination: [u8; 16],
        timeout: Duration,
    ) -> Result<Hub, Error> {
        let mut events = self.subscribe();
        if let Some(hub) = self.hub(destination).await?
            && hub.connected
            && hub.name.is_some()
        {
            return Ok(hub);
        }
        tokio::time::timeout(timeout, async {
            loop {
                match events.recv().await {
                    Ok(Event::HubChanged(hub))
                        if hub.destination_hash == destination
                            && hub.connected
                            && hub.name.is_some() =>
                    {
                        return Ok(hub);
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        return Err(Error::EventLagged(count));
                    }
                    Err(broadcast::error::RecvError::Closed) => return Err(Error::Stopped),
                }
            }
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    pub async fn wait_until_joined(
        &self,
        destination: [u8; 16],
        room: &str,
        timeout: Duration,
    ) -> Result<Hub, Error> {
        let room = room.trim().trim_start_matches('#').to_ascii_lowercase();
        let mut events = self.subscribe();
        if let Some(hub) = self.hub(destination).await?
            && hub.rooms.iter().any(|value| value == &room)
        {
            return Ok(hub);
        }
        tokio::time::timeout(timeout, async {
            loop {
                match events.recv().await {
                    Ok(Event::HubChanged(hub))
                        if hub.destination_hash == destination
                            && hub.rooms.iter().any(|value| value == &room) =>
                    {
                        return Ok(hub);
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        return Err(Error::EventLagged(count));
                    }
                    Err(broadcast::error::RecvError::Closed) => return Err(Error::Stopped),
                }
            }
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    pub async fn wait_for_message(
        &self,
        destination: Option<[u8; 16]>,
        room: Option<&str>,
        timeout: Duration,
    ) -> Result<Message, Error> {
        let room = room.map(|value| value.trim().trim_start_matches('#').to_ascii_lowercase());
        let mut events = self.subscribe();
        tokio::time::timeout(timeout, async {
            loop {
                match events.recv().await {
                    Ok(Event::Message(message))
                        if message_matches(&message, destination, room.as_deref()) =>
                    {
                        return Ok(message);
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        return Err(Error::EventLagged(count));
                    }
                    Err(broadcast::error::RecvError::Closed) => return Err(Error::Stopped),
                }
            }
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    pub async fn list_rooms(
        &self,
        destination: [u8; 16],
        timeout: Duration,
    ) -> Result<Vec<RoomInfo>, Error> {
        let query_id = self.next_query_id();
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::ListRooms {
                destination,
                query_id,
                response,
            })
            .await
            .map_err(|_| Error::Stopped)?;
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::Stopped),
            Err(_) => {
                let _ = self.commands.send(Command::CancelQuery { query_id }).await;
                Err(Error::Timeout)
            }
        }
    }

    pub async fn list_users(
        &self,
        destination: [u8; 16],
        room: &str,
        timeout: Duration,
    ) -> Result<Vec<UserInfo>, Error> {
        let room = normalize_room(room, 64).ok_or(Error::InvalidRoom)?;
        let query_id = self.next_query_id();
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::ListUsers {
                destination,
                room,
                query_id,
                response,
            })
            .await
            .map_err(|_| Error::Stopped)?;
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::Stopped),
            Err(_) => {
                let _ = self.commands.send(Command::CancelQuery { query_id }).await;
                Err(Error::Timeout)
            }
        }
    }

    pub async fn ping(&self, destination: [u8; 16], timeout: Duration) -> Result<Duration, Error> {
        let query_id = self.next_query_id();
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::Ping {
                destination,
                query_id,
                response,
            })
            .await
            .map_err(|_| Error::Stopped)?;
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::Stopped),
            Err(_) => {
                let _ = self.commands.send(Command::CancelQuery { query_id }).await;
                Err(Error::Timeout)
            }
        }
    }

    pub async fn join(
        &self,
        destination: [u8; 16],
        room: &str,
        key: Option<&str>,
    ) -> Result<(), Error> {
        let envelope = Envelope::join(&self.source, room, key).ok_or(Error::InvalidRoom)?;
        self.send_envelope(destination, envelope).await
    }

    pub async fn part(&self, destination: [u8; 16], room: &str) -> Result<(), Error> {
        let envelope = Envelope::part(&self.source, room).ok_or(Error::InvalidRoom)?;
        self.send_envelope(destination, envelope).await
    }

    pub async fn send_message(
        &self,
        destination: [u8; 16],
        room: &str,
        body: &str,
    ) -> Result<(), Error> {
        self.send_text(destination, room, body, false).await
    }

    pub async fn send_action(
        &self,
        destination: [u8; 16],
        room: &str,
        body: &str,
    ) -> Result<(), Error> {
        self.send_text(destination, room, body, true).await
    }

    pub async fn send_direct_notice(
        &self,
        destination: [u8; 16],
        target: [u8; 16],
        body: &str,
    ) -> Result<(), Error> {
        let hub = self
            .hub(destination)
            .await?
            .filter(|hub| hub.connected)
            .ok_or(Error::NotConnected)?;
        let welcome = hub.welcome.as_ref();
        if !welcome.is_some_and(|value| value.capabilities.direct_notice) {
            return Err(Error::DirectNoticesUnsupported);
        }
        if body.len()
            > welcome
                .and_then(|value| value.limits.max_message_bytes)
                .unwrap_or(16_384)
        {
            return Err(Error::InvalidMessage);
        }
        let envelope =
            Envelope::direct_notice(&self.source, &target, body).ok_or(Error::InvalidMessage)?;
        self.send_envelope(destination, envelope).await
    }

    pub async fn send_command(
        &self,
        destination: [u8; 16],
        room: Option<&str>,
        command: &str,
    ) -> Result<(), Error> {
        let command = if command.starts_with('/') {
            command.to_string()
        } else {
            format!("/{command}")
        };
        let envelope =
            Envelope::command(&self.source, room, &command).ok_or(Error::InvalidMessage)?;
        self.send_envelope(destination, envelope).await
    }

    pub async fn register_room(
        &self,
        destination: [u8; 16],
        room: &str,
        registered: bool,
    ) -> Result<(), Error> {
        let room = command_room(room)?;
        let command = if registered {
            format!("/register {room}")
        } else {
            format!("/unregister {room}")
        };
        self.send_command(destination, Some(&room), &command).await
    }

    pub async fn set_topic(
        &self,
        destination: [u8; 16],
        room: &str,
        topic: &str,
    ) -> Result<(), Error> {
        let room = command_room(room)?;
        let topic = command_text(topic)?;
        self.send_command(destination, Some(&room), &format!("/topic {room} {topic}"))
            .await
    }

    pub async fn set_room_mode(
        &self,
        destination: [u8; 16],
        room: &str,
        mode: RoomMode,
        enabled: bool,
    ) -> Result<(), Error> {
        let room = command_room(room)?;
        let sign = if enabled { '+' } else { '-' };
        self.send_command(
            destination,
            Some(&room),
            &format!("/mode {room} {sign}{}", mode.flag()),
        )
        .await
    }

    pub async fn set_room_key(
        &self,
        destination: [u8; 16],
        room: &str,
        key: Option<&str>,
    ) -> Result<(), Error> {
        let room = command_room(room)?;
        let command = match key {
            Some(key) => format!("/mode {room} +k {}", command_text(key)?),
            None => format!("/mode {room} -k"),
        };
        self.send_command(destination, Some(&room), &command).await
    }

    pub async fn set_operator(
        &self,
        destination: [u8; 16],
        room: &str,
        target: &str,
        enabled: bool,
    ) -> Result<(), Error> {
        self.member_command(
            destination,
            room,
            target,
            if enabled { "op" } else { "deop" },
        )
        .await
    }

    pub async fn set_voice(
        &self,
        destination: [u8; 16],
        room: &str,
        target: &str,
        enabled: bool,
    ) -> Result<(), Error> {
        self.member_command(
            destination,
            room,
            target,
            if enabled { "voice" } else { "devoice" },
        )
        .await
    }

    pub async fn kick(&self, destination: [u8; 16], room: &str, target: &str) -> Result<(), Error> {
        self.member_command(destination, room, target, "kick").await
    }

    pub async fn invite(
        &self,
        destination: [u8; 16],
        room: &str,
        target: &str,
    ) -> Result<(), Error> {
        self.access_command(destination, room, "invite", "add", Some(target))
            .await
    }

    pub async fn revoke_invite(
        &self,
        destination: [u8; 16],
        room: &str,
        target: &str,
    ) -> Result<(), Error> {
        self.access_command(destination, room, "invite", "del", Some(target))
            .await
    }

    pub async fn ban(&self, destination: [u8; 16], room: &str, target: &str) -> Result<(), Error> {
        self.access_command(destination, room, "ban", "add", Some(target))
            .await
    }

    pub async fn unban(
        &self,
        destination: [u8; 16],
        room: &str,
        target: &str,
    ) -> Result<(), Error> {
        self.access_command(destination, room, "ban", "del", Some(target))
            .await
    }

    pub async fn list_invites(&self, destination: [u8; 16], room: &str) -> Result<(), Error> {
        self.access_command(destination, room, "invite", "list", None)
            .await
    }

    pub async fn list_bans(&self, destination: [u8; 16], room: &str) -> Result<(), Error> {
        self.access_command(destination, room, "ban", "list", None)
            .await
    }

    async fn member_command(
        &self,
        destination: [u8; 16],
        room: &str,
        target: &str,
        command: &str,
    ) -> Result<(), Error> {
        let room = command_room(room)?;
        let target = command_target(target)?;
        self.send_command(
            destination,
            Some(&room),
            &format!("/{command} {room} {target}"),
        )
        .await
    }

    async fn access_command(
        &self,
        destination: [u8; 16],
        room: &str,
        command: &str,
        operation: &str,
        target: Option<&str>,
    ) -> Result<(), Error> {
        let room = command_room(room)?;
        let target = target.map(command_target).transpose()?;
        let command = match target {
            Some(target) => format!("/{command} {room} {operation} {target}"),
            None => format!("/{command} {room} {operation}"),
        };
        self.send_command(destination, Some(&room), &command).await
    }

    pub async fn send_envelope(
        &self,
        destination: [u8; 16],
        envelope: Envelope,
    ) -> Result<(), Error> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::Send {
                destination,
                envelope,
                response,
            })
            .await
            .map_err(|_| Error::Stopped)?;
        receiver.await.map_err(|_| Error::Stopped)?
    }

    pub async fn send_resource(
        &self,
        destination: [u8; 16],
        room: Option<&str>,
        kind: &str,
        data: Vec<u8>,
        encoding: Option<&str>,
    ) -> Result<(), Error> {
        let hub = self
            .hub(destination)
            .await?
            .filter(|hub| hub.connected)
            .ok_or(Error::NotConnected)?;
        if !hub
            .welcome
            .as_ref()
            .is_some_and(|welcome| welcome.capabilities.resource_envelope)
        {
            return Err(Error::ResourcesUnsupported);
        }
        let envelope = Envelope::resource(&self.source, room, kind, &data, encoding)
            .ok_or(Error::InvalidResource)?;
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(Command::SendResource {
                destination,
                envelope,
                data,
                response,
            })
            .await
            .map_err(|_| Error::Stopped)?;
        receiver.await.map_err(|_| Error::Stopped)?
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        self.commands
            .send(Command::Shutdown)
            .await
            .map_err(|_| Error::Stopped)
    }

    async fn send_text(
        &self,
        destination: [u8; 16],
        room: &str,
        body: &str,
        action: bool,
    ) -> Result<(), Error> {
        let hub = self
            .hub(destination)
            .await?
            .filter(|hub| hub.connected)
            .ok_or(Error::NotConnected)?;
        let welcome = hub.welcome.as_ref();
        if action && !welcome.is_some_and(|value| value.capabilities.action) {
            return Err(Error::ActionsUnsupported);
        }
        let supports_resources = welcome.is_some_and(|value| value.capabilities.resource_envelope);
        let packet_limit = welcome
            .and_then(|value| value.limits.max_message_bytes)
            .unwrap_or(16_384);
        if use_resource_for_text(body.len(), supports_resources, packet_limit)? {
            return self
                .send_resource(
                    destination,
                    Some(room),
                    if action { "action" } else { "message" },
                    body.as_bytes().to_vec(),
                    Some("utf-8"),
                )
                .await;
        }
        let envelope =
            Envelope::message(&self.source, room, body, action).ok_or(Error::InvalidMessage)?;
        self.send_envelope(destination, envelope).await
    }

    fn next_query_id(&self) -> u64 {
        self.query_ids.fetch_add(1, Ordering::Relaxed).max(1)
    }
}

fn message_matches(message: &Message, destination: Option<[u8; 16]>, room: Option<&str>) -> bool {
    destination.is_none_or(|destination| message.hub == destination)
        && room.is_none_or(|room| message.room.as_deref() == Some(room))
}

fn take_session_id(next: &mut u64) -> u64 {
    let current = *next;
    *next = next.wrapping_add(1).max(1);
    current
}

async fn run(
    runtime: ReticulumHandle,
    identity: Identity,
    command_tx: mpsc::Sender<Command>,
    mut commands: mpsc::Receiver<Command>,
    events: broadcast::Sender<Event>,
) {
    let source = identity.hash;
    let (inbound_tx, mut inbound_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let channels = ActorChannels {
        commands: command_tx,
        inbound: inbound_tx,
        events,
    };
    let mut sessions = BTreeMap::<[u8; 16], Session>::new();
    let mut next_session_id = 1u64;
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                if handle_command(
                    &runtime,
                    &identity,
                    &channels,
                    &mut sessions,
                    &mut next_session_id,
                    command,
                ).await {
                    break;
                }
            }
            inbound = inbound_rx.recv() => {
                let Some(inbound) = inbound else { break };
                handle_inbound(source, &channels, &mut sessions, inbound).await;
            }
        }
    }
    for session in sessions.values() {
        let _ = session.handle.close().await;
    }
}

async fn handle_command(
    runtime: &ReticulumHandle,
    identity: &Identity,
    channels: &ActorChannels,
    sessions: &mut BTreeMap<[u8; 16], Session>,
    next_session_id: &mut u64,
    command: Command,
) -> bool {
    let source = identity.hash;
    match command {
        Command::Connect {
            destination,
            nick,
            response,
        } => {
            let session_id = take_session_id(next_session_id);
            let result = connect(
                runtime,
                identity,
                destination,
                session_id,
                nick.as_deref(),
                &channels.inbound,
            )
            .await;
            match result {
                Ok(session) => {
                    if let Some(previous) = sessions.insert(destination, session) {
                        let _ = previous.handle.close().await;
                    }
                    let hub = sessions[&destination].hub.clone();
                    let _ = channels.events.send(Event::HubChanged(hub.clone()));
                    let _ = response.send(Ok(hub));
                }
                Err(error) => {
                    let _ = response.send(Err(error));
                }
            }
        }
        Command::Send {
            destination,
            mut envelope,
            response,
        } => {
            envelope.set_source(&source);
            let message_type = envelope.message_type();
            let room = envelope.room().map(str::to_string);
            let room_key = envelope.body_text().map(str::to_string);
            let result = match sessions.get_mut(&destination) {
                Some(session) => {
                    if matches!(message_type, Some(T_JOIN | T_MSG | T_NOTICE | T_ACTION))
                        && let Some(nick) = &session.nick
                    {
                        envelope.set_nick(nick);
                    }
                    let result = send(&session.handle, envelope).await;
                    if result.is_ok() {
                        match (message_type, room) {
                            (Some(T_JOIN), Some(room)) => {
                                session.desired_rooms.insert(room, room_key);
                            }
                            (Some(T_PART), Some(room)) => {
                                session.desired_rooms.remove(&room);
                            }
                            _ => {}
                        }
                    }
                    result
                }
                None => Err(Error::NotConnected),
            };
            let _ = response.send(result);
        }
        Command::SendResource {
            destination,
            mut envelope,
            data,
            response,
        } => {
            envelope.set_source(&source);
            let result = match sessions.get(&destination) {
                Some(session) => {
                    async {
                        send(&session.handle, envelope).await?;
                        session
                            .handle
                            .send_resource(data, false, TIMEOUT)
                            .await
                            .map_err(|error| Error::Transport(error.to_string()))?;
                        Ok(())
                    }
                    .await
                }
                None => Err(Error::NotConnected),
            };
            let _ = response.send(result);
        }
        Command::Disconnect {
            destination,
            response,
        } => {
            let result = match sessions.remove(&destination) {
                Some(mut session) => {
                    session.hub.connected = false;
                    session.hub.detail = "Disconnected".into();
                    let _ = channels.events.send(Event::HubChanged(session.hub));
                    session
                        .handle
                        .close()
                        .await
                        .map_err(|error| Error::Transport(error.to_string()))
                }
                None => Err(Error::NotConnected),
            };
            let _ = response.send(result);
        }
        Command::SetNick {
            destination,
            nick,
            response,
        } => {
            let result = match sessions.get_mut(&destination) {
                Some(session) => {
                    session.nick = Some(nick.clone());
                    session.hub.nick = Some(nick);
                    let hub = session.hub.clone();
                    let _ = channels.events.send(Event::HubChanged(hub.clone()));
                    Ok(hub)
                }
                None => Err(Error::NotConnected),
            };
            let _ = response.send(result);
        }
        Command::Hubs { response } => {
            let _ = response.send(
                sessions
                    .values()
                    .map(|session| session.hub.clone())
                    .collect(),
            );
        }
        Command::ListRooms {
            destination,
            query_id,
            response,
        } => {
            let result = match sessions.get_mut(&destination) {
                Some(session) => {
                    let mut envelope =
                        Envelope::command(&source, None, "/list").expect("valid LIST command");
                    if let Some(nick) = &session.nick {
                        envelope.set_nick(nick);
                    }
                    match send(&session.handle, envelope).await {
                        Ok(()) => {
                            session.pending_room_queries.push_back((query_id, response));
                            None
                        }
                        Err(error) => Some((response, error)),
                    }
                }
                None => Some((response, Error::NotConnected)),
            };
            if let Some((response, error)) = result {
                let _ = response.send(Err(error));
            }
        }
        Command::ListUsers {
            destination,
            room,
            query_id,
            response,
        } => {
            let result = match sessions.get_mut(&destination) {
                Some(session) => {
                    let command = format!("/who {room}");
                    let mut envelope = Envelope::command(&source, Some(&room), &command)
                        .expect("validated WHO command");
                    if let Some(nick) = &session.nick {
                        envelope.set_nick(nick);
                    }
                    match send(&session.handle, envelope).await {
                        Ok(()) => {
                            session
                                .pending_user_queries
                                .entry(room)
                                .or_default()
                                .push_back((query_id, response));
                            None
                        }
                        Err(error) => Some((response, error)),
                    }
                }
                None => Some((response, Error::NotConnected)),
            };
            if let Some((response, error)) = result {
                let _ = response.send(Err(error));
            }
        }
        Command::Ping {
            destination,
            query_id,
            response,
        } => {
            let result = match sessions.get_mut(&destination) {
                Some(session) => {
                    match send(&session.handle, Envelope::ping(&source, query_id)).await {
                        Ok(()) => {
                            session
                                .pending_pings
                                .insert(query_id, (Instant::now(), response));
                            None
                        }
                        Err(error) => Some((response, error)),
                    }
                }
                None => Some((response, Error::NotConnected)),
            };
            if let Some((response, error)) = result {
                let _ = response.send(Err(error));
            }
        }
        Command::CancelQuery { query_id } => {
            for session in sessions.values_mut() {
                session
                    .pending_room_queries
                    .retain(|(id, _)| *id != query_id);
                session.pending_user_queries.retain(|_, queries| {
                    queries.retain(|(id, _)| *id != query_id);
                    !queries.is_empty()
                });
                session.pending_pings.remove(&query_id);
            }
        }
        Command::Reconnect {
            destination,
            session_id,
        } => {
            reconnect(
                runtime,
                identity,
                channels,
                sessions,
                next_session_id,
                destination,
                session_id,
            )
            .await;
        }
        Command::Shutdown => return true,
    }
    false
}

async fn reconnect(
    runtime: &ReticulumHandle,
    identity: &Identity,
    channels: &ActorChannels,
    sessions: &mut BTreeMap<[u8; 16], Session>,
    next_session_id: &mut u64,
    destination: [u8; 16],
    expected_session_id: u64,
) {
    let Some(current) = sessions.get(&destination) else {
        return;
    };
    if current.id != expected_session_id || current.hub.connected {
        return;
    }
    let nick = current.nick.clone();
    let desired_rooms = current.desired_rooms.clone();
    let session_id = take_session_id(next_session_id);
    match connect(
        runtime,
        identity,
        destination,
        session_id,
        nick.as_deref(),
        &channels.inbound,
    )
    .await
    {
        Ok(mut replacement) => {
            replacement.desired_rooms = desired_rooms.clone();
            replacement.restore_rooms_on_welcome = true;
            replacement.hub.detail = "Reconnected; waiting for WELCOME".into();
            if let Some(previous) = sessions.insert(destination, replacement) {
                let _ = previous.handle.close().await;
            }
            let _ = channels
                .events
                .send(Event::HubChanged(sessions[&destination].hub.clone()));
        }
        Err(error) => {
            let Some(current) = sessions.get_mut(&destination) else {
                return;
            };
            if current.id != expected_session_id {
                return;
            }
            current.reconnect_attempt = current.reconnect_attempt.saturating_add(1);
            current.hub.detail = format!("Reconnect failed: {error}");
            let _ = channels.events.send(Event::HubChanged(current.hub.clone()));
            schedule_reconnect(
                channels.commands.clone(),
                destination,
                current.id,
                current.reconnect_attempt,
            );
        }
    }
}

fn schedule_reconnect(
    commands: mpsc::Sender<Command>,
    destination: [u8; 16],
    session_id: u64,
    attempt: u32,
) {
    tokio::spawn(async move {
        tokio::time::sleep(reconnect_delay(attempt)).await;
        let _ = commands
            .send(Command::Reconnect {
                destination,
                session_id,
            })
            .await;
    });
}

fn reconnect_delay(attempt: u32) -> Duration {
    Duration::from_secs(1u64 << attempt.saturating_sub(1).min(6))
}

async fn connect(
    runtime: &ReticulumHandle,
    identity: &Identity,
    destination: [u8; 16],
    session_id: u64,
    nick: Option<&str>,
    inbound_tx: &mpsc::Sender<Inbound>,
) -> Result<Session, Error> {
    runtime
        .transport_tx
        .send(TransportMessage::RequestPath {
            destination_hash: destination,
        })
        .await
        .map_err(|error| Error::Transport(error.to_string()))?;
    runtime
        .await_path(destination, TIMEOUT)
        .await
        .map_err(|error| Error::Transport(error.to_string()))?;
    let public_key = match runtime
        .query_control(TransportQuery::Recall {
            destination_hash: destination,
        })
        .await
    {
        Some(TransportQueryResponse::Announce(Some(entry))) => entry.public_key,
        _ => None,
    }
    .ok_or(Error::UnknownIdentity)?;
    let handle =
        LinkSession::prepare_with_public_key(runtime, identity.clone(), destination, public_key, 1)
            .spawn(TIMEOUT);
    handle
        .identify()
        .await
        .map_err(|error| Error::Transport(error.to_string()))?;
    send(&handle, Envelope::hello(&identity.hash, nick)).await?;
    let reader = handle.clone();
    let tx = inbound_tx.clone();
    tokio::spawn(async move {
        loop {
            let result = reader.recv().await.map_err(|error| error.to_string());
            let closed = result.is_err();
            if tx
                .send(Inbound {
                    hub: destination,
                    session_id,
                    result,
                })
                .await
                .is_err()
                || closed
            {
                break;
            }
        }
    });
    Ok(Session {
        id: session_id,
        handle,
        hub: Hub {
            destination_hash: destination,
            name: None,
            nick: nick.map(str::to_string),
            welcome: None,
            connected: true,
            rooms: Vec::new(),
            public_rooms: Vec::new(),
            room_states: BTreeMap::new(),
            room_users: BTreeMap::new(),
            detail: "Waiting for WELCOME".into(),
        },
        nick: nick.map(str::to_string),
        desired_rooms: BTreeMap::new(),
        reconnect_attempt: 0,
        pending_room_queries: VecDeque::new(),
        pending_user_queries: BTreeMap::new(),
        pending_pings: BTreeMap::new(),
        restore_rooms_on_welcome: false,
    })
}

async fn send(handle: &LinkSessionHandle, envelope: Envelope) -> Result<(), Error> {
    let payload = envelope
        .encode()
        .map_err(|error| Error::Transport(error.to_string()))?;
    handle
        .send_payload(payload, false, TIMEOUT)
        .await
        .map_err(|error| Error::Transport(error.to_string()))?;
    Ok(())
}

async fn handle_inbound(
    source: [u8; 16],
    channels: &ActorChannels,
    sessions: &mut BTreeMap<[u8; 16], Session>,
    inbound: Inbound,
) {
    let Some(session) = sessions.get_mut(&inbound.hub) else {
        return;
    };
    if session.id != inbound.session_id {
        return;
    }
    let bytes = match inbound.result {
        Ok(bytes) => bytes,
        Err(error) => {
            session.hub.connected = false;
            session.hub.detail = error;
            session.reconnect_attempt = session.reconnect_attempt.saturating_add(1);
            let _ = channels.events.send(Event::HubChanged(session.hub.clone()));
            schedule_reconnect(
                channels.commands.clone(),
                inbound.hub,
                session.id,
                session.reconnect_attempt,
            );
            return;
        }
    };
    let envelope = match Envelope::decode(&bytes) {
        Ok(envelope) => envelope,
        Err(error) => {
            let _ = channels.events.send(Event::InvalidEnvelope {
                hub: inbound.hub,
                error: error.to_string(),
            });
            return;
        }
    };
    let room_state_changed = envelope
        .room()
        .zip(envelope.room_state())
        .map(|(room, state)| {
            let directory_changed =
                apply_room_state_to_directory(&mut session.hub.public_rooms, room, &state);
            let previous = session
                .hub
                .room_states
                .insert(room.to_string(), state.clone());
            previous.as_ref() != Some(&state) || directory_changed
        })
        .unwrap_or(false);
    if room_state_changed && envelope.message_type() != Some(T_JOINED) {
        let _ = channels.events.send(Event::HubChanged(session.hub.clone()));
    }
    match envelope.message_type() {
        Some(T_WELCOME) => {
            session.reconnect_attempt = 0;
            session.hub.welcome = envelope.welcome();
            session.hub.name = session
                .hub
                .welcome
                .as_ref()
                .and_then(|welcome| welcome.hub_name.clone());
            session.hub.detail = "Connected".into();
            if session.restore_rooms_on_welcome {
                session.restore_rooms_on_welcome = false;
                for (room, key) in session.desired_rooms.clone() {
                    let Some(mut join) = Envelope::join(&source, &room, key.as_deref()) else {
                        continue;
                    };
                    if let Some(nick) = &session.nick {
                        join.set_nick(nick);
                    }
                    if let Err(error) = send(&session.handle, join).await {
                        session.hub.detail = format!("Room restore failed: {error}");
                        break;
                    }
                }
            }
            let _ = channels.events.send(Event::HubChanged(session.hub.clone()));
        }
        Some(T_JOINED) => {
            if let Some(room) = envelope.room() {
                if !session.hub.rooms.iter().any(|value| value == room) {
                    session.hub.rooms.push(room.to_string());
                }
                session.hub.room_users.remove(room);
            }
            let _ = channels.events.send(Event::HubChanged(session.hub.clone()));
        }
        Some(T_PARTED) => {
            if let Some(room) = envelope.room() {
                session.hub.rooms.retain(|value| value != room);
                session.hub.room_states.remove(room);
                session.hub.room_users.remove(room);
            }
            let _ = channels.events.send(Event::HubChanged(session.hub.clone()));
        }
        Some(T_PING) => {
            let _ = send(&session.handle, Envelope::pong(&source, &envelope)).await;
        }
        Some(T_PONG) => {
            if let Some(query_id) = envelope.integer(K_BODY)
                && let Some((started, response)) = session.pending_pings.remove(&query_id)
            {
                let _ = response.send(Ok(started.elapsed()));
            }
        }
        Some(T_RESOURCE_ENVELOPE) => {
            let Some(descriptor) = envelope.resource_descriptor() else {
                emit_invalid(
                    &channels.events,
                    inbound.hub,
                    "invalid RRC resource descriptor",
                );
                return;
            };
            let received = match session.handle.recv_resource(TIMEOUT).await {
                Ok(received) => received,
                Err(error) => {
                    emit_invalid(&channels.events, inbound.hub, &error.to_string());
                    return;
                }
            };
            let actual_sha: [u8; 32] = Sha256::digest(&received.data).into();
            if received.data.len() != descriptor.size
                || descriptor
                    .sha256
                    .is_some_and(|expected| expected != actual_sha)
            {
                emit_invalid(
                    &channels.events,
                    inbound.hub,
                    "RRC resource size or SHA-256 mismatch",
                );
                return;
            }
            let resource = Resource {
                hub: inbound.hub,
                room: envelope.room().map(str::to_string),
                source: envelope.source(),
                nick: envelope.nick().map(str::to_string),
                timestamp_ms: envelope.timestamp_ms().unwrap_or_default(),
                descriptor,
                data: received.data,
                resource_hash: received.resource_hash,
            };
            if resource
                .descriptor
                .encoding
                .as_deref()
                .is_none_or(|encoding| encoding.eq_ignore_ascii_case("utf-8"))
                && let Ok(body) = String::from_utf8(resource.data.clone())
                && let Some(kind) = resource_message_kind(&resource.descriptor.kind)
            {
                let consumed = kind == MessageKind::Notice
                    && resolve_query_notice(session, &channels.events, inbound.hub, &body, None);
                if !consumed {
                    let _ = channels.events.send(Event::Message(Message {
                        hub: resource.hub,
                        room: resource.room.clone(),
                        source: resource.source,
                        destination: None,
                        nick: resource.nick.clone(),
                        body,
                        timestamp_ms: resource.timestamp_ms,
                        kind,
                    }));
                }
            }
            let _ = channels.events.send(Event::Resource(resource));
        }
        Some(T_NOTICE) => {
            let body = envelope.body_text().unwrap_or_default().to_string();
            let users = envelope.user_list();
            if body.starts_with("nick changed:")
                && let Some(room) = envelope.room()
                && session.hub.room_users.remove(room).is_some()
            {
                let _ = channels.events.send(Event::HubChanged(session.hub.clone()));
            }
            if !resolve_query_notice(session, &channels.events, inbound.hub, &body, users) {
                let _ = channels.events.send(Event::Message(Message {
                    hub: inbound.hub,
                    room: envelope.room().map(str::to_string),
                    source: envelope.source(),
                    destination: envelope
                        .bytes(K_DST)
                        .and_then(|value| <[u8; 16]>::try_from(value).ok()),
                    nick: envelope.nick().map(str::to_string),
                    body,
                    timestamp_ms: envelope.timestamp_ms().unwrap_or_default(),
                    kind: MessageKind::Notice,
                }));
            }
        }
        Some(kind @ (T_MSG | T_ACTION | T_ERROR)) => {
            let kind = match kind {
                T_MSG => MessageKind::Message,
                T_ACTION => MessageKind::Action,
                _ => MessageKind::Error,
            };
            let _ = channels.events.send(Event::Message(Message {
                hub: inbound.hub,
                room: envelope.room().map(str::to_string),
                source: envelope.source(),
                destination: envelope
                    .bytes(K_DST)
                    .and_then(|value| <[u8; 16]>::try_from(value).ok()),
                nick: envelope.nick().map(str::to_string),
                body: envelope.body_text().unwrap_or_default().to_string(),
                timestamp_ms: envelope.timestamp_ms().unwrap_or_default(),
                kind,
            }));
        }
        _ => {}
    }
    let _ = channels.events.send(Event::Envelope {
        hub: inbound.hub,
        envelope,
    });
}

fn resolve_query_notice(
    session: &mut Session,
    events: &broadcast::Sender<Event>,
    hub: [u8; 16],
    body: &str,
    structured_users: Option<Vec<UserInfo>>,
) -> bool {
    if let Some(rooms) = parse_room_list_notice(body) {
        session.hub.public_rooms = rooms.clone();
        let _ = events.send(Event::RoomList {
            hub,
            rooms: rooms.clone(),
        });
        let _ = events.send(Event::HubChanged(session.hub.clone()));
        if let Some((_, response)) = session.pending_room_queries.pop_front() {
            let _ = response.send(Ok(rooms));
            return true;
        }
        return false;
    }
    if let Some((room, parsed_users)) = parse_who_notice(body) {
        let users = structured_users.unwrap_or(parsed_users);
        session.hub.room_users.insert(room.clone(), users.clone());
        let _ = events.send(Event::UserList {
            hub,
            room: room.clone(),
            users: users.clone(),
        });
        let _ = events.send(Event::HubChanged(session.hub.clone()));
        let Some(queries) = session.pending_user_queries.get_mut(&room) else {
            return false;
        };
        let Some((_, response)) = queries.pop_front() else {
            return false;
        };
        if queries.is_empty() {
            session.pending_user_queries.remove(&room);
        }
        let _ = response.send(Ok(users));
        return true;
    }
    false
}

fn emit_invalid(events: &broadcast::Sender<Event>, hub: [u8; 16], error: &str) {
    let _ = events.send(Event::InvalidEnvelope {
        hub,
        error: error.to_string(),
    });
}

fn resource_message_kind(kind: &str) -> Option<MessageKind> {
    match kind.to_ascii_lowercase().as_str() {
        "message" | "msg" => Some(MessageKind::Message),
        "notice" => Some(MessageKind::Notice),
        "action" => Some(MessageKind::Action),
        "error" => Some(MessageKind::Error),
        _ => None,
    }
}

fn command_room(room: &str) -> Result<String, Error> {
    normalize_room(room, 64).ok_or(Error::InvalidRoom)
}

fn command_target(target: &str) -> Result<&str, Error> {
    let target = target.trim();
    if target.is_empty()
        || target.len() > 128
        || target.chars().any(char::is_whitespace)
        || target.contains('\0')
    {
        Err(Error::InvalidTarget)
    } else {
        Ok(target)
    }
}

fn command_text(text: &str) -> Result<&str, Error> {
    let text = text.trim();
    if text.is_empty() || text.len() > 16_384 || text.contains(['\0', '\r', '\n']) {
        Err(Error::InvalidMessage)
    } else {
        Ok(text)
    }
}

fn use_resource_for_text(
    body_bytes: usize,
    supports_resources: bool,
    packet_limit: usize,
) -> Result<bool, Error> {
    if supports_resources && (body_bytes > RESOURCE_TEXT_THRESHOLD || body_bytes > packet_limit) {
        Ok(true)
    } else if body_bytes > packet_limit {
        Err(Error::ResourcesUnsupported)
    } else {
        Ok(false)
    }
}

fn apply_room_state_to_directory(rooms: &mut Vec<RoomInfo>, room: &str, state: &RoomState) -> bool {
    let existing = rooms.iter().position(|entry| entry.name == room);
    if !state.registered {
        if let Some(index) = existing {
            rooms.remove(index);
            return true;
        }
        return false;
    }
    if let Some(index) = existing {
        if rooms[index].topic != state.topic {
            rooms[index].topic = state.topic.clone();
            return true;
        }
        return false;
    }
    rooms.push(RoomInfo {
        name: room.to_string(),
        topic: state.topic.clone(),
    });
    rooms.sort_by(|left, right| left.name.cmp(&right.name));
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Message {
        Message {
            hub: [7; 16],
            room: Some("bots".into()),
            source: Some([8; 16]),
            destination: None,
            nick: Some("helper".into()),
            body: "hello".into(),
            timestamp_ms: 1,
            kind: MessageKind::Message,
        }
    }

    #[test]
    fn message_filters_are_optional_and_exact() {
        let message = message();
        assert!(message_matches(&message, None, None));
        assert!(message_matches(&message, Some([7; 16]), Some("bots")));
        assert!(!message_matches(&message, Some([9; 16]), Some("bots")));
        assert!(!message_matches(&message, Some([7; 16]), Some("other")));
    }

    #[test]
    fn recognizes_text_resource_kinds() {
        assert_eq!(resource_message_kind("NOTICE"), Some(MessageKind::Notice));
        assert_eq!(resource_message_kind("binary"), None);
    }

    #[test]
    fn session_ids_never_wrap_to_zero() {
        let mut next = u64::MAX;
        assert_eq!(take_session_id(&mut next), u64::MAX);
        assert_eq!(next, 1);
        assert_eq!(take_session_id(&mut next), 1);
    }

    #[test]
    fn reconnect_backoff_is_bounded() {
        assert_eq!(reconnect_delay(1), Duration::from_secs(1));
        assert_eq!(reconnect_delay(4), Duration::from_secs(8));
        assert_eq!(reconnect_delay(100), Duration::from_secs(64));
    }

    #[test]
    fn command_arguments_reject_injection_and_normalize_rooms() {
        assert_eq!(command_room(" #Rust ").unwrap(), "rust");
        assert!(matches!(command_room("bad room"), Err(Error::InvalidRoom)));
        assert_eq!(command_target("alice").unwrap(), "alice");
        assert!(matches!(
            command_target("alice /kick rust bob"),
            Err(Error::InvalidTarget)
        ));
        assert_eq!(command_text(" Room topic ").unwrap(), "Room topic");
        assert!(matches!(
            command_text("topic\n/kick rust bob"),
            Err(Error::InvalidMessage)
        ));
    }

    #[test]
    fn text_transport_respects_welcome_capabilities_and_limits() {
        assert!(!use_resource_for_text(300, true, 350).unwrap());
        assert!(use_resource_for_text(301, true, 350).unwrap());
        assert!(use_resource_for_text(201, true, 200).unwrap());
        assert!(!use_resource_for_text(300, false, 350).unwrap());
        assert!(matches!(
            use_resource_for_text(351, false, 350),
            Err(Error::ResourcesUnsupported)
        ));
    }

    #[test]
    fn structured_room_state_keeps_public_directory_current() {
        let mut rooms = vec![RoomInfo {
            name: "zeta".into(),
            topic: None,
        }];
        let registered = RoomState {
            registered: true,
            modes: "+r".into(),
            topic: Some("Rust".into()),
        };
        assert!(apply_room_state_to_directory(
            &mut rooms,
            "alpha",
            &registered
        ));
        assert_eq!(
            rooms,
            vec![
                RoomInfo {
                    name: "alpha".into(),
                    topic: Some("Rust".into()),
                },
                RoomInfo {
                    name: "zeta".into(),
                    topic: None,
                },
            ]
        );
        assert!(!apply_room_state_to_directory(
            &mut rooms,
            "alpha",
            &registered
        ));

        let unregistered = RoomState {
            registered: false,
            modes: "(none)".into(),
            topic: None,
        };
        assert!(apply_room_state_to_directory(
            &mut rooms,
            "alpha",
            &unregistered
        ));
        assert_eq!(rooms.len(), 1);
    }
}
