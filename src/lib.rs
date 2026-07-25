mod client;

pub use client::{Error, Event, Hub, Message, MessageKind, Resource, RrcClient};
pub use rs_rrc::{self, Envelope, RoomInfo, UserInfo};
