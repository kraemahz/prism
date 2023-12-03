use serde::{Serialize, Deserialize};

pub mod log_capnp {
    include!(concat!(env!("OUT_DIR"), "/log_capnp.rs"));
}
pub use crate::log_capnp as log;
pub mod pubsub_capnp {
    include!(concat!(env!("OUT_DIR"), "/pubsub_capnp.rs"));
}
pub use crate::pubsub_capnp as pubsub;


pub type Beam = String;

#[derive(Serialize, Deserialize)]
pub struct ClientRequest {
    pub id: u64,
    pub rtype: RequestType
}

#[derive(Serialize, Deserialize)]
pub enum RequestType {
    AddBeam(Beam),
    ListBeams,
    Subscribe(Beam, Option<u64>),
    Unsubscribe(Beam)
}


#[derive(Serialize, Deserialize)]
pub struct ServerResponse {
    pub id: u64,
    pub rtype: ResponseType
}

#[derive(Serialize, Deserialize)]
pub enum ResponseType {
    Ack,
    Beams(Vec<Beam>),
}
