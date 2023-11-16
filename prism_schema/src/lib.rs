pub mod log_capnp {
    include!(concat!(env!("OUT_DIR"), "/log_capnp.rs"));
}
pub use crate::log_capnp as log;
pub mod pubsub_capnp {
    include!(concat!(env!("OUT_DIR"), "/pubsub_capnp.rs"));
}
pub use crate::pubsub_capnp as pubsub;
