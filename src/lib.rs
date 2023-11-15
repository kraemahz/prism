pub mod queue;

mod log_capnp {
    include!(concat!(env!("OUT_DIR"), "/log_capnp.rs"));
}
