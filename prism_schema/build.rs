fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/log.capnp")
        .file("schema/pubsub.capnp")
        .run()
        .expect("compiling schema");
}
