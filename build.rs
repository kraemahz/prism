fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/log.capnp")
        .run()
        .expect("compiling schema");
}
