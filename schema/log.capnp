# log.capnp
@0x808a3ae4d62197a0;
struct DiskEntry {
  index @0 :UInt64;
  time @1 :Int64;
  hash @2 :UInt32;
  payload @3 :Data;
}
