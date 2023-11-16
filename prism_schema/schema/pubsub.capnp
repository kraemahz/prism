@0xc7d34a1f82bf2a86;

struct Capture {
    beam @0 :Text;
}

struct Emit {
    beam @0 :Text;
    payload @1 :Data;
}

struct Photon {
    index @0 :UInt64;
    time @1 :Int64;
    payload @2 :Data;
}

struct Transmissions {
    beams @0 :List(Text);
}

struct ClientMessage {
    id @0 :UInt64;
    command: union {
      capture @1 :Capture;
      emit @2 :Emit;
      transmissions @3 :Void;
    }
}

struct ServerMessage {
    message: union {
      id @0 :UInt64;
      photon @1 :Photon;
      transmissions @2 :Transmissions;
    }
}
