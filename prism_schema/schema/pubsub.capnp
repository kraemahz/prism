@0xc7d34a1f82bf2a86;

struct Emission {
    beam @0 :Text;
    payload @1 :Data;
}

struct Beam {
    name @0: Text;
    photons @1: List(Photon);
}

struct Photon {
    index @0 :UInt64;
    time @1 :Int64;
    payload @2 :Data;
}

struct ClientGreeting {
    id @0 :UInt64;
}

struct ClientMessage {
    emission @0 :Emission;
}

struct ServerGreeting {
    id @0 :UInt64;
}

struct ServerMessage {
    beams @0 :List(Beam);
}
