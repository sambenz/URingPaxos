
namespace java ch.usi.da.smr.thrift.gen

struct Value {
  1: binary cmd,
  2: optional bool skip
}

struct Decision {
  1: i32 ring,
  2: i64 instance,
  3: Value value,
}

service PaxosProposerService {

   i64 propose(1:Value value),
   
   void nb_propose(1:Value value),
}

service PaxosLearnerService {
      
   Decision deliver(1:i32 timeout),

   Decision nb_deliver(),

   void safe(1:i32 ring,2:i64 instance),
}


// additional state machine replication stuff
enum CmdType {
    GET,
    GETRANGE,
    PUT,
    DELETE,
    RESPONSE
}

struct Cmd {
  1: i32 id,
  2: CmdType type,
  3: string key,
  4: binary value,
  5: i32 count
}

struct Message {
  1: i32 id,
  2: string from,
  3: string to,
  4: list<Cmd> commands
}
