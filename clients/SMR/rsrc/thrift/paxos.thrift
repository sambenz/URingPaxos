
namespace java ch.usi.da.smr.thrift.gen

struct Value {
  1: binary cmd,
}

struct Decision {
  1: i32 ring,
  2: i32 instance,
  3: Value value,
}

service PaxosProposerService {

   i32 propose(1:Value value),
   
   void nb_propose(1:Value value),
}

service PaxosLearnerService {
      
   Decision deliver(1:i32 timeout),

   Decision nb_deliver(),

   void safe(1:i32 ring,2:i32 instance),
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
  4: binary value
}

struct Message {
  1: i32 id,
  2: string sender,
  3: list<Cmd> commands
}
