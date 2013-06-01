
namespace java ch.usi.da.smr.thrift.gen

struct Value {
  1: binary cmd,
}

service PaxosProposerService {

   i32 propose(1:Value value),
   
   void nb_propose(1:Value value),

}

service PaxosLearnerService {
      
   Value deliver(1:i32 timeout),

   Value nb_deliver(),

}

// additional state machine replication stuff
enum CmdType {
    GET,
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
