
namespace java ch.usi.da.smr.thrift.gen

enum ControlType {
    SUBSCRIBE,
    UNSUBSCRIBE,
    PREPARE,
}

struct Control {
  1: i32 id,
  2: ControlType type,
  3: i32 group,
  4: i32 ring,
}

struct Value {
  1: binary cmd,
  2: optional bool skip,
  3: optional Control control,
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
    RESPONSE,
}

struct Cmd {
  1: i32 id,
  2: CmdType type,
  3: string key,
  4: binary value,
  5: i32 count,
  6: optional Control control,
}

struct Message {
  1: i32 id,
  2: string sender,
  3: string receiver,
  4: list<Cmd> commands,
}
