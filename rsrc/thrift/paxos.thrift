
namespace java ch.usi.da.paxos.thrift.gen
namespace hs paxos

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
