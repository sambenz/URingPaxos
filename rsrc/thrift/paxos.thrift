
namespace java ch.usi.da.paxos.thrift.gen
namespace hs paxos

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
