
namespace java ch.usi.da.paxos.thrift.gen
namespace hs paxos

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

