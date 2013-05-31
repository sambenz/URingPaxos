package ch.usi.da.paxos.thrift;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.ring.ProposerRole;
import ch.usi.da.paxos.ring.RingManager;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.storage.FutureDecision;
import ch.usi.da.paxos.thrift.gen.PaxosProposerService;
import ch.usi.da.paxos.thrift.gen.Value;

public class PaxosProposerServiceImpl implements PaxosProposerService.Iface {

	private final static Logger logger = Logger.getLogger(PaxosProposerServiceImpl.class);
	
	private final ProposerRole proposer;
	
	public PaxosProposerServiceImpl(RingManager ring) {
		PaxosRole role = PaxosRole.Proposer;
		proposer = new ProposerRole(ring,true);
		logger.debug("register role: " + role + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
		ring.registerRole(role);
		Thread t = new Thread(proposer);
		t.setName(role.toString());
		t.start();
	}

	@Override
	public int propose(Value value) throws TException {
		byte[] b = new byte[value.cmd.remaining()];
		value.cmd.get(b);
		FutureDecision f = proposer.propose(b);
		try {
			Decision d = f.getDecision(3000);
			if(d != null){
				return d.getInstance();
			}
		} catch (InterruptedException e) {
			logger.error(e);
		}
		return -1;
	}

	@Override
	public void nb_propose(Value value) throws TException {
		byte[] b = new byte[value.cmd.remaining()];
		value.cmd.get(b);
		proposer.propose(b);
	}

}
