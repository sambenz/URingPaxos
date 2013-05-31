package ch.usi.da.paxos.thrift;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.ring.LearnerRole;
import ch.usi.da.paxos.ring.RingManager;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.thrift.gen.PaxosLearnerService;
import ch.usi.da.paxos.thrift.gen.Value;

public class PaxosLearnerServiceImpl implements PaxosLearnerService.Iface {

	private final static Logger logger = Logger.getLogger(PaxosLearnerServiceImpl.class);
	
	private final BlockingQueue<Decision> values;
	
	public PaxosLearnerServiceImpl(RingManager ring) {
		PaxosRole role = PaxosRole.Learner;
		LearnerRole r = new LearnerRole(ring,true,null);
		values = r.getValues();
		logger.debug("register role: " + role + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
		ring.registerRole(role);
		Thread t = new Thread(r);
		t.setName(role.toString());
		t.start();
	}

	@Override
	public Value deliver(int timeout) throws TException {
		Value value = null;
		try {
			Decision d = values.poll(timeout,TimeUnit.MILLISECONDS);
			if(d != null){
				value = new Value(ByteBuffer.wrap(d.getValue().getValue()));
			}
		} catch (InterruptedException e) {
		}
		return value == null ? new Value() : value;
	}

	@Override
	public Value nb_deliver() throws TException {
		Value value = null;
		Decision d = values.poll();
		if(d != null){
			value = new Value(ByteBuffer.wrap(d.getValue().getValue()));
		}
		return value == null ? new Value() : value;
	}

}
