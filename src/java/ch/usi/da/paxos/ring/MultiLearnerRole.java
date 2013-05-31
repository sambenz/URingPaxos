package ch.usi.da.paxos.ring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.Decision;

/**
 * Name: MultiLearnerRole<br>
 * Description: <br>
 * 
 * Creation date: Mar 04, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class MultiLearnerRole extends Role {

	private final static Logger logger = Logger.getLogger(MultiLearnerRole.class);
	
	private final static Logger valuelogger = Logger.getLogger(Value.class);
	
	private final Map<Integer,RingDescription> ringmap = new HashMap<Integer,RingDescription>();
	
	private final List<Integer> ring = new ArrayList<Integer>();
	
	private final int maxRing = 20;
	
	private final LearnerRole[] learner = new LearnerRole[maxRing];
	
	private int M = 1;
		
	private int deliverRing;

	private final int[] skip_count = new int[maxRing];

	/**
	 * @param rings a list of rings
	 */
	public MultiLearnerRole(List<RingDescription> rings) {
		int minRing = maxRing+1;
		for(RingDescription ring : rings){
			if(ring.getRingID() < minRing){
				minRing = ring.getRingID();
			}
			this.ring.add(ring.getRingID());
			this.ringmap.put(ring.getRingID(),ring);
		}
		Collections.sort(ring);
		RingManager firstRing = rings.get(0).getRingmanger();
		deliverRing = minRing;
		logger.debug("MultiRingLearner initial deliverRing=" + deliverRing);
		if(firstRing.getConfiguration().containsKey(ConfigKey.multi_ring_m)){
			M = Integer.parseInt(firstRing.getConfiguration().get(ConfigKey.multi_ring_m));
			logger.debug("MultiRingLearner M=" + M);
		}
	}

	@Override
	public void run() {
		CountDownLatch latch = new CountDownLatch(ringmap.size());
		for(Entry<Integer,RingDescription> e : ringmap.entrySet()){
			// create learners
			RingManager ring = e.getValue().getRingmanger();
			Role r = new LearnerRole(ring,true,latch);
			learner[e.getKey()] = (LearnerRole) r;
			logger.debug("MultiRingLeaner register role: " + PaxosRole.Learner + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
			ring.registerRole(PaxosRole.Learner);		
			Thread t = new Thread(r);
			t.setName(PaxosRole.Learner + "-" + e.getKey());
			t.start();
			skip_count[e.getKey()] = 0;
		}
		try {
			latch.await(); // wait until all learner are ready
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		int count = 0;
		while(true){
			try{
				if(skip_count[deliverRing] > 0){
					count++;
					skip_count[deliverRing]--;
					if(valuelogger.isDebugEnabled()){
						valuelogger.info("Learner " + ringmap.get(deliverRing).getNodeID() + " ring " + deliverRing + " skiped a value (" + skip_count[deliverRing] + " skips left)");
					}
				}else{
					Decision d = learner[deliverRing].getValues().take();
					if(d.getValue() != null && d.getValue().getID().equals(Value.skipID) && d.getValue().getValue().length == 4){
						int skip = NetworkManager.byteToInt(d.getValue().getValue());
						skip_count[deliverRing] = skip_count[deliverRing] + skip;
					}else{
						count++;
						if(valuelogger.isDebugEnabled()){
							valuelogger.info("Learner " + ringmap.get(deliverRing).getNodeID() + " ring " + deliverRing + " " + d);
						}else if(valuelogger.isInfoEnabled()){
							valuelogger.info(d.getValue().asString());
						}
					}
				}
				if(count >= M){
					count = 0;
					deliverRing = getRingSuccessor(deliverRing);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;				
			}
		}
	}
	
	private int getRingSuccessor(int id){
		int pos = ring.indexOf(new Integer(id));
		if(pos+1 >= ring.size()){
			return ring.get(0);
		}else{
			return ring.get(pos+1);
		}
	}

}
