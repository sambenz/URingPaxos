package ch.usi.da.paxos.ring;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.Promise;

/**
 * Name: CoordinatorRole<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class CoordinatorRole extends Role {

	private final static Logger logger = Logger.getLogger(CoordinatorRole.class);

	private final RingManager ring;
	
	private final AtomicInteger instance = new AtomicInteger();

	private final TransferQueue<Promise> promises = new LinkedTransferQueue<Promise>();
	
	private final Map<Integer,Promise> phase1_in_transit = new ConcurrentHashMap<Integer,Promise>();

	private final Map<Integer,Promise> phase1range_in_transit = new ConcurrentHashMap<Integer,Promise>();

	private int reserved = 10000;
	
	private int resend_time = 2000;
		
	private volatile boolean fastmode = true; // True == Phase1Range
	
	private int successful_promise_count = 0; // Used to switch fastmode
	
	private final int enable_fastmode_threashold = 100;
	
	public int multi_ring_lambda = 9000; 

	public int multi_ring_delta_t = 100;
	
	public int value_count = 0;
	
	/**
	 * @param ring
	 */
	public CoordinatorRole(RingManager ring) {
		this.ring = ring;
		if(ring.getConfiguration().containsKey(ConfigKey.p1_preexecution_number)){
			reserved = Integer.parseInt(ring.getConfiguration().get(ConfigKey.p1_preexecution_number));
			logger.info("Coordinator p1_preexecution_number: " + reserved);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.p1_resend_time)){
			resend_time = Integer.parseInt(ring.getConfiguration().get(ConfigKey.p1_resend_time));
			logger.info("Coordinator p1_resend_time: " + resend_time);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.multi_ring_delta_t)){
			multi_ring_delta_t = Integer.parseInt(ring.getConfiguration().get(ConfigKey.multi_ring_delta_t));
			logger.info("Coordinator multi_ring_delta_t: " + multi_ring_delta_t);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.multi_ring_lambda)){
			multi_ring_lambda = Integer.parseInt(ring.getConfiguration().get(ConfigKey.multi_ring_lambda));
			logger.info("Coordinator multi_ring_lambda: " + multi_ring_lambda);
		}
	}

	@Override
	public void run() {
		ring.getNetwork().registerCallback(this);
		try { // wait until ring is big enough
			while(ring.getRing().size() < ring.getQuorum()){
				Thread.sleep(1000);
			}
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();			
		}
		
		Thread t = new Thread(new InstanceSkipper(ring,this));
		t.setName("InstanceSkipper");
		t.start();
		
		// phase 1 reserver loop
		while(ring.isNodeCoordinator()){
			try {
				if(fastmode){ // Phase1Range
					while(promises.size() < (reserved/2) && phase1range_in_transit.isEmpty()){
						final int ballot = 10+ring.getNodeID();
						Value v = new Value("",NetworkManager.intToByte(reserved));
						Message m = new Message(instance.incrementAndGet(),ring.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1Range, ballot, v);
						instance.addAndGet(reserved-1);
						phase1range_in_transit.put(m.getInstance(),new Promise(m.getInstance(),m.getBallot()));
						if(ring.getNetwork().getAcceptor() != null){
							ring.getNetwork().getAcceptor().deliver(ring,m);
						}else{ // else should never happen, since there is no coordinator without acceptor!
							ring.getNetwork().send(m);
						}
					}
					long time = System.currentTimeMillis();
					for(Entry<Integer, Promise> e : phase1range_in_transit.entrySet()){
						if(time-e.getValue().getDate()>resend_time){
							instance.addAndGet(-reserved);
							fastmode = false;
							logger.error("Coordinator timeout in phase1range reservation for instance: " + e.getKey());
							logger.debug("Coordinator switch to standard reservation.");
						}
					}
				}else{ // Phase1
					while(promises.size() < (reserved/2) && phase1_in_transit.size() < reserved){
						final int ballot = 10+ring.getNodeID();
						for(int i=0;i<reserved;i++){
							Message m = new Message(instance.incrementAndGet(),ring.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, ballot, null);
							phase1_in_transit.put(m.getInstance(),new Promise(m.getInstance(),m.getBallot()));
							if(ring.getNetwork().getAcceptor() != null){
								ring.getNetwork().getAcceptor().deliver(ring,m);
							}else{ // else should never happen, since there is no coordinator without acceptor!
								ring.getNetwork().send(m);
							}
						}
					}
					long time = System.currentTimeMillis();
					for(Entry<Integer, Promise> e : phase1_in_transit.entrySet()){
						if(time-e.getValue().getDate()>resend_time){ 
							Message m = new Message(e.getKey(),ring.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1,e.getValue().getBallot()+10, null);
							phase1_in_transit.put(m.getInstance(),new Promise(m.getInstance(),m.getBallot()));
							if(ring.getNetwork().getAcceptor() != null){
								ring.getNetwork().getAcceptor().deliver(ring,m);
							}else{ // else should never happen, since there is no coordinator without acceptor!
								ring.getNetwork().send(m);
							}
							logger.error("Coordinator timeout in phase1 reservation for instance: " + e.getKey());
						}
					}
				}
				Thread.sleep(20);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;				
			}
		}
		ring.getNetwork().setLeader(null);
		logger.debug("Coordinator stopped!");
	}

	public synchronized void deliver(RingManager fromRing,Message m){
		/*if(logger.isDebugEnabled()){
			logger.debug("coordinator " + ring.getNodeID() + " received " + m);
		}*/
		if(m.getType() == MessageType.Value){
			value_count++;
			Promise p = null;
			try {
				p = promises.take(); // wait for a promise
			} catch (InterruptedException e) {
			}
			if(p != null){
				int instance = p.getInstance();
				PaxosRole rcv = PaxosRole.Acceptor;
				Message n = new Message(instance,m.getSender(),rcv,MessageType.Phase2,p.getBallot(),m.getValue());
				if(ring.getNetwork().getAcceptor() != null){
					ring.getNetwork().getAcceptor().deliver(ring,n);
				}else{ // else should never happen, since there is no coordinator without acceptor!
					ring.getNetwork().send(n);
				}
			}
		}else if(m.getType() == MessageType.Phase1 && m.getSender() == ring.getNodeID()){
			if(m.getValue() != null){ // instance already decided -> resend 2b
				phase1_in_transit.remove(m.getInstance());
				Message n = new Message(m.getInstance(),m.getSender(),PaxosRole.Acceptor,MessageType.Phase2,m.getBallot(),m.getValue());
				if(ring.getNetwork().getAcceptor() != null){
					ring.getNetwork().getAcceptor().deliver(ring,n);
				}else{ // else should never happen, since there is no coordinator without acceptor!
					ring.getNetwork().send(n);
				}
			}else if(m.getVoteCount() >= ring.getQuorum()){
				Promise p = new Promise(m.getInstance(),m.getBallot());
				promises.add(p);
				if(logger.isDebugEnabled()){
					logger.debug("Coordinator reserved instance " + m.getInstance() + " (Phase1)");
				}
				phase1_in_transit.remove(m.getInstance());
				successful_promise_count++;
				if(successful_promise_count>=enable_fastmode_threashold){
					successful_promise_count = 0;
					fastmode = true;
					logger.debug("Coordinator switch to fastmode reservation.");
				}
			}else{
				logger.error(m +" at ring end without quorum! (" + m.getVoteCount() + ")");
			}
		}else if(m.getType() == MessageType.Phase1Range && m.getSender() == ring.getNodeID()){
			if(m.getVoteCount() >= ring.getQuorum()){
				int n = NetworkManager.byteToInt(m.getValue().getValue());
				for(int i=m.getInstance();i<n+m.getInstance();i++){
					Promise p = new Promise(i,m.getBallot());
					promises.add(p);
				}
				if(logger.isDebugEnabled()){
					logger.debug("Coordinator reserved instance " + m.getInstance() + "-" + (m.getInstance()+reserved-1) + " (Phase1Range)");
				}
				phase1range_in_transit.remove(m.getInstance());
			}else{
				logger.error(m +" at ring end without quorum! (" + m.getVoteCount() + ")");
			}
		}else if(m.getType() == MessageType.Decision){
			if(m.getInstance()>instance.get()){
				instance.set(m.getInstance());
			}
		}
	}
	
	public TransferQueue<Promise> getPromiseQueue(){
		return promises;
	}
}
