package ch.usi.da.paxos.ring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.storage.FutureDecision;
import ch.usi.da.paxos.storage.Proposal;

/**
 * Name: ProposerRole<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class ProposerRole extends Role {
	
	private final static Logger logger = Logger.getLogger(ProposerRole.class);

	private final static Logger stats = Logger.getLogger("ch.usi.da.paxos.Stats");
	
	private final RingManager ring;
	
	private int concurrent_values = 20;
	
	private int value_size = 8912;
	
	private int value_count = 900000;
	
	private final Map<String,Proposal> proposals = new ConcurrentHashMap<String,Proposal>();

	private final Map<String,FutureDecision> futures = new ConcurrentHashMap<String,FutureDecision>();
	
	private long send_count = 0;
	
	private final boolean service;
	
	private boolean test = false;
	
	private final List<Long> latency = new ArrayList<Long>();

	/**
	 * @param ring 
	 */
	public ProposerRole(RingManager ring) {
		this(ring,false);
	}
	
	/**
	 * @param ring 
	 * @param service
	 */
	public ProposerRole(RingManager ring,boolean service) {
		this.ring = ring;
		this.service = service;
		if(ring.getConfiguration().containsKey(ConfigKey.concurrent_values)){
			concurrent_values = Integer.parseInt(ring.getConfiguration().get(ConfigKey.concurrent_values));
			logger.info("Proposer concurrent_values: " + concurrent_values);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.value_size)){
			value_size = Integer.parseInt(ring.getConfiguration().get(ConfigKey.value_size));
			logger.info("Proposer value_size: " + value_size);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.value_count)){
			value_count = Integer.parseInt(ring.getConfiguration().get(ConfigKey.value_count));
			logger.info("Proposer value_count: " + value_count);
		}
	}

	@Override
	public void run() {
		ring.getNetwork().registerCallback(this);
		Thread t = new Thread(new ProposerResender(this));
		t.setName("ProposerResender");
		t.start();
		if(!service){
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		    String s;
		    try {
			    while ((s = in.readLine()) != null && s.length() != 0){
		    		if(s.contains("start")){
		    			test = true;
    					for(int i=0;i<concurrent_values;i++){
	    					Value v = new Value(System.nanoTime() + "" + ring.getNodeID(),new byte[value_size]);
		    				send(new Message(0,ring.getNodeID(),PaxosRole.Leader,MessageType.Value,0,v));			    						
    					}
			    	}else{
			    		Value v = new Value(System.nanoTime() + "" + ring.getNodeID(),s.getBytes());
		    			send(new Message(0,ring.getNodeID(),PaxosRole.Leader,MessageType.Value,0,v));
			    	}
			    }
			    in.close();
			    System.exit(0); // used to stop properly in eclipse
		    }catch(IOException e){
		    	logger.error(e);
		    }
		}
	}

	/**
	 * Use this method if you propose byte[] from outside!
	 * 
	 * @param b A byte array which will proposed in a paxos instance
	 * @return A FutureDecision object on which you can wait until the value is proposed
	 */
	public FutureDecision propose(byte[] b){
		Value v = new Value(System.nanoTime() + "" + ring.getNodeID(),b);
		FutureDecision future = new FutureDecision();
		futures.put(v.getID(),future);		
		send(new Message(0,ring.getNodeID(),PaxosRole.Leader,MessageType.Value,0,v));
		return future;
	}
	
	/**
	 * @param m
	 */
	public void send(Message m){
		send_count++;
		proposals.put(m.getValue().getID(),new Proposal(m.getValue()));
		if(ring.getNetwork().getLearner() != null){
			ring.getNetwork().getLearner().deliver(ring,m);
		}
		if(ring.getNetwork().getAcceptor() != null){
			ring.getNetwork().getAcceptor().deliver(ring,m);
		}
		if(ring.getNetwork().getLeader() != null){
			ring.getNetwork().getLeader().deliver(ring,m);
		}else{
			ring.getNetwork().send(m);
		}
	}

	public void deliver(RingManager fromRing,Message m){
		/*if(logger.isDebugEnabled()){
			logger.debug("proposer " + ring.getNodeID() + " received " + m);
		}*/
		if(m.getType() == MessageType.Decision && m.getSender() == ring.getNodeID()){
			String ID = m.getValue().getID();
			if(proposals.containsKey(ID)){
				Proposal p = proposals.get(ID);
				Value v = p.getValue();
				if(m.getValue().equals(v)){ // compared by ID
					if(futures.containsKey(ID)){
						FutureDecision f = futures.get(ID);
						f.setDecision(new Decision(m.getInstance(),m.getBallot(),v));
						futures.remove(ID);
					}
					if(test){
						long time = System.nanoTime();
						long send_time = Long.valueOf(ID.substring(0,ID.length()-1)); // since ID == nano-time + ring-id
						long lat = time - send_time;
						latency.add(lat);
						if(send_count < value_count){
							Value v2 = new Value(System.nanoTime() + "" + ring.getNodeID(),new byte[value_size]);
							send(new Message(0,ring.getNodeID(),PaxosRole.Leader,MessageType.Value,0,v2));
						}else{
							printHistogram();
							test = false;
						}
					}
					/*if(!test && logger.isDebugEnabled()){
						long time = System.nanoTime();
						long send_time = Long.valueOf(ID.substring(0,ID.length()-1)); // since ID == nano-time + ring-id
						long lat = time - send_time;
						logger.debug("Value " + v + " proposed and learned in " + lat + " ns (@proposer)");
					}*/
				}else{
					logger.error("Proposer received Decision with different values for same instance " + m.getInstance() + "!");
				}
				proposals.remove(ID);
			}
		}
	}
	
	/**
	 * @return the open proposals
	 */
	public Map<String, Proposal> getProposals(){
		return proposals;
	}
	
	/**
	 * @return the ring manager
	 */
	public RingManager getRingManager(){
		return ring;
	}
	
	private void printHistogram(){
		Map<Long,Long> histogram = new HashMap<Long,Long>();
		int a = 0,b = 0,b2 = 0,c = 0,d = 0,e = 0,f = 0;
		long sum = 0;
		for(Long l : latency){
			sum = sum + l;
			if(l < 1000000){ // <1ms
				a++;
			}else if (l < 10000000){ // <10ms
				b++;
			}else if (l < 25000000){ // <25ms
				b2++;
			}else if (l < 50000000){ // <50ms
				c++;
			}else if (l < 75000000){ // <75ms
				f++;
			}else if (l < 100000000){ // <100ms
				d++;
			}else{
				e++;
			}
			Long key = new Long(Math.round(l/1000/1000));
			if(histogram.containsKey(key)){
				histogram.put(key,histogram.get(key)+1);
			}else{
				histogram.put(key,1L);
			}
		}
		float avg = (float)sum/latency.size()/1000/1000;
		logger.info("proposer latency histogram: <1ms:" + a + " <10ms:" + b + " <25ms:" + b2 + " <50ms:" + c + " <75ms:" + f + " <100ms:" + d + " >100ms:" + e + " avg:" + avg);
		if(stats.isDebugEnabled()){
			for(Entry<Long, Long> bin : histogram.entrySet()){
				stats.debug(bin.getKey() + "," + bin.getValue());
			}
		}
	}
}
