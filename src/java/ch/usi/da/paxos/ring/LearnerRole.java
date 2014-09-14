package ch.usi.da.paxos.ring;
/* 
 * Copyright (c) 2013 Università della Svizzera italiana (USI)
 * 
 * This file is part of URingPaxos.
 *
 * URingPaxos is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * URingPaxos is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with URingPaxos.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.ConfigKey;
import ch.usi.da.paxos.api.Learner;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.Decision;

/**
 * Name: LearnerRole<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class LearnerRole extends Role implements Learner {

	private final static Logger logger = Logger.getLogger(LearnerRole.class);
		
	private final RingManager ring;
	
	private final Map<String,Value> learned = new ConcurrentHashMap<String,Value>();
	
	private final LinkedList<Decision> delivery = new LinkedList<Decision>();
	
	private final BlockingQueue<Decision> values = new LinkedBlockingQueue<Decision>();
	
	private final CountDownLatch latch; // used to synchronize multi-learner start
	
	// at most once delivery for the most recent 500k Values
	Set<String> delivered = Collections.newSetFromMap(new LinkedHashMap<String, Boolean>(){
		private static final long serialVersionUID = -5679181663800465934L;
		protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
	        return size() > 500000;
	    }
	});
		
	private long next_instance = 1; // only needed to optimize linked list insert
	
	private long delivered_instance = 0;
	
	private long safe_instance = 0;
	
	private long highest_online_instance = 0;
	
	private boolean auto_trim = false; // for testing purpose (safe_instance = delivered_instance)
	
	private boolean recovery = false;
	
	private volatile boolean recovered = false;
			
	public long deliver_count = 0;
	
	public long batch_count = 0;
	
	public long deliver_bytes = 0;
	
	public volatile int latency_to_coordinator = 0;

	private final int median_window = 1000;

	private DescriptiveStatistics latencies = new DescriptiveStatistics(median_window);

	/**
	 * @param ring
	 */
	public LearnerRole(RingManager ring) {
		this(ring, null);
	}
	
	/**
	 * @param ring
	 * @param latch
	 */
	public LearnerRole(RingManager ring, CountDownLatch latch) {
		this.ring = ring;
		this.latch = latch;
		if(ring.getConfiguration().containsKey(ConfigKey.learner_recovery)){
			if(ring.getConfiguration().get(ConfigKey.learner_recovery).equals("1")){
				recovery = true;
			}
			logger.info("Learner recovery: " + (recovery ? "enabled" : "disabled"));
		}
		if(ring.getConfiguration().containsKey(ConfigKey.auto_trim)){
			if(ring.getConfiguration().get(ConfigKey.auto_trim).equals("1")){
				auto_trim = true;
			}
			logger.info("Learner auto_trim: " + (auto_trim ? "enabled" : "disabled"));
		}
	}

	@Override
	public void run() {		
		ring.getNetwork().registerCallback(this);
		Thread t = new Thread(new LearnerStatsWriter(ring,this));
		t.setName("LearnerStatsWriter");
		t.start();
		if(latch != null){
			latch.countDown();
		}
		while(true){
			try{
				Decision head = delivery.peek();
				// initial recovering
				if(!recovered && recovery && head != null){
					Message m = new Message(0,ring.getNodeID(),PaxosRole.Leader,MessageType.Safe,0,0,new Value("","0".getBytes()));
					m.setVoteCount(1);
					logger.debug("Send safe message to recover highest_online_instance. (" + recovered + "," + delivery.isEmpty() + ")");
					ring.getNetwork().send(m);
				}
				// re-request missing decisions
				else if(head != null && head.getInstance() > delivered_instance){
					for(long i=delivered_instance+1;i<head.getInstance();i++){
						if(highest_online_instance == 0 || i >= highest_online_instance){
							Message m = new Message(i,ring.getNodeID(),PaxosRole.Leader,MessageType.Relearn,0,0,null);
							logger.debug("Learner re-request missing instance " + i);
							ring.getNetwork().receive(m);
						}else{
							recovered = false;
						}
					}
				}
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;				
			}
		}
	}
	
	public void deliver(RingManager fromRing,Message m){
		/*if(logger.isDebugEnabled()){
			logger.debug("learner " + ring.getNodeID() + " received " + m);
		}*/
		if(m.getType() == MessageType.Decision){
			Decision d = null;
			if(learned.get(m.getValue().getID()) != null){
				// value was previously learned with an other message
				d = new Decision(fromRing.getRingID(),m.getInstance(),m.getBallot(),learned.get(m.getValue().getID()));
				learned.remove(m.getValue().getID());
			}else{
				d = new Decision(fromRing.getRingID(),m.getInstance(),m.getBallot(),m.getValue());
			}
			if(d != null && d.getValue().getValue().length > 0){
				if(!recovered && !recovery){
					next_instance = d.getInstance();
					delivered_instance = d.getInstance()-1;
					logger.info("Learner start from instance " + d.getInstance() + " without recovery");
				}
				if(d.getInstance() == next_instance){
					delivery.add(d);
					next_instance++;
				}else{
					int pos = findPos(d.getInstance());
					if(pos >= 0){
						delivery.add(pos,d);
						if(pos == delivery.size()-1){
							next_instance = d.getInstance().longValue()+1;
						}
					}
				}
				// deliver sorted instances
				Decision head = null;
				while((head = delivery.peek()) != null && head.getInstance()-1 <= delivered_instance){
					if(head.getInstance()-1 == delivered_instance){
						recovered = true;
						Decision de = delivery.poll();
						delivered_instance = de.getInstance();
						if(auto_trim) { safe_instance = delivered_instance; }
						deliver_bytes = deliver_bytes + de.getValue().getValue().length;
						if(de.getValue().isSkip()){
							try {
								if(logger.isTraceEnabled()){
									logger.trace("Learner received SKIP " + de.getRing() + " " + de.getInstance() + " " + new String(de.getValue().getValue()));
								}
								latencies.addValue(System.currentTimeMillis() - Long.parseLong(d.getValue().getID().split(":")[1]));
								if(latencies.getN() >= median_window){
									latency_to_coordinator = (int) latencies.getPercentile(50);
								}
							}catch(Exception e){
							}
						}
						if(de.getValue().isBatch()){
							batch_count++;
							ByteBuffer buffer = ByteBuffer.wrap(de.getValue().getValue());
							while(buffer.remaining() > 0){
								try {
									Message n = Message.fromBuffer(buffer);
									// decision from a batch will contain same instance number for all entries !!!
									Decision bd = new Decision(fromRing.getRingID(),m.getInstance(),m.getBallot(),n.getValue());
									deliver_count++;
									if(!delivered.contains(bd.getValue().getID())){
										values.add(bd);
										delivered.add(bd.getValue().getID());
									}
								} catch (Exception e) {
									logger.error("Learner could not de-serialize batch message!" + e);
								}
							}
						}else{
							deliver_count++;
							if(!delivered.contains(de.getValue().getID())){
								values.add(de);
								delivered.add(de.getValue().getID());
							}
						}
					}else{
						delivery.poll(); // remove duplicate
					}
				}
			}
		}else if(m.getType() == MessageType.Safe){
			Value v = null;
			if(m.getValue().getValue().length == 0){
				v = new Value(m.getValue().getID(),String.valueOf(safe_instance).getBytes());
			}else{
				String s = new String(m.getValue().getValue());
				v = new Value(m.getValue().getID(),(s + ";" + String.valueOf(safe_instance)).getBytes());
			}
			Message n = new Message(m.getInstance(),m.getSender(),m.getReceiver(),m.getType(),m.getBallot(),m.getBallot(),v);
			n.setVoteCount(m.getVoteCount()+1);
			ring.getNetwork().send(n);
		}else if(m.getType() == MessageType.Trim){
			highest_online_instance = m.getInstance()+1;
			if(!recovered){ // recover only what is available
				delivered_instance = highest_online_instance == 0 ? highest_online_instance : highest_online_instance-1;
				recovered = true;
			}
			logger.debug("Learner notified last highest_online_instance: " + highest_online_instance);
		}else{
			if(learned.get(m.getValue().getID()) == null){
				learned.put(m.getValue().getID(),m.getValue());
			}
		}		
	}
	
	@Override
	public BlockingQueue<Decision> getDecisions(){
		return values;
	}
	
	public Map<String, Value> getLearned(){
		return Collections.unmodifiableMap(learned);
	}
	
	@Override
	public void setSafeInstance(Integer ring,Long instance){
		if(instance <= delivered_instance){
			safe_instance = instance.longValue();
		}else{
			logger.error("Learner setSafeInstance() for not delivered instance number?!");
		}
	}
	
	private int findPos(long instance) {
		int pos = 0;
		Iterator<Decision> i = delivery.iterator();
		while(i.hasNext()){
			Decision d = i.next();
			if(instance == d.getInstance().longValue()){
				return -1;
			}else if(instance < d.getInstance().longValue()){
				return pos;
			}
			pos++;
		}
		return pos;
	}
}
