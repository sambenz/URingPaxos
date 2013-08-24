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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

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
 * @author Samuel Benz <benz@geoid.ch>
 */
public class LearnerRole extends Role implements Learner {

	private final static Logger logger = Logger.getLogger(LearnerRole.class);
		
	private final RingManager ring;
	
	private final Map<String,Value> learned = new ConcurrentHashMap<String,Value>();
	
	private final LinkedList<Decision> delivery = new LinkedList<Decision>();
	
	private final BlockingQueue<Decision> values = new LinkedBlockingQueue<Decision>();
	
	private final CountDownLatch latch; // used to synchronize multi-learner start
	
	private int next_instance = 1; // only needed to optimize linked list insert
	
	private int delivered_instance = 0;
	
	private int safe_instance = 0;
	
	private int highest_online_instance = 0;
	
	private boolean auto_trim = false; // for testing purpose (safe_instance = delivered_instance)
	
	private boolean recovery = false;
	
	private boolean recovered = false;
			
	public long deliver_count = 0;
	
	public long deliver_bytes = 0;

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
		while(recovery){
			try{
				// init recovering by getting highest available instance
				if(!recovered && !delivery.isEmpty()){
					Message m = new Message(0,ring.getNodeID(),PaxosRole.Leader,MessageType.Safe,0,new Value("","0".getBytes()));
					m.setVoteCount(1);
					ring.getNetwork().send(m);
				}
				// re-request missing decisions
				else if(!delivery.isEmpty() && delivery.peek().getInstance() > delivered_instance){
					Decision head = delivery.peek();
					for(int i=delivered_instance+1;i<head.getInstance();i++){
						Message m = new Message(i,ring.getRingSuccessor(ring.getNodeID()),PaxosRole.Acceptor,MessageType.Phase2,new Integer(9999),new Value(System.currentTimeMillis()+ "" + ring.getNodeID(),new byte[0]));
						ring.getNetwork().send(m);
						logger.debug("Learner re-request missing instance " + i);
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
			if(d != null){
				if(!recovery){
					deliver_count++;
					deliver_bytes = deliver_bytes + d.getValue().getValue().length;
					if(auto_trim) { safe_instance = d.getInstance(); }
					values.add(d);
				}else{
					if(d.getInstance() == next_instance){
						delivery.add(d);
						next_instance++;
					}else{
						int pos = findPos(d.getInstance());
						if(pos >= 0){
							delivery.add(pos,d);
							if(pos == delivery.size()-1){
								next_instance = d.getInstance().intValue()+1;
							}
						}
					}
				}
				// deliver sorted instances
				while(!delivery.isEmpty() && delivery.peek().getInstance()-1 <= delivered_instance){
					if(delivery.peek().getInstance()-1 == delivered_instance){
						recovered = true;
						Decision de = delivery.poll();
						delivered_instance = de.getInstance();
						if(auto_trim) { safe_instance = delivered_instance; }
						deliver_count++;
						deliver_bytes = deliver_bytes + de.getValue().getValue().length;
						values.add(de);
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
			Message n = new Message(m.getInstance(),m.getSender(),m.getReceiver(),m.getType(),m.getBallot(),v);
			n.setVoteCount(m.getVoteCount()+1);
			ring.getNetwork().send(n);
		}else if(m.getType() == MessageType.Trim){
			highest_online_instance = m.getInstance();
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
	
	@Override
	public void setSafeInstance(Integer ring,Integer instance){
		safe_instance = instance.intValue();
	}
	
	private int findPos(int instance) {
		int pos = 0;
		Iterator<Decision> i = delivery.iterator();
		while(i.hasNext()){
			Decision d = i.next();
			if(instance == d.getInstance().intValue()){
				return -1;
			}else if(instance < d.getInstance().intValue()){
				return pos;
			}
			pos++;
		}
		return pos;
	}

}
