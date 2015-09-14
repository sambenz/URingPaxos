package ch.usi.da.paxos.ring;
/* 
 * Copyright (c) 2015 Università della Svizzera italiana (USI)
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.ConfigKey;
import ch.usi.da.paxos.api.Learner;
import ch.usi.da.paxos.api.PaxosNode;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.storage.Decision;

/**
 * Name: ElasticLearnerRole<br>
 * Description: <br>
 * 
 * Creation date: Sept 08, 2015<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ElasticLearnerRole extends Role implements Learner {

	private final static Logger logger = Logger.getLogger(ElasticLearnerRole.class);
	
	private final PaxosNode node;
	
	private final int replication_group;
	
	private final Map<Integer,RingDescription> ringmap = new HashMap<Integer,RingDescription>();
	
	private final List<Integer> rings = new ArrayList<Integer>();
	
	private final int maxRing = 100;
	
	private final BlockingQueue<Decision> values = new LinkedBlockingQueue<Decision>(); 
	
	private final LearnerRole[] learner = new LearnerRole[maxRing];
	
	private int deliverRing;
	
	private int minRing;
	
	private final long[] skip_count = new long[maxRing];

	private final long[] v_count = new long[maxRing];

	private long v_subscribe = Long.MAX_VALUE;
	
	private boolean deliver_skip_messages = false;

	/**
	 * @param initial_ring the RingDescription of the initial ring
	 */
	public ElasticLearnerRole(RingDescription initial_ring) {
		RingManager rm = initial_ring.getRingManager();
		node = rm.getPaxosNode();
		replication_group = node.getGroupID();
		rings.add(rm.getRingID());
		ringmap.put(rm.getRingID(),initial_ring);
		deliverRing = rm.getRingID();
		logger.debug("ElasticLearnerRole initial ring=" + deliverRing);
		if(rm.getConfiguration().containsKey(ConfigKey.deliver_skip_messages)){
			if(rm.getConfiguration().get(ConfigKey.deliver_skip_messages).contains("1")){
				deliver_skip_messages = true;
			}
			logger.info("ElasticLearnerRole deliver_skip_messages: " + (deliver_skip_messages ? "enabled" : "disabled"));
		}
	}

	@Override
	public void run() {
		// create initial learner
		int initial_ring = rings.get(0);
		minRing = initial_ring;
		startLearner(initial_ring);
		int count = 0;
		while(true){
			try{
				if(skip_count[deliverRing] > 0){
					count++;
					v_count[deliverRing]++;
					skip_count[deliverRing]--;
					//logger.debug("ElasticLearnerRole " + ringmap.get(deliverRing).getNodeID() + " ring " + deliverRing + " skiped a value (" + skip_count[deliverRing] + " skips left)");
				}else{
					Decision d = learner[deliverRing].getDecisions().take();
					if(d.getValue() != null && d.getValue().isSubscribe()){
						v_count[deliverRing]++;
						// subscribe message
						try {
							String[] token = d.getValue().asString().split(",");
							int group = Integer.parseInt(token[0]);
							int newring = Integer.parseInt(token[1]);
							logger.info("ElasticLearner received subscribe: " + newring + " for group " + group);
							if(learner[newring] == null && replication_group == group){
								v_subscribe = Long.MAX_VALUE;
								List<PaxosRole> rl = new ArrayList<PaxosRole>();
								rl.add(PaxosRole.Learner);
								RingDescription rd = new RingDescription(newring,rl);
								ringmap.put(newring,rd);
								if(!node.updateRing(rd)){
									logger.error("ElasticLearnerRole failed to create Learner in ring " + newring);
								}
								rings.add(newring);
								startLearner(newring);
								while(true){
									Decision d2 = learner[newring].getDecisions().take();
									v_count[newring]++; //TODO: FIXME: What if a skip was inside? How do I know offset after trim? 
									if(d2 != null && d2.getValue().isSubscribe()){
										if(d.getValue().asString().equals(d2.getValue().asString())){
											break;
										}
									}
								}
								long maxPosition = 0;
								for(Integer r : rings){
									if(v_count[r] > maxPosition){
										maxPosition = v_count[r];
									}
								}
								v_subscribe  = maxPosition;
								minRing = minRing(rings); // the ring to start RR after v_subscribe
								logger.info("ElasticLearner subscribe to ring " + newring + " in group " + group + " at position " + v_subscribe+1);								
							}
						}catch (NumberFormatException e) {
							logger.error("ElasticLearnerRole received incomplete subscribe message! -> " + d,e);
						}
					}else if(d.getValue() != null && d.getValue().isSkip()){
						// skip message
						try {
							long skip = Long.parseLong(new String(d.getValue().getValue()));
							skip_count[deliverRing] = skip_count[deliverRing] + skip;
						}catch (NumberFormatException e) {
							logger.error("ElasticLearnerRole received incomplete SKIP message! -> " + d,e);
						}
						if(deliver_skip_messages){
							values.add(d);
						}
					}else{
						count++;
						v_count[deliverRing]++;
						// learning an actual proposed value
						values.add(d);
					}
				}
				if(count >= 1){ // removed variable M
					count = 0;
					deliverRing = getRingSuccessor(deliverRing);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;				
			}
		}
	}

	private void startLearner(int ringID){
		RingManager ring = ringmap.get(ringID).getRingManager();
		Role r = new LearnerRole(ring);
		learner[ringID] = (LearnerRole) r;
		logger.debug("ElasticLearnerRole register role: " + PaxosRole.Learner + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
		ring.registerRole(PaxosRole.Learner);		
		Thread t = new Thread(r);
		t.setName(PaxosRole.Learner + "-" + ringID);
		t.start();
		skip_count[ringID] = 0;
	}
	
	private int getRingSuccessor(int id){
		if(v_subscribe > v_count[minRing]){ // skip
			return id;
		}else if(v_subscribe == v_count[minRing]){ // start round-robin
			v_subscribe = 0;
			return minRing(rings);
		}else{ // round-robin
			int pos = rings.indexOf(new Integer(id));
			if(pos+1 >= rings.size()){
				return rings.get(0);
			}else{
				return rings.get(pos+1);
			}
		}
	}

	private int minRing(List<Integer> rings) {
		int min = Integer.MAX_VALUE;
		for(int i : rings){
			if(i < min){
				min = i;
			}
		}
		return min;
	}
	
	@Override
	public BlockingQueue<Decision> getDecisions() {
		return values;
	}

	public void setSafeInstance(Integer ring, Long instance) {
		learner[ring].setSafeInstance(ring,instance);
	}

}
