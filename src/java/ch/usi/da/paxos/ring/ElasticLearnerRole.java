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
	
	private final Map<Integer,RingDescription> ringmap = new HashMap<Integer,RingDescription>();
	
	private final List<Integer> rings = new ArrayList<Integer>();
	
	private final int maxRing = 20;
	
	private final BlockingQueue<Decision> values = new LinkedBlockingQueue<Decision>(); 
	
	private final LearnerRole[] learner = new LearnerRole[maxRing];
	
	private int deliverRing;
	
	private final long[] skip_count = new long[maxRing];
	
	private boolean deliver_skip_messages = false;

	/**
	 * @param initial_ring the RingDescription of the initial ring
	 */
	public ElasticLearnerRole(RingDescription initial_ring) {
		RingManager rm = initial_ring.getRingManager();
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
		RingManager ring = ringmap.get(initial_ring).getRingManager();
		Role r = new LearnerRole(ring);
		learner[initial_ring] = (LearnerRole) r;
		logger.debug("ElasticLearnerRole register role: " + PaxosRole.Learner + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
		ring.registerRole(PaxosRole.Learner);		
		Thread t = new Thread(r);
		t.setName(PaxosRole.Learner + "-" + initial_ring);
		t.start();
		skip_count[initial_ring] = 0;
		int count = 0;
		while(true){
			try{
				if(skip_count[deliverRing] > 0){
					count++;
					skip_count[deliverRing]--;
					//logger.debug("ElasticLearnerRole " + ringmap.get(deliverRing).getNodeID() + " ring " + deliverRing + " skiped a value (" + skip_count[deliverRing] + " skips left)");
				}else{
					Decision d = learner[deliverRing].getDecisions().take();
					if(d.getValue() != null && d.getValue().isSkip()){
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
						// learning an actual proposed value
						values.add(d);
					}
				}
				if(count >= 1){
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
		int pos = rings.indexOf(new Integer(id));
		if(pos+1 >= rings.size()){
			return rings.get(0);
		}else{
			return rings.get(pos+1);
		}
	}

	@Override
	public BlockingQueue<Decision> getDecisions() {
		return values;
	}

	public void setSafeInstance(Integer ring, Long instance) {
		learner[ring].setSafeInstance(ring,instance);
	}

}
