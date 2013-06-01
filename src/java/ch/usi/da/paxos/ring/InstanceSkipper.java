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

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.Promise;

/**
 * Name: InstanceSkipper<br>
 * Description: <br>
 * 
 * Creation date: Mar 08, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class InstanceSkipper implements Runnable {

	private final static Logger logger = Logger.getLogger(InstanceSkipper.class);
	
	private final CoordinatorRole coordinator;
	
	private final RingManager ring;
	
	private long last_time = System.nanoTime();
	
	private long last_value_count = 0;
		
	public InstanceSkipper(RingManager ring,CoordinatorRole coordinator) {
		this.coordinator = coordinator;
		this.ring = ring;
	}
	
	@Override
	public void run() {
		if(coordinator.multi_ring_lambda>0){
		while(true){
				try {
					long time = System.nanoTime();
					long value_count = coordinator.value_count - last_value_count;
					float t = (float)(time-last_time)/(1000*1000*1000);
					int skip = (int)(coordinator.multi_ring_lambda-((float)value_count/t));
					if(skip > 0) {
						if(logger.isDebugEnabled()){
							logger.debug(String.format("skip %d values (%.1f/s)",skip,value_count/t));
						}
						Promise p = null;
						try {
							p = coordinator.getPromiseQueue().take(); // wait for a promise
						} catch (InterruptedException e) {
						}
						//send Phase2 with skip value
						if(p != null){
							Value v = new Value(Value.skipID,NetworkManager.intToByte(skip));
							coordinator.value_count = coordinator.value_count + skip;
							Message m = new Message(p.getInstance(),ring.getNodeID(),PaxosRole.Acceptor,MessageType.Phase2,p.getBallot(),v);
							if(ring.getNetwork().getLearner() != null){
								ring.getNetwork().getLearner().deliver(ring,m);
							}
							if(ring.getNetwork().getAcceptor() != null){
								ring.getNetwork().getAcceptor().deliver(ring,m);
							}else{ // else should never happen, since there is no coordinator without acceptor!
								ring.getNetwork().send(m);
							}
						}
					}
					
					last_value_count = last_value_count + value_count;
					last_time = time;
					Thread.sleep(coordinator.multi_ring_delta_t);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;				
				}
			}
		}
	}

}
