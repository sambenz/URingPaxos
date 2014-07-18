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

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
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
	
	private final long boot_time;
	
	public InstanceSkipper(RingManager ring,CoordinatorRole coordinator) {
		this.coordinator = coordinator;
		this.ring = ring;
		if(ring.getConfiguration().containsKey(ConfigKey.multi_ring_start_time)){
			this.boot_time = Long.parseLong(ring.getConfiguration().get(ConfigKey.multi_ring_start_time));
		}else{
			this.boot_time = 0;
		}
		logger.info("InstanceSkipper use boot time: " + boot_time);
	}
	
	@Override
	public void run() {
		if(coordinator.multi_ring_lambda>0){
		while(true){
				try {
					long time = System.currentTimeMillis();
					int latency_compensation = coordinator.latency_compensation;
					long valueSent = coordinator.value_count.get();
					float executionTime = ((float)(time-boot_time-latency_compensation)) / 1000.0f;
					long expectedValues = (long) ((coordinator.multi_ring_lambda) * executionTime);
					long skip = expectedValues - valueSent;
					if(skip > 0) {
						if(logger.isDebugEnabled()){
							logger.debug(String.format("skip %d values", skip));
						}
						Promise p = null;
						try {
							p = coordinator.getPromiseQueue().take(); // wait for a promise
						} catch (InterruptedException e) {
						}
						//send Phase2 with skip value
						if(p != null){
							Value v = new Value(Value.getSkipID(),Long.toString(skip).getBytes());
							coordinator.value_count.addAndGet(skip);
							Message m = new Message(p.getInstance(),ring.getNodeID(),PaxosRole.Acceptor,MessageType.Phase2,p.getBallot(),0,v);
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
					Thread.sleep(coordinator.multi_ring_delta_t);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;				
				}
			}
		}
	}

}
