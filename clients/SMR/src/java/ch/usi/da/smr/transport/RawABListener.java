package ch.usi.da.smr.transport;
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

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingDescription;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.smr.message.Message;

/**
 * Name: RawABListener<br>
 * Description: <br>
 * 
 * Creation date: Dec 06, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class RawABListener implements ABListener, Runnable {
	
	private Receiver receiver = null;
	
	private final Node paxos;
	
	public RawABListener(String zoo_host, List<RingDescription> rings) throws IOException, KeeperException, InterruptedException {
		paxos = new Node(zoo_host, rings);
		paxos.start();
	}

	@Override
	public void run() {
		while(true) {
			try {
				Decision d = paxos.getLearner().getDecisions().peek();
				if(d != null && receiver.is_ready(d.getRing(),d.getInstance())){ // keep in queue until ready
					d = paxos.getLearner().getDecisions().take();
					if(d != null && d.getValue() != null){					
						Message m = Message.fromDecision(d);
						if(m != null && receiver != null){
							receiver.receive(m);
						}
					}
				}
			} catch (InterruptedException | RuntimeException e) {
				break;
			}
		}
	}

	@Override
	public void safe(int ring, long instance) {
		paxos.getLearner().setSafeInstance(ring, instance);
	}

	@Override
	public void registerReceiver(Receiver receiver){
		this.receiver = receiver; 
	}

	public void close(){
		try {
			paxos.stop();
		} catch (InterruptedException e) {
		}
	}
}
