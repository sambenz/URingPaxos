package ch.usi.da.paxos.thrift;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.ring.LearnerRole;
import ch.usi.da.paxos.ring.RingManager;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.thrift.gen.PaxosLearnerService;
import ch.usi.da.paxos.thrift.gen.Value;

/**
 * Name: PaxosLearnerServiceImpl<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class PaxosLearnerServiceImpl implements PaxosLearnerService.Iface {

	private final static Logger logger = Logger.getLogger(PaxosLearnerServiceImpl.class);
	
	private final BlockingQueue<Decision> values;
	
	public PaxosLearnerServiceImpl(RingManager ring) {
		PaxosRole role = PaxosRole.Learner;
		LearnerRole r = new LearnerRole(ring,true,null);
		values = r.getValues();
		logger.debug("register role: " + role + " at node " + ring.getNodeID() + " in ring " + ring.getRingID());
		ring.registerRole(role);
		Thread t = new Thread(r);
		t.setName(role.toString());
		t.start();
	}

	@Override
	public Value deliver(int timeout) throws TException {
		Value value = null;
		try {
			Decision d = values.poll(timeout,TimeUnit.MILLISECONDS);
			if(d != null){
				value = new Value(ByteBuffer.wrap(d.getValue().getValue()));
			}
		} catch (InterruptedException e) {
		}
		return value == null ? new Value() : value;
	}

	@Override
	public Value nb_deliver() throws TException {
		Value value = null;
		Decision d = values.poll();
		if(d != null){
			value = new Value(ByteBuffer.wrap(d.getValue().getValue()));
		}
		return value == null ? new Value() : value;
	}

}
