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

import ch.usi.da.paxos.api.Learner;
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
 * @author Samuel Benz benz@geoid.ch
 */
public class PaxosLearnerServiceImpl implements PaxosLearnerService.Iface {

	private final static Logger valuelogger = Logger.getLogger(Value.class);

	private final Learner learner;
	
	private final BlockingQueue<Decision> values;
	
	public PaxosLearnerServiceImpl(Learner learner) {
		this.learner = learner;
		values = learner.getDecisions();
	}

	@Override
	public ch.usi.da.paxos.thrift.gen.Decision deliver(int timeout) throws TException {
		ch.usi.da.paxos.thrift.gen.Decision decision = null;
		try {
			Decision d = values.poll(timeout,TimeUnit.MILLISECONDS);
			if(d != null){
				valuelogger.debug(d);
				decision = new ch.usi.da.paxos.thrift.gen.Decision();
				decision.setInstance(d.getInstance());
				decision.setRing(d.getRing());
				Value value = new Value(ByteBuffer.wrap(d.getValue().getValue()));
				if(d.getValue().isSkip()){
					value.setSkip(true);
				}
				decision.setValue(value);
			}
		} catch (InterruptedException e) {
		}
		return decision == null ? new ch.usi.da.paxos.thrift.gen.Decision() : decision;
	}

	@Override
	public ch.usi.da.paxos.thrift.gen.Decision nb_deliver() throws TException {
		ch.usi.da.paxos.thrift.gen.Decision decision = null;
		Decision d = values.poll();
		if(d != null){
			valuelogger.debug(d);
			decision = new ch.usi.da.paxos.thrift.gen.Decision();
			decision.setInstance(d.getInstance());
			decision.setRing(d.getRing());
			Value value = new Value(ByteBuffer.wrap(d.getValue().getValue()));
			decision.setValue(value);
		}
		return decision == null ? new ch.usi.da.paxos.thrift.gen.Decision() : decision;
	}

	@Override
	public void safe(int ring,long instance) throws TException {
		learner.setSafeInstance(ring,instance);
	}

}
