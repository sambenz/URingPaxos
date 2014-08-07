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

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import ch.usi.da.paxos.api.Proposer;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.storage.FutureDecision;
import ch.usi.da.paxos.thrift.gen.PaxosProposerService;
import ch.usi.da.paxos.thrift.gen.Value;

/**
 * Name: PaxosProposerServiceImpl<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class PaxosProposerServiceImpl implements PaxosProposerService.Iface {

	private final static Logger logger = Logger.getLogger(PaxosProposerServiceImpl.class);
	
	private final Proposer proposer;
	
	public PaxosProposerServiceImpl(Proposer proposer) {
		this.proposer = proposer;
	}

	@Override
	public long propose(Value value) throws TException {
		byte[] b = new byte[value.cmd.remaining()];
		value.cmd.get(b);
		FutureDecision f = proposer.propose(b);
		try {
			Decision d = f.getDecision(3000);
			if(d != null){
				logger.debug("TrhiftProposer proposed and " + d);
				return d.getInstance();
			}
		} catch (InterruptedException e) {
			logger.error(e);
		}
		return -1;
	}

	@Override
	public void nb_propose(Value value) throws TException {
		byte[] b = new byte[value.cmd.remaining()];
		value.cmd.get(b);
		proposer.propose(b);
	}

}
