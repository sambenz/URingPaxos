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
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.storage.Proposal;

/**
 * Name: ProposerResender<br>
 * Description: <br>
 * 
 * Creation date: Sep 14, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ProposerResender implements Runnable {
	
	private final static Logger logger = Logger.getLogger(ProposerResender.class);
	
	private final ProposerRole proposer;
	
	private int resend_time = 3000;
	
	/**
	 * @param proposer
	 */
	public ProposerResender(ProposerRole proposer) {
		this.proposer = proposer;
		if(proposer.getRingManager().getConfiguration().containsKey(ConfigKey.value_resend_time)){
			resend_time = Integer.parseInt(proposer.getRingManager().getConfiguration().get(ConfigKey.value_resend_time));
			logger.info("Proposer value_resend_time: " + resend_time);
		}
	}

	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(200);
				long time = System.currentTimeMillis();
				Iterator<Entry<String, Proposal>> i = proposer.getProposals().entrySet().iterator();
				while(i.hasNext()){
					Entry<String, Proposal> e = i.next();
					if(time-e.getValue().getDate()>resend_time){
						i.remove();
						logger.error("Proposer timeout in proposing value: " + e.getValue().getValue());
						proposer.send(new Message(0,proposer.getRingManager().getNodeID(),PaxosRole.Leader,MessageType.Value,0,0,e.getValue().getValue()));
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;				
			}
		}
	}

}
