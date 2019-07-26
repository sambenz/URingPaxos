package ch.usi.da.dmap.server;
/* 
 * Copyright (c) 2017 Università della Svizzera italiana (USI)
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

import ch.usi.da.paxos.storage.Decision;

/**
 * Name: ABReceiver<br>
 * Description: <br>
 * 
 * Creation date: Mar 03, 2017<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ABReceiver implements Runnable {
	
	private final static Logger logger = Logger.getLogger(ABReceiver.class);
	
	private final DMapReplica<?,?> replica;
	
	public ABReceiver(DMapReplica<?,?> replica){
		this.replica = replica;
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				Decision d = replica.getNode().getLearner().getDecisions().take();
				logger.trace("ABReceiver received " + d);
				if(d != null){					
					replica.receive(d);
				}
			} catch (InterruptedException | RuntimeException e) {
				break;
			}
		}
	}

}
