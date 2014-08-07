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
import ch.usi.da.smr.message.Message;

/**
 * Name: RawABSender<br>
 * Description: <br>
 * 
 * Creation date: Dec 13, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class RawABSender implements ABSender {

	private final Node paxos;

	private final int ring;
	
	public RawABSender(String zoo_host, List<RingDescription> rings) throws IOException, KeeperException, InterruptedException {
		paxos = new Node(zoo_host, rings);
		ring = rings.get(0).getRingID();
		paxos.start();
	}
			
	@Override
	public long abroadcast(Message m){
		paxos.getProposer(ring).propose(Message.toByteArray(m));
		return 1;
	}
	
	@Override
	public void close(){
		try {
			paxos.stop();
		} catch (InterruptedException e) {
		}
	}
}
