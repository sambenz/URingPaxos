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

import java.util.Collections;
import java.util.List;

import ch.usi.da.paxos.api.PaxosRole;


/**
 * Name: Ring<br>
 * Description: <br>
 * 
 * Creation date: Mar 04, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class RingDescription {

	private final int ringID;
	
	private final int nodeID;
	
	private final List<PaxosRole> roles;
	
	private RingManager ring;
	
	public RingDescription(int ringID,int nodeID,List<PaxosRole> roles){
		this.ringID = ringID;
		this.nodeID = nodeID;
		this.roles = roles;
	}

	public int getRingID() {
		return ringID;
	}

	public int getNodeID() {
		return nodeID;
	}

	public List<PaxosRole> getRoles() {
		return Collections.unmodifiableList(roles);
	}
	
	public synchronized void setRingManager(RingManager ring){
		this.ring = ring;
	}
	
	public synchronized RingManager getRingManager(){
		return ring;
	}
	
	public String toString(){
		return "ringID:" + ringID + " nodeID:" + nodeID + " " + roles;
	}
}
