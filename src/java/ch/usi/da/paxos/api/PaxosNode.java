package ch.usi.da.paxos.api;
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

import java.net.InetAddress;
import java.util.List;

import ch.usi.da.paxos.ring.RingDescription;

/**
 * Name: PaxosNode<br>
 * Description: <br>
 * 
 * Creation date: Jun 21, 2013<br>
 * $Id$
 * 
 * @author leandro.pacheco.de.sousa@usi.ch
 */
public interface PaxosNode {
	/**
	 * Get a list of descriptions for the rings this node is part of.
	 * @return list of ring descriptions
	 */
	public List<RingDescription> getRings();

	/**
	 * Update the list of rings a node manage
	 * @return true if successful added
	 */
	public boolean updateRing(RingDescription ring);
	
	/**
	 * Get the learner interface.
	 * @return The learner. If the node is not initialized as a Learner in any ring, returns null.
	 */
	public Learner getLearner();
	
	/**
	 * Get the proposer interface. 
	 * @param ringID the ID of the ring where the value is to be proposed.
	 * @return The proposer. If the node is not initialized as a Proposer in the given ring, returns null.
	 */
	public Proposer getProposer(int ringID);
	
	/**
	 * Get the node ID
	 * @return the node ID
	 */
	public int getNodeID();

	/**
	 * Get the group ID
	 * @return the group ID
	 */
	public int getGroupID();
	
	/**
	 * Get the node IP address
	 * @return the IP address
	 */
	public InetAddress getInetAddress();
	
}
