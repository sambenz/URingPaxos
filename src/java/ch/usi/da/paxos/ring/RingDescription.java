package ch.usi.da.paxos.ring;

import java.util.Collections;
import java.util.List;

import ch.usi.da.paxos.message.PaxosRole;

/**
 * Name: Ring<br>
 * Description: <br>
 * 
 * Creation date: Mar 04, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
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
	
	public synchronized RingManager getRingmanger(){
		return ring;
	}
}
