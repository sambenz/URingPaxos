package ch.usi.da.paxos.ring;

import ch.usi.da.paxos.message.Message;

/**
 * Name: Role<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public abstract class Role implements Runnable {

	/**
	 * Method is called from NetworkManager
	 * 
	 * @param fromRing source ring
	 * @param m The message to deliver
	 */
	public void deliver(RingManager fromRing,Message m){
	}
	
}
