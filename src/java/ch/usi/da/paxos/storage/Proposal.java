package ch.usi.da.paxos.storage;

import ch.usi.da.paxos.message.Value;
	
/**
 * Name: Proposal<br>
 * Description: <br>
 * 
 * Creation date: Sep 5, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class Proposal {

	private final long date = System.currentTimeMillis();

	private final Value value;
	
	/**
	 * @param v
	 */
	public Proposal(Value v){
		this.value = v;
	}
	
	/**
	 * @return the valuemessage
	 */
	public Value getValue(){
		return value;
	}
	
	/**
	 * @return the date
	 */
	public long getDate(){
		return date;
	}
}
