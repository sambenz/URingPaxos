package ch.usi.da.paxos.storage;

/**
 * Name: Promise<br>
 * Description: <br>
 * 
 * Creation date: Apr 10, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class Promise {
	
	private final long date = System.currentTimeMillis();
	
	private final Integer instance;
	
	private final Integer ballot;

	/**
	 * Public constructor
	 * 
	 * @param instance
	 * @param ballot
	 */
	public Promise(Integer instance,Integer ballot){
		this.instance = instance;
		this.ballot = ballot;
	}
	
	/**
	 * Get the instance
	 * 
	 * @return the instance number
	 */
	public Integer getInstance(){
		return instance;
	}
	
	/**
	 * Get the ballot
	 * 
	 * @return the ballot number
	 */
	public Integer getBallot(){
		return ballot;
	}
	
	/**
	 * @return the date
	 */
	public long getDate(){
		return date;
	}
}
