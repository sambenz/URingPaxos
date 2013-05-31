package ch.usi.da.paxos.storage;

/**
 * Name: StableStorage<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public interface StableStorage {

	public void put(Integer instance, Decision decision);
	
	public Decision get(Integer instance);
	
	public boolean contains(Integer instance);
	
	public void close();
}
