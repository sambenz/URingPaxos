package ch.usi.da.paxos.storage;

/**
 * Name: SyncBerkeleyStorage<br>
 * Description: <br>
 * 
 * This class only exists that you can the accepters
 * can always call a default constructor!
 *  
 * Creation date: Apr 20, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class SyncBerkeleyStorage implements StableStorage {

	private final BerkeleyStorage storage;
	
	public SyncBerkeleyStorage(){
		storage = new BerkeleyStorage(null,false,false);
	}

	@Override
	public void put(Integer instance, Decision decision) {
		storage.put(instance, decision);
		
	}

	@Override
	public Decision get(Integer instance) {
		return storage.get(instance);
	}

	@Override
	public boolean contains(Integer instance) {
		return storage.contains(instance);
	}

	@Override
	public void close() {
		storage.close();
	}
}
