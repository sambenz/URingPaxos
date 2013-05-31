package ch.usi.da.paxos.storage;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Name: InMemory<br>
 * Description: <br>
 * 
 * Creation date: Mar 31, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class InMemory implements StableStorage {

	private final Map<Integer, Decision> decided = new LinkedHashMap<Integer,Decision>(10000,0.75F,false){
		private static final long serialVersionUID = -3704400228030327063L;
			protected boolean removeEldestEntry(Map.Entry<Integer, Decision> eldest) {  
				return size() > 15000; // hold only 15'000 values in memory !                                 
	}};
	
	@Override
	public void put(Integer instance, Decision decision) {
		decided.put(instance, decision);
	}

	@Override
	public Decision get(Integer instance) {
		return decided.get(instance);
	}

	@Override
	public boolean contains(Integer instance) {
		return decided.containsKey(instance);
	}

	@Override
	public void close(){
		
	}
}
