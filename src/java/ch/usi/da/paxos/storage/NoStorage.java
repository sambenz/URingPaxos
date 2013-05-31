package ch.usi.da.paxos.storage;


/**
 * Name: NoStorage<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class NoStorage implements StableStorage {

	//private final static Logger logger = Logger.getLogger(NoStorage.class);

	@Override
	public void put(Integer instance, Decision decision) {
		/*if(logger.isDebugEnabled()){
			logger.debug("add " + decision + " to stable storage");
		}*/
	}

	@Override
	public Decision get(Integer instance) {
		return null;
	}

	@Override
	public boolean contains(Integer instance) {
		/*if(logger.isDebugEnabled()){
			logger.debug("check if " + instance + " exists in stable storage");
		}*/
		return false;
	}

	@Override
	public void close(){
		
	}
}
