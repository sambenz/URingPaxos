package ch.usi.da.paxos.storage;
/* 
 * Copyright (c) 2015 Università della Svizzera italiana (USI)
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

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import ch.usi.da.paxos.api.StableStorage;



/**
 * Name: H2Storage<br>
 * Description: <br>
 * 
 * Creation date: Jun 1, 2015<br>
 * $Id$
 * 
 * @author Odorico Mendizabal omendizabal@gmail.com
 */
public class H2Storage implements StableStorage {

	private final static Logger logger = Logger.getLogger(StableStorage.class);
	
	private final MVStore store;
	private MVMap<Long,Integer> promised;
	private MVMap<Long,Decision> accepted;
	
	private long last_trimmed_instance = 0;	

	private final int cacheSize = 4;				// size in MB
//	private final int writeBuffer = 1024*64; 		// size in KB
	
	//private final int size = 64*1024;
	//private final int max = 15000;
	
	
	public H2Storage(){	
		this(null,false,true);
	}
	
	public H2Storage(File file, boolean readonly, boolean async){
		int pid = 0;
	    if(file == null){
	    	try {
				pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
			} catch (NumberFormatException | IOException e) {
			}
	        String path = "/tmp";
			String db_path = System.getenv("DB");
			if(db_path != null){
				path = db_path;
			}	        
	        file = new File(path + "/h2-db/");
	        file.mkdirs();
		}
	    
	    /*
		OffHeapStore offHeap = new OffHeapStore();
		store = new MVStore.Builder().				
		        fileStore(offHeap).
		        fileName(file.getPath()+"/"+pid).
		        //fileName(file.getPath()+"/"+pid).
		        cacheSize(cacheSize).		        	// size for read cache in MB
		        autoCommitBufferSize(writeBuffer).		// size of the write buffer in KB disk space
		        open();
		*/
	    
	    store = MVStore.open(file.getPath()+"/"+pid);	    
	    
	    store.setCacheSize(cacheSize);
		store.setAutoCommitDelay(10*1000); 		// save updates in disk every X miliseconds
		
	    /*
        store = new MVStore.Builder().
                writeBufferSize(25).
                writeDelay(-1).
                fileName(file.getPath()+"/"+pid).
                cacheSize(100).
                open();
	    */
		
		// create or open existing maps 
		promised = store.openMap("ballot");
		accepted = store.openMap("decision");
		
	    logger.debug("H2Storage created");
	}
	
	@Override
	public void putBallot(Long instance, int ballot) {
		promised.put(instance, ballot);
	}

	@Override
	public int getBallot(Long instance) {
		return promised.get(instance);
	}

	@Override
	public boolean containsBallot(Long instance) {
		return promised.containsKey(instance);
	}

	@Override
	public void putDecision(Long instance, Decision decision) {		
		accepted.put(instance, decision);
	}

	@Override
	public Decision getDecision(Long instance) {
		return accepted.get(instance); 	
	}

	@Override
	public boolean containsDecision(Long instance) {
		boolean found = false;
		Decision decision = accepted.get(instance);		
	    if(decision != null){
	    	found = true;
	    }
		if(logger.isDebugEnabled()){
			logger.debug("DB contains " + instance + " " + found + " (" + decision + ")");
		}
		return found;		
	}

	@Override
	public boolean trim(Long instance) {
		if(instance == 0) 
			return true;	// fast track
		
		for(long i= last_trimmed_instance; i <= instance; i++){
			promised.remove(i);
			accepted.remove(i);
		}
		last_trimmed_instance = instance;
		putDecision(-1L,new Decision(0,instance,0,null));
		logger.debug("DB delete up to instance " + instance);		
		return true;
	}

	@Override
	public Long getLastTrimInstance() {		
		Decision d = getDecision(-1L);
		if(d != null){
			return getDecision(-1L).getInstance();
		}else{
			return 0L;
		}
	}

	@Override
	public void close(){
		store.close();		
	}	
}
