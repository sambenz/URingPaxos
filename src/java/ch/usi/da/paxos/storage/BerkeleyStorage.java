package ch.usi.da.paxos.storage;
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

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.StableStorage;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Name: BerkeleyStorage<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class BerkeleyStorage implements StableStorage {

	private final static Logger logger = Logger.getLogger(BerkeleyStorage.class);

	private final Environment env;
	
	private final Database db;

	private final Database classCatalogDb;

    private final StoredClassCatalog classCatalog;

	private final DatabaseEntry key = new DatabaseEntry();

    private final EntryBinding<Long> keyBinding;

	private final DatabaseEntry data = new DatabaseEntry();
    
	private final EntryBinding<Decision> dataBinding;

	private final DatabaseEntry ballot_data = new DatabaseEntry();
    
	private final EntryBinding<Integer> ballotBinding;

	public BerkeleyStorage(){
		this(null,false,true);
	}
	
	public BerkeleyStorage(File file, boolean readonly, boolean async){
		if(file == null){
	        int pid = 0;
	        try {
				pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
			} catch (NumberFormatException | IOException e) {
			}
	        String path = "/tmp";
			String db_path = System.getenv("DB");
			if(db_path != null){
				path = db_path;
			}
	        file = new File(path + "/ringpaxos-db/" + pid);
	        file.mkdirs();
		}
        EnvironmentConfig envConfig = new EnvironmentConfig();
        DatabaseConfig dbConfig = new DatabaseConfig();
        envConfig.setReadOnly(readonly);
        dbConfig.setReadOnly(readonly);
        envConfig.setAllowCreate(!readonly);
        dbConfig.setAllowCreate(!readonly);

        // performance settings
    	envConfig.setTransactional(true);
    	envConfig.setCacheMode(CacheMode.DEFAULT);
    	//envConfig.setCacheSize(1000000*800); // 800M
    	if(async){
    		dbConfig.setTransactional(false);
        	envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        	dbConfig.setDeferredWrite(true);
        }else{
    		dbConfig.setTransactional(true);
        	envConfig.setDurability(Durability.COMMIT_SYNC);
        	dbConfig.setDeferredWrite(false);        	
        }
        
        env = new Environment(file, envConfig);
        db = env.openDatabase(null,"paxosDB",dbConfig);
        classCatalogDb = env.openDatabase(null,"ClassCatalogDB", dbConfig);
        classCatalog = new StoredClassCatalog(classCatalogDb);
        keyBinding = TupleBinding.getPrimitiveBinding(Long.class);
        dataBinding = new SerialBinding<Decision>(classCatalog,Decision.class);
        ballotBinding = TupleBinding.getPrimitiveBinding(Integer.class);

        logger.info("BerkeleyStorage cache size: " + env.getMutableConfig().getCacheSize());
        logger.info("BerkeleyStorage durability: " + env.getMutableConfig().getDurability().getLocalSync());
        logger.info("BerkeleyStorage deferred write: " + db.getConfig().getDeferredWrite());
	}

	@Override
	public synchronized void putBallot(Long instance, int ballot) {
        keyBinding.objectToEntry(instance,key);
        ballotBinding.objectToEntry(ballot,ballot_data);
        OperationStatus status = db.put(null,key,ballot_data);
        if(logger.isDebugEnabled()){
        	logger.debug("DB put ballot " + ballot + " for instance " + instance + " " + status.name());
        }
	}

	@Override
	public synchronized int getBallot(Long instance) {
	    keyBinding.objectToEntry(instance,key);
	    Integer ballot = null;
	    OperationStatus status = db.get(null,key,ballot_data,LockMode.DEFAULT);
	    if (status == OperationStatus.SUCCESS) {
	        ballot = ballotBinding.entryToObject(ballot_data);
	    }
	    if(logger.isDebugEnabled()){
	    	logger.debug("DB get ballot " + ballot + " for instance " + instance + " " + status.name());
	    }
		return ballot.intValue();
	}

	@Override
	public synchronized boolean containsBallot(Long instance) {
		boolean found = false;
		keyBinding.objectToEntry(instance,key);
	    OperationStatus status = db.get(null,key,ballot_data,LockMode.DEFAULT);
	    if(status == OperationStatus.SUCCESS){
	    	found = true;
	    }
		if(logger.isDebugEnabled()){
			logger.debug("DB contains " + instance + " " + found + " (" + status.name() + ")");
		}
		return found;
	}

	@Override
	public synchronized void putDecision(Long instance, Decision decision) {
        keyBinding.objectToEntry(instance,key);
        dataBinding.objectToEntry(decision,data);
        OperationStatus status = db.put(null,key,data);
        if(logger.isDebugEnabled()){
        	logger.debug("DB put " + decision + " " + status.name());
        }
	}

	@Override
	public synchronized Decision getDecision(Long instance) {
	    keyBinding.objectToEntry(instance,key);
	    Decision decision = null;
	    OperationStatus status = db.get(null,key,data,LockMode.DEFAULT);
	    if (status == OperationStatus.SUCCESS) {
	        decision = dataBinding.entryToObject(data);
	    }
	    if(logger.isDebugEnabled()){
	    	logger.debug("DB get " + decision + " " + status.name());
	    }
		return decision;
	}

	@Override
	public synchronized boolean containsDecision(Long instance) {
		boolean found = false;
		keyBinding.objectToEntry(instance,key);
	    OperationStatus status = db.get(null,key,data,LockMode.DEFAULT);
	    if(status == OperationStatus.SUCCESS){
	    	found = true;
	    }
		if(logger.isDebugEnabled()){
			logger.debug("DB contains " + instance + " " + found + " (" + status.name() + ")");
		}
		return found;
	}
	
	@Override
	public synchronized boolean trim(Long instance) {
		if(instance == 0) { return true; } // fast track
		Transaction t = null;
		if(db.getConfig().getTransactional()){
			t = env.beginTransaction(null,null);
		}
		Cursor cursor = db.openCursor(t, null);
		boolean dirty = false; 
		try{
			while (cursor.getNext(key,data,LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
				Long i = keyBinding.entryToObject(key);
				if(i < instance && cursor.delete() != OperationStatus.SUCCESS){
					logger.error("Error deleting instance " + i + " from DB!");
					dirty = true;
				}
			}
		}finally{
			cursor.close();
			if(!dirty){
				if(t != null){ t.commit(); }
			}else{
				if(t != null){ t.abort(); }
				return false;
			}
		}
		putDecision(-1L,new Decision(0,instance,0,null));
		logger.debug("DB deltete up to instance " + instance);
		return true;
	}

	@Override
	public synchronized Long getLastTrimInstance() {
		Decision d = getDecision(-1L);
		if(d != null){
			return getDecision(-1L).getInstance();
		}else{
			return 0L;
		}
	}
	
	@Override
	public synchronized void close() {
		try {
			db.close();
			classCatalogDb.close();
			env.close();
		} catch(DatabaseException dbe) {
			logger.error("Error closing db environment!",dbe);
		}
    }
	
	/**
	 * Debug method
	 */
	public synchronized void listAll() {
		Cursor cursor = db.openCursor(null, null);
	    while (cursor.getNext(key,data,LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	    	
            Long instance = keyBinding.entryToObject(key);
            Decision decision = dataBinding.entryToObject(data);
            System.out.println("instance " +  instance + " -> " + decision + "");
	    }
	    cursor.close();
	}
	
}
