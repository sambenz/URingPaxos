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
import ch.usi.da.paxos.message.Value;

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

/**
 * Name: BerkeleyStorage<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class BerkeleyStorage implements StableStorage {

	private final static Logger logger = Logger.getLogger(BerkeleyStorage.class);

	private final Environment env;
	
	private final Database db;

	private final Database classCatalogDb;

    private final StoredClassCatalog classCatalog;

	private final DatabaseEntry key = new DatabaseEntry();

    private final EntryBinding<Integer> keyBinding;

	private final DatabaseEntry data = new DatabaseEntry();
    
	private final EntryBinding<Decision> dataBinding;
	
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
	        file = new File("/tmp/ringpaxos-db/" + pid);
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
        	envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        	dbConfig.setDeferredWrite(true);
        }else{
        	envConfig.setDurability(Durability.COMMIT_SYNC);
        	dbConfig.setDeferredWrite(false);        	
        }
        
        env = new Environment(file, envConfig);
        db = env.openDatabase(null,"paxosDB",dbConfig);
        classCatalogDb = env.openDatabase(null,"ClassCatalogDB", dbConfig);
        classCatalog = new StoredClassCatalog(classCatalogDb);
        keyBinding = TupleBinding.getPrimitiveBinding(Integer.class);
        dataBinding = new SerialBinding<Decision>(classCatalog,Decision.class);

        logger.info("BerkeleyStorage cache size: " + env.getMutableConfig().getCacheSize());
        logger.info("BerkeleyStorage durability: " + env.getMutableConfig().getDurability().getLocalSync());
        logger.info("BerkeleyStorage deferred write: " + db.getConfig().getDeferredWrite());
	}
	
	@Override
	public synchronized void put(Integer instance, Decision decision) {
        keyBinding.objectToEntry(instance,key);
        dataBinding.objectToEntry(decision,data);
        OperationStatus status = db.put(null,key,data);
        if(logger.isDebugEnabled()){
        	logger.debug("DB put " + decision + " " + status.name());
        }
	}

	@Override
	public synchronized Decision get(Integer instance) {
	    keyBinding.objectToEntry(instance,key);
	    Decision decision = null;
	    OperationStatus status = db.get(null,key,data,LockMode.DEFAULT);
	    if (status == OperationStatus.SUCCESS) {
	        decision = dataBinding.entryToObject(data);
	    }
	    if(decision == null){
	    	logger.error("Error getting Decsion from DB! (" + status + ")");
	    }
	    if(logger.isDebugEnabled()){
	    	logger.debug("DB get " + decision + " " + status.name());
	    }
		return decision;
	}

	@Override
	public synchronized boolean contains(Integer instance) {
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
	public synchronized boolean trim(Integer instance) {
		if(instance == 0) { return true; } // fast track
		Cursor cursor = db.openCursor(null, null);
		try{
			while (cursor.getNext(key,data,LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				Integer i = keyBinding.entryToObject(key);
				if(i < instance && cursor.delete() != OperationStatus.SUCCESS){
					logger.error("Error deleting instance " + i + " from DB!");
					return false;
				}
			}
			put(-1,new Decision(0,instance,0,null));
		}finally{
			cursor.close();
		}
		logger.debug("DB deltete up to instance " + instance);
		return true;
	}

	@Override
	public synchronized Integer getLastTrimInstance() {
		return get(-1).getInstance();
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
	    	
            Integer instance = keyBinding.entryToObject(key);
            Decision decision = dataBinding.entryToObject(data);
            System.out.println("instance " +  instance + " -> " + decision + "");
	    }
	    cursor.close();
	}
	
	/**
	 * Debug method
	 */
	public static void main(String[] args){
		File file = new File("/tmp/ringpaxos-db/0");
		file.mkdirs();
		BerkeleyStorage db = new BerkeleyStorage(file,false,true);
		Decision d = new Decision(0,1,42,new Value("id","value".getBytes()));
		Decision d2 = new Decision(0,1,43,new Value("id","value".getBytes()));
		db.contains(1);
		db.put(1,d);
		db.put(1,d2);
		db.contains(1);
		System.out.println(db.get(1));
		
		db.put(2,d);
		db.put(3,d);
		db.put(4,d);
		db.put(5,d);
		db.put(6,d);
		db.put(7,d);
		db.put(8,d);
		db.put(9,d);
		db.put(10,d);
		System.out.println(db.trim(7));
		
		db.listAll();
		db.close();
		/*
		BerkeleyStorage db = new BerkeleyStorage(new File("/home/benz/download/db/24306"),true);
		db.listAll();
		db.close();
		*/
	}
	
}
