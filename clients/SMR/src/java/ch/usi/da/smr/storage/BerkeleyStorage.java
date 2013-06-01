package ch.usi.da.smr.storage;
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

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * Name: BerkeleyStorage<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class BerkeleyStorage {

	private final static Logger logger = Logger.getLogger(BerkeleyStorage.class);

	private final Environment env;
	
	private final Database db;

	private final Database classCatalogDb;
    
    private final StoredClassCatalog classCatalog;

	private final DatabaseEntry key = new DatabaseEntry();

    private final EntryBinding<String> keyBinding;

	private final DatabaseEntry data = new DatabaseEntry();
    
	private final EntryBinding<byte[]> dataBinding;
	
	public BerkeleyStorage(){
		this(null,false);
	}
	
	public BerkeleyStorage(File file, boolean readonly){
		if(file == null){
	        int pid = 0;
	        try {
				pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
			} catch (NumberFormatException | IOException e) {
			}        
	        file = new File("/tmp/replica-db/" + pid);
		}
		file.mkdirs();
        EnvironmentConfig envConfig = new EnvironmentConfig();
        DatabaseConfig dbConfig = new DatabaseConfig();
        envConfig.setReadOnly(readonly);
        dbConfig.setReadOnly(readonly);
        envConfig.setAllowCreate(!readonly);
        dbConfig.setAllowCreate(!readonly);
        env = new Environment(file, envConfig);
        db = env.openDatabase(null,"smrDB",dbConfig);
        classCatalogDb = env.openDatabase(null,"ClassCatalogDB", dbConfig);
        classCatalog = new StoredClassCatalog(classCatalogDb);
        keyBinding = TupleBinding.getPrimitiveBinding(String.class);
        dataBinding = new SerialBinding<byte[]>(classCatalog,byte[].class);
	}
	
	public synchronized boolean put(String id, byte[] value) {
        keyBinding.objectToEntry(id,key);
        dataBinding.objectToEntry(value,data);
        OperationStatus status = db.put(null,key,data);
        if(status == OperationStatus.SUCCESS){
	    	return true;
	    }
		return false;
	}

	public synchronized byte[] get(String id) {
	    keyBinding.objectToEntry(id,key);
	    byte[] value = null;
	    OperationStatus status = db.get(null,key,data,LockMode.DEFAULT);
	    if (status == OperationStatus.SUCCESS) {
	        value = dataBinding.entryToObject(data);
	    }
		return value;
	}

	public synchronized boolean delete(String id) {
	    keyBinding.objectToEntry(id,key);
	    OperationStatus status = db.delete(null,key);
	    if (status == OperationStatus.SUCCESS) {
	        return true;
	    }
		return false;
	}

	public synchronized boolean contains(String id) {
		keyBinding.objectToEntry(id,key);
	    OperationStatus status = db.get(null,key,data,LockMode.DEFAULT);
	    if(status == OperationStatus.SUCCESS){
	    	return true;
	    }
		return false;
	}
	
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
            String id = keyBinding.entryToObject(key);
            byte[] value = dataBinding.entryToObject(data);
            System.out.println(id + " -> " + new String(value) + "");
	    }
	    cursor.close();
	}
	
	/**
	 * Debug method
	 */
	public static void main(String[] args){
		/*
		File file = new File("/tmp/replica-db/0");
		file.mkdirs();
		BerkeleyStorage db = new BerkeleyStorage(file,false);
		System.out.println(db.contains("Key1"));
		db.put("Key1","Value1".getBytes());
		System.out.println(new String(db.get("Key1")));
		db.delete("Key1");
		System.out.println(db.get("Key1"));		
		db.close();
		*/
		
		BerkeleyStorage db = new BerkeleyStorage(new File("/tmp/replica-db/8468"),true);
		db.listAll();
		db.close();
		
	}
	
}
