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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import ch.usi.da.paxos.Util;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.api.StableStorage;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;

import com.sleepycat.je.DatabaseException;

/**
 * Name: RocksDbStorage<br>
 * Description: <br>
 * 
 * Creation date: Aug 4, 2015<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class RocksDbStorage implements StableStorage {

	private final static Logger logger = Logger.getLogger(RocksDbStorage.class);

	private RocksDB db = null;

	private final DBOptions dboptions = new DBOptions();

	private final WriteOptions woptions = new WriteOptions();
	
	private ColumnFamilyHandle ballotdb;

	static {
		RocksDB.loadLibrary();
	}
	
	public RocksDbStorage(){
		this(null,true);
	}
	
	public RocksDbStorage(File file, boolean async){
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
		
		try {
			dboptions.setCreateIfMissing(true);
			dboptions.setCreateMissingColumnFamilies(true);
			List<ColumnFamilyDescriptor> cfdesc = new ArrayList<ColumnFamilyDescriptor>();
			cfdesc.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
			cfdesc.add(new ColumnFamilyDescriptor("ballot".getBytes()));		
		    List<ColumnFamilyHandle> cfhandle = new ArrayList<ColumnFamilyHandle>();
			db = RocksDB.open(dboptions,file.toString(),cfdesc,cfhandle);			
			for(ColumnFamilyHandle handle : cfhandle){ // better way to identify ColumnFamilyHandle ?
				if(handle != db.getDefaultColumnFamily()){
					ballotdb = handle;
				}
			}
		} catch (RocksDBException e) {
			logger.error("RocksDbStorage DB create failed!", e);
		}
		
    	if(async){
    		woptions.setSync(false);
    	}else{
    		woptions.setSync(true);
    	}
        
        logger.info("RocksDbStorage sync: " + woptions.sync());
	}

	@Override
	public synchronized void putBallot(Long instance, int ballot) {
		try {
			db.put(ballotdb,woptions,Util.longToByte(instance),Util.intToByte(ballot));
		} catch (RocksDBException e) {
			logger.error("RocksDbStorage ballot put failed!", e);
		}
	}

	@Override
	public synchronized int getBallot(Long instance) {
		try {
			byte[] ret = db.get(ballotdb,Util.longToByte(instance));
			if(ret != null){
				return Util.byteToInt(ret);
			}
		} catch (RocksDBException e) {
			logger.error("RocksDbStorage ballot get failed!", e);
		}
		return -1;
	}

	@Override
	public synchronized boolean containsBallot(Long instance) {
		boolean found = false;
		if(getBallot(instance) > 0){
			found = true;
		}
		return found;
	}

	@Override
	public synchronized void putDecision(Long instance, Decision decision) {
		Message m = new Message(decision.getInstance(), decision.getRing(), PaxosRole.Proposer, MessageType.Value, decision.getBallot(), decision.getBallot(), decision.getValue());
		try {
			db.put(woptions,Util.longToByte(instance),Message.toWire(m));
		} catch (RocksDBException e) {
			logger.error("RocksDbStorage decision put failed!", e);
		}
	}

	@Override
	public synchronized Decision getDecision(Long instance) {
	    Decision decision = null;
		try {
			byte[] ret = db.get(Util.longToByte(instance));
			if(ret != null){
				Message m = Message.fromWire(ret);
				Decision d = new Decision(m.getSender(),m.getInstance(),m.getBallot(),m.getValue());
				return d;
			}
		} catch (RocksDBException e) {
			logger.error("RocksDbStorage decision get failed!", e);
		}
		return decision;
	}

	@Override
	public synchronized boolean containsDecision(Long instance) {
		boolean found = false;
		if(getDecision(instance) != null){
			found = true;
		}
		return found;
	}
	
	@Override
	public synchronized boolean trim(Long instance) {
		if(instance == 0) { return true; } // fast track
		long init = getLastTrimInstance();
		putDecision(-1L,new Decision(0,instance,0,null));
		for(long i = init; i < instance; i++){
			try {
				db.remove(Util.longToByte(i));
			} catch (RocksDBException e) {
				logger.error("RocksDbStorage decision remove failed!", e);
			}
		}
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
			if(db != null){
				db.close();
			}
			dboptions.dispose();
		} catch(DatabaseException dbe) {
			logger.error("Error closing db environment!",dbe);
		}
    }
	
}
