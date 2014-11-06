package ch.usi.da.smr.recovery;
/* 
 * Copyright (c) 2014 Università della Svizzera italiana (USI)
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import ch.usi.da.smr.PartitionManager;
import ch.usi.da.smr.Replica;

/**
 * Name: DfsRecovery<br>
 * Description: <br>
 * 
 * Assumes a local mounted distributed file system to access
 * local and remote snapshots.
 * 
 * Mountpoint: /prefix
 * 
 * Creation date: Nov 6, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class DfsRecovery implements RecoveryInterface {

	private final static Logger logger = Logger.getLogger(DfsRecovery.class);

	public final int nodeID;

	public final String token;
	
	private final PartitionManager partitions;
	
	private final int max_ring = 20;

	private final String path;
	
	private final String prefix;
	
	public static final String snapshot_file = "snapshot.ser";

	public static final String state_file = "snapshot.state"; // only used to quickly decide if remote snapshot is newer

	public DfsRecovery(int nodeID, String token, String prefix, PartitionManager partitions){
		this.nodeID = nodeID;
		this.token = token;
		this.prefix = prefix;
		this.partitions = partitions;
		this.path = prefix + "/" + token + "/" + nodeID;
		File dir = new File(path);
		dir.mkdirs();
	}
	
	public boolean storeState(Map<Integer,Long> instances, Map<String,byte[]> db){
		try {
			synchronized(db){
				logger.info("Replica start storing state ... ");
				for(Entry<Integer,Long> e : instances.entrySet()){
					db.put("r:" + e.getKey(),e.getValue().toString().getBytes());
				}
				FileOutputStream fs = new FileOutputStream(path + "/" + snapshot_file);
		        ObjectOutputStream os = new ObjectOutputStream(fs);
		        os.writeObject(db);
		        os.flush();
		        fs.getChannel().force(false); // fsync
		        os.close();
				fs = new FileOutputStream(path + "/" + state_file);
				for(Entry<Integer, Long> e : instances.entrySet()){
					fs.write((e.getKey() + "=" + e.getValue() + "\n").getBytes());
				}
				fs.getChannel().force(false);
		        os.close();
		        logger.info("... state stored up to instance: " + instances);
			}
	        return true;
		} catch (IOException e) {
			logger.error(e);
		}
		return false;
	}
	
	public Map<Integer,Long> installState(String token,Map<String,byte[]> db) throws Exception{
		Map<Integer,Long> instances = new HashMap<Integer,Long>();
		String node = null;
		InputStreamReader isr;
		String line;
		Map<Integer, Long> state = new HashMap<Integer, Long>();	
		// local
		try{
			isr = new InputStreamReader(new FileInputStream(path + "/" + state_file));
			BufferedReader bin = new BufferedReader(isr);	
			while ((line = bin.readLine()) != null){
				String[] s = line.split("=");
				state.put(Integer.parseInt(s[0]),Long.parseLong(s[1]));
			}
			logger.info("Replica found local snapshot: " + state);
			bin.close();
		}catch (FileNotFoundException e){
			logger.info("No local snapshot present.");
		}
		
		// remote TODO: must ask min. GSQ replicas
		for(String n : partitions.getReplicaIDs(token)){
			try{
				isr = new InputStreamReader(new FileInputStream(prefix + "/" + token + "/" + n + "/" + state_file));
				BufferedReader in = new BufferedReader(isr);
				Map<Integer, Long> nstate = new HashMap<Integer, Long>();
				while ((line = in.readLine()) != null){
					String[] s = line.split("=");
					nstate.put(Integer.parseInt(s[0]),Long.parseLong(s[1]));
				}
				if(state.isEmpty()){
					state = nstate;
					node = n;
				}else if(Replica.newerState(nstate,state)){
					state = nstate;
					node = n;
				}
				logger.info("Replica found remote snapshot: " + nstate + " (" + n + ")");
				in.close();
			}catch(Exception e){
				logger.error("Error getting state from " + n,e);
			}
		}
		synchronized(db){
			InputStream in;
			if(node != null){
				logger.info("Use remote snapshot from host " + node);
				in = new FileInputStream(new File(prefix + "/" + token + "/" + node + "/" + snapshot_file));
			}else{
				in = new FileInputStream(new File(path + "/" + snapshot_file));
			}
			ObjectInputStream ois = new ObjectInputStream(in);
			@SuppressWarnings("unchecked")
			Map<String,byte[]> m = (Map<String,byte[]>) ois.readObject();
			ois.close();
			in.close();
			db.clear();
			db.putAll(m);
			byte[] b = null;
			for(int i = 1;i<max_ring+1;i++){
				if((b = db.get("r:" + i)) != null){
					instances.put(i,Long.valueOf(new String(b)));
				}
			}
		}
		logger.info("Replica installed snapshot instance: " + instances);
		return instances;
	}

	public void close(){

	}

}
