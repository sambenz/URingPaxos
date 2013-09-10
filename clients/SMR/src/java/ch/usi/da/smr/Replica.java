package ch.usi.da.smr;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.ABListener;
import ch.usi.da.smr.transport.Receiver;
import ch.usi.da.smr.transport.UDPSender;

/**
 * Name: Replica<br>
 * Description: <br>
 * 
 * TODO: RECOVERY IS WORK IN PROGRESS (and maybe wrong) !!!!
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class Replica implements Receiver {

	private final static Logger logger = Logger.getLogger(Replica.class);
	
	private final PartitionManager partitions;
	
	private final Partition partition;
	
	private final int nodeID;
	
	private final UDPSender udp;
	
	private final ABListener ab;
	
	private final Map<String,byte[]> db;
	
	private final int max_response_msg = 10000;
	
	private final String snapshot_prefix = "/tmp";
	
	private int[] exec_instance = new int[20];
	
	private long exec_cmd = 0;
	
	public Replica(PartitionManager partitions, Partition partition, int nodeID) throws IOException, TTransportException {
		this.partitions = partitions;
		this.partition = partition;
		this.nodeID = nodeID;
		udp = new UDPSender();
		ab = partitions.getABListener(partition,nodeID);
		db = new  HashMap<String,byte[]>();
	}

	public void start(){
		// install old state
		logger.info("Replica start install state: " + exec_instance[partition.getRing()]);
		exec_instance[partition.getRing()] = installState();
		// start listening
		ab.registerReceiver(this);
		logger.info("Replica start serving partition: " + partition);
		Thread t = new Thread(ab);
		t.start();
	}

	public void close(){
		ab.close();
		partitions.deregister(partition,nodeID);
	}
	
	public boolean checkpoint(){
		int ring = partition.getRing();
		if(storeState(exec_instance[ring])){
			try {
				ab.getLearner().safe(ring,exec_instance[ring]);
				logger.info("Replica checkpointed up to instance " + exec_instance[ring] + " of ring " + ring);
				return true;
			} catch (TException e) {
				logger.error(e);
			}
		}
		return false;
	}
	
	private boolean storeState(int instance){
		try {
			synchronized(db){
				logger.info("Replica start storing state ... ");
				FileOutputStream fs = new FileOutputStream(snapshot_prefix + "/map-" + instance + ".ser");
		        ObjectOutputStream os = new ObjectOutputStream(fs);
		        os.writeObject(db);
		        os.flush();
		        fs.getChannel().force(true); // fsync
		        os.close();
		        logger.info("... state stored up to instance " + instance);
			}
	        return true;
		} catch (IOException e) {
			logger.error(e);
		}
		return false;
	}
	
	private int installState(){
		int instance = 0;
		try {
			// get local snapshot
			File dir = new File(snapshot_prefix);
			for(File f : dir.listFiles()){
				if(f.getName().startsWith("map-")){
					int i = Integer.parseInt(f.getName().replaceAll("\\D",""));
					if(i > instance){
						instance = i;
					}
				}
			}
			//TODO: get remote snapshot
			//must ask get-state quorum (GSQ) servers and then get the highest snapshot nr.
			synchronized(db){
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(snapshot_prefix + "/map-" + instance + ".ser"));
				@SuppressWarnings("unchecked")
				Map<String,byte[]> m = (Map<String,byte[]>) ois.readObject();
				ois.close();
				db.clear();
				db.putAll(m);
				logger.info("Replica installed snapshot instance " + instance);
			}
			return instance;
		} catch (IOException | ClassNotFoundException e) {
			logger.error(e);
		}
		return instance;
	}
	
	@Override
	public void receive(Message m) {
		List<Command> cmds = new ArrayList<Command>();
		
		//if(exec_instance[partition.getRing()] == 0){} -> already done in start(); 
		
		// skip already executed commands
		if(m.getInstnce() <= exec_instance[m.getRing()]){
			return;
		}else if(m.isSkip()){ // skip skip-instances
			exec_instance[m.getRing()] = m.getInstnce();
			return;
		}
		
		// recover if a not ascending instance arrives 
		if(m.getRing() == partition.getRing() && m.getInstnce()-1 != exec_instance[partition.getRing()]){
			logger.info("Replica start recovery: " + exec_instance[partition.getRing()] + " to " + (m.getInstnce()-1));
			while(m.getInstnce()-1 > exec_instance[partition.getRing()]){
				exec_instance[partition.getRing()] = installState();
			}
		}
		
		// snapshot
		exec_cmd++;
		if(exec_cmd % 5 == 0){ //TODO: testing only!
			logger.warn("force testing snapshot (" + exec_cmd + ")");
			checkpoint();
		}
		
		synchronized(db){
			byte[] data;
			for(Command c : m.getCommands()){
		    	switch(c.getType()){
		    	case PUT:
		    		db.put(c.getKey(),c.getValue());
		    		if(db.containsKey(c.getKey())){
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"OK".getBytes());
		    			cmds.add(cmd);
		    		}else{
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"FAIL".getBytes());
		    			cmds.add(cmd);
		    		}
		    		break;
				case DELETE:
		    		if(db.remove(c.getKey()) != null){
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"OK".getBytes());
		    			cmds.add(cmd);
		    		}else{
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"FAIL".getBytes());
		    			cmds.add(cmd);
		    		}
					break;
				case GET:
					data = db.get(c.getKey());
					if(data != null){
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),data);
		    			cmds.add(cmd);
					}else{
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),"<no entry>".getBytes());
		    			cmds.add(cmd);
					}
					break;
				case GETRANGE:
					int from = Integer.valueOf(c.getKey());
					if(from < partition.getLow()){
						from = partition.getLow();
					}
					int to = Integer.valueOf(new String(c.getValue()));
					if(to > partition.getHigh()){
						to = partition.getHigh();
					}
					int msg = 0;
					while(from <= to){
						String key = Integer.toString(from);
						data = db.get(key);
						if(data != null){
			    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,key,data);
			    			cmds.add(cmd);
						}else{
			    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,key,"<no entry>".getBytes());
			    			cmds.add(cmd);
						}					
						from++;
						if(msg++ > max_response_msg){ break; }
					}
					break;
				default:
					System.err.println("Receive RESPONSE as Command!"); break;
		    	}
			}
		}
		exec_instance[m.getRing()] = m.getInstnce();
		Message msg = new Message(m.getID(),m.getSender(),cmds);
		udp.send(msg);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String zoo_host = "127.0.0.1:2181";
		if (args.length > 1) {
			zoo_host = args[1];
		}
		if (args.length < 1) {
			System.err.println("Plese use \"Replica\" \"ringID,nodeID,Token\"");
		} else {
			String[] arg = args[0].split(",");
			final int nodeID = Integer.parseInt(arg[1]);
			final int ringID = Integer.parseInt(arg[0]);
			final int token = Integer.parseInt(arg[2]);
			try {
				final ZooKeeper zoo = new ZooKeeper(zoo_host,3000,null);
				final PartitionManager partitions = new PartitionManager(zoo);
				final Partition partition = new Partition(ringID,0,token);
				partitions.init();
				final Replica replica = new Replica(partitions,partitions.register(partition,nodeID),nodeID);
				Runtime.getRuntime().addShutdownHook(new Thread(){
					@Override
					public void run(){
						replica.close();
						try {
							zoo.close();
						} catch (InterruptedException e) {
						}
					}
				});
				replica.start();
				BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
				in.readLine();
				replica.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

}
