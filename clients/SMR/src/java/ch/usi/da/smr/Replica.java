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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.Util;
import ch.usi.da.paxos.message.Control;
import ch.usi.da.paxos.message.ControlType;
import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.recovery.DfsRecovery;
import ch.usi.da.smr.recovery.RecoveryInterface;
import ch.usi.da.smr.recovery.SnapshotWriter;
import ch.usi.da.smr.transport.ABListener;
import ch.usi.da.smr.transport.ABSender;
import ch.usi.da.smr.transport.Receiver;
import ch.usi.da.smr.transport.UDPSender;

/**
 * Name: Replica<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Replica implements Receiver {
	static {
		// get hostname and pid for log file name
		String host = "localhost";
		try {
			Process proc = Runtime.getRuntime().exec("hostname");
			BufferedInputStream in = new BufferedInputStream(proc.getInputStream());
			proc.waitFor();
			byte [] b = new byte[in.available()];
			in.read(b);
			in.close();
			host = new String(b).replace("\n","");
		} catch (IOException | InterruptedException e) {
		}
		int pid = 0;
		try {
			pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
		} catch (NumberFormatException | IOException e) {
		}
		System.setProperty("logfilename", "L" + host + "-" + pid + ".log");
	}

	private final static Logger logger = Logger.getLogger(Replica.class);
	
	public final int nodeID;

	public String token;
	
	private final PartitionManager partitions;
	
	private volatile int min_token;
	
	private volatile int max_token; // if min_token > max_token : replica serves whole key space

	private final UDPSender udp;

	private final ABListener ab;
	
	private final InetAddress ip = Util.getHostAddress();
	
	private volatile SortedMap<String,byte[]> db;
		
	private volatile Map<Integer,Long> exec_instance = new HashMap<Integer,Long>();
	
	private long exec_cmd = 0;
	
	private int snapshot_modulo = 0; // disabled
	
	private final boolean use_thrift = false;
	
	RecoveryInterface stable_storage;
	
	private volatile boolean recovery = false;
	
	private volatile boolean active_snapshot = false;
	
	public Replica(String token, int ringID, int nodeID, int snapshot_modulo, String zoo_host) throws Exception {
		this.nodeID = nodeID;
		this.token = token;
		this.snapshot_modulo = snapshot_modulo;
		this.partitions = new PartitionManager(zoo_host);
		partitions.init();
		setPartition(partitions.register(nodeID, ringID, ip, token));
		udp = new UDPSender();
		if(use_thrift){
			ab = partitions.getThriftABListener(ringID,nodeID);
		}else{
			ab = partitions.getRawABListener(ringID,nodeID);
		}
		db = new  TreeMap<String,byte[]>();
		//stable_storage = new HttpRecovery(partitions);
		stable_storage = new DfsRecovery(nodeID,token,"/tmp/smr",partitions);		
	}

	public void setPartition(Partition partition){
		logger.info("Replica update partition " + partition);
		min_token = partition.getLow();
		max_token = partition.getHigh();
	}

	public void start(){
		partitions.registerPartitionChangeNotifier(this);
		// install old state
		//FIXME: disabled recovery! exec_instance = load();
		// start listening
		ab.registerReceiver(this);
		if(min_token > max_token){
			logger.info("Replica start serving partition " + token + ": whole key space");
		}else{
			logger.info("Replica start serving partition " + token + ": " + min_token + "->" + max_token);			
		}
		Thread t = new Thread((Runnable) ab);
		t.setName("ABListener");
		t.start();
		/*Thread c = new Thread("Experiemnt controller"){
			@Override
			public void run(){
				try {
					if(nodeID == 5){
						//re-partitioning: split group
						Thread.sleep(35000);
						
						//!!!!!!!!!!!! Edit also RawABListener to set correct replication group !!!!!!!!!!!!
					
						int oldRing = 1;
						int newRing = 2;
						int groupID = 2;
						token = "7FFFFFFF";

						// add ring 2
						ABSender old_sender = partitions.getThriftABSender(oldRing,2);
						ABSender new_sender = partitions.getThriftABSender(newRing,2);
				    	//String c = "s," + groupID + "," + newRing;
						Control c = new Control(1,ControlType.Subscribe,groupID,newRing); 
						Message m = new Message(1,ip + ";" + 8000,"",null);
						m.setControl(c);
						old_sender.abroadcast(m);
						new_sender.abroadcast(m);
						
						Thread.sleep(5000);
					
						// register new partition
						setPartition(partitions.register(nodeID, newRing, ip, token));
						
						//Thread.sleep(4000);

						// remove ring 1
				    	c = new Control(2,ControlType.Unsubscribe,groupID,oldRing); 
						m = new Message(1,ip + ";" + 8000,"",null);
						m.setControl(c);
						new_sender.abroadcast(m);

					}
				} catch (Exception e) {
				}
			}
		};
		c.start();*/
	}

	public void close(){
		ab.close();
		stable_storage.close();
		partitions.deregister(nodeID,token);
	}

	@Override
	public void receive(Message m) {
		logger.debug("Replica received ring " + m.getRing() + " instnace " + m.getInstnce() + " (" + m + ")");
		
		// skip already executed commands
		/*FIXME: disabled recovery!if(m.getInstnce() <= exec_instance.get(m.getRing())){
			return;
		}*/
		if(m.isSkip() || m.isSetControl()){ // skip skip-instances
			exec_instance.put(m.getRing(),m.getInstnce());
			return;
		}

		List<Command> cmds = new ArrayList<Command>();

		// recover if a not ascending instance arrives 
		/*FIXME: disabled recovery! if(m.getInstnce()-1 != exec_instance.get(m.getRing())){
			while(m.getInstnce()-1 > exec_instance.get(m.getRing())){
				logger.info("Replica start recovery: " + exec_instance.get(m.getRing()) + " to " + (m.getInstnce()-1));				
				exec_instance = load();
			}
		}*/
		
		// write snapshot
		exec_cmd++;
		if(snapshot_modulo > 0 && exec_cmd % snapshot_modulo == 0){
			async_checkpoint(); 
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
		    			Command cmd = new Command(c.getID(),CommandType.RESPONSE,c.getKey(),null);
		    			cmds.add(cmd);
					}
					break;
				case GETRANGE: // key range (token range not implemented)
					/* Inspired by the Cassandra API:
					The semantics of start keys and tokens are slightly different. 
					Keys are start-inclusive; tokens are start-exclusive. Token 
					ranges may also wrap -- that is, the end token may be less than 
					the start one. Thus, a range from keyX to keyX is a one-element 
					range, but a range from tokenY to tokenY is the full ring (one 
					exception is if keyX is mapped to the minimum token, then the 
					range from keyX to keyX is the full ring).
					Attribute	Description
					start_key	The first key in the inclusive KeyRange.
					end_key		The last key in the inclusive KeyRange.
					start_token	The first token in the exclusive KeyRange.
					end_token	The last token in the exclusive KeyRange.
					count		The total number of keys to permit in the KeyRange.
					 */
					String start_key = c.getKey();
					String end_key = new String(c.getValue()).split(";")[0];
					int count = c.getCount();
					logger.info("getrange " + start_key + " -> " + end_key + " (" + MurmurHash.hash32(start_key) + "->" + MurmurHash.hash32(end_key) + ")");
					//logger.debug("tailMap:" + db.tailMap(start_key).keySet() + " count:" + count);
					int msg = 0;
					int msg_size = 0;
					for(Entry<String,byte[]> e : db.tailMap(start_key).entrySet()){
						if(msg >= count || (!end_key.isEmpty() && e.getKey().compareTo(end_key) > 0)){ break; }
						if(msg_size >= 50000){ break; } // send by UDP						
			    		Command cmd = new Command(c.getID(),CommandType.RESPONSE,e.getKey(),e.getValue());
			    		msg_size += e.getValue().length;
			    		cmds.add(cmd);
			    		msg++;
					}
					if(msg == 0){
			    		Command cmd = new Command(c.getID(),CommandType.RESPONSE,"",null);
			    		cmds.add(cmd);						
					}
					// signal
					partitions.singal(token,c);
					// wait until signal from every involved partition
					boolean ret = partitions.waitSignal(c);
					if(ret != true){
						cmds.clear();
					}
					break;
				default:
					System.err.println("Receive RESPONSE as Command!"); break;
		    	}
			}
		}
		exec_instance.put(m.getRing(),m.getInstnce());
		int msg_id = MurmurHash.hash32(m.getInstnce() + "-" + token);
		Message msg = new Message(msg_id,token,m.getFrom(),cmds);
		//logger.debug("Send UDP: " + msg);
		udp.send(msg);
	}

	public Map<Integer,Long> load(){
		try{
			return stable_storage.installState(token,db);
		}catch(Exception e){
			if(!exec_instance.isEmpty()){
				return exec_instance;
			}else{ // init to 0
				Map<Integer,Long> instances = new HashMap<Integer,Long>();
				instances.put(partitions.getGlobalRing(),0L);
				for(Partition p : partitions.getPartitions()){
					instances.put(p.getRing(),0L);
				}
				return instances;
			}
		}
	}
	
	public boolean sync_checkpoint(){
		if(stable_storage.storeState(exec_instance,db)){
			try {
				for(Entry<Integer, Long> e : exec_instance.entrySet()){
					ab.safe(e.getKey(),e.getValue());
				}
				logger.info("Replica checkpointed up to instance " + exec_instance);
				return true;
			} catch (Exception e) {
				logger.error(e);
			}
		}
		return false;
	}

	public boolean async_checkpoint(){
		if(!active_snapshot){
			active_snapshot = true;
			// shallow copy
			Map<Integer,Long> old_exec_instance = new HashMap<Integer,Long>(exec_instance);
			SortedMap<String,byte[]> old_db = new TreeMap<String,byte[]>(db);
			// deep copy
			/* Map<Integer,Long> old_exec_instance = new HashMap<Integer,Long>();
			for(Entry<Integer,Long> e : exec_instance.entrySet()){
				old_exec_instance.put(new Integer(e.getKey()),new Long(e.getValue()));
			}
			SortedMap<String,byte[]> old_db = new TreeMap<String,byte[]>();
			for(Entry<String,byte[]> e : db.entrySet()){
				old_db.put(new String(e.getKey()),Arrays.copyOf(e.getValue(),e.getValue().length));
			}
			old_db.putAll(db); */
			Thread t = new Thread(new SnapshotWriter(this,old_exec_instance,old_db,stable_storage,ab));
			t.start();
		}else{
			logger.info("Async checkpoint supressed since other active!");
		}
		return true;
	}
	
	public void setActiveSnapshot(boolean b){
		active_snapshot = b;
	}

	public boolean getRecovery(){
		return recovery;
	}

	/**
	 * Do not accept commands until you know you have recovered!
	 * 
	 * The commands are queued in the learner itself.
	 * 
	 */
	@Override
	public boolean is_ready(Integer ring, Long instance) {
		if(instance <= exec_instance.get(ring)+1){
			if(recovery == true){
				recovery = false;
				logger.info("Recovery set false.");
			}
			return true;
		}
		if(recovery == false){
			recovery = true;
			Thread t = new Thread(){
				@Override
				public void run() {
					logger.info("Replica starts recovery thread.");
					while(getRecovery()){
						exec_instance = load();
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							break;
						}
					}
					logger.info("Recovery thread stopped.");
				}
			};
			t.setName("Recovery");
			t.start();
		}
		return false;
	}

	public static boolean newerState(Map<Integer, Long> nstate, Map<Integer, Long> state) {
		for(Entry<Integer, Long> e : state.entrySet()){
			long i = e.getValue();
			if(i > 0){
				long ni = nstate.get(e.getKey());
				if(ni > i){
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String zoo_host = "127.0.0.1:2181";
		int snapshot = 0;
		if (args.length > 2) {
			zoo_host = args[2];
		}
		if (args.length > 1) {
			snapshot = Integer.parseInt(args[1]);
		}
		if (args.length < 1) {
			System.err.println("Plese use \"Replica\" \"ringID,nodeID,Token\" [snapshot_modulo] [zookeeper host]");
		} else {
			String[] arg = args[0].split(",");
			final int nodeID = Integer.parseInt(arg[1]);
			final int ringID = Integer.parseInt(arg[0]);
			final String token = arg[2];
			try {
				final Replica replica = new Replica(token,ringID,nodeID,snapshot,zoo_host);
				Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
					@Override
					public void run(){
						replica.close();
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
