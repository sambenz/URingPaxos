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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Name: Replica<br>
 * Description: <br>
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
	
	private final String snapshot_file = "/tmp/snapshot.ser";

	private final String state_file = "/tmp/snapshot.state"; // only used to quickly decide if remote snapshot is newer
	
	private final int max_ring = 20;
	
	private Map<Integer,Integer> exec_instance = new HashMap<Integer,Integer>();
	
	private long exec_cmd = 0;
	
	public Replica(PartitionManager partitions, Partition partition, int nodeID) throws IOException, TTransportException {
		this.partitions = partitions;
		this.partition = partition;
		this.nodeID = nodeID;
		udp = new UDPSender();
		ab = partitions.getABListener(partition,nodeID);
		db = new  HashMap<String,byte[]>();
		
		// remote snapshot transfer server
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/state", new SendFile(state_file));        
        server.createContext("/snapshot", new SendFile(snapshot_file));
        server.setExecutor(null); // creates a default executor
        server.start();
	}

	public void start(){
		// install old state
		exec_instance = installState();
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
		if(storeState(exec_instance)){
			try {
				for(Entry<Integer, Integer> e : exec_instance.entrySet()){
					ab.getLearner().safe(e.getKey(),e.getValue());
				}
				logger.info("Replica checkpointed up to instance " + exec_instance);
				return true;
			} catch (TException e) {
				logger.error(e);
			}
		}
		return false;
	}
	
	private boolean storeState(Map<Integer,Integer> instances){
		try {
			synchronized(db){
				logger.info("Replica start storing state ... ");
				for(Entry<Integer,Integer> e : instances.entrySet()){
					db.put("r:" + e.getKey(),e.getValue().toString().getBytes());
				}
				FileOutputStream fs = new FileOutputStream(snapshot_file);
		        ObjectOutputStream os = new ObjectOutputStream(fs);
		        os.writeObject(db);
		        os.flush();
		        fs.getChannel().force(false); // fsync
		        os.close();
				fs = new FileOutputStream(state_file);
				for(Entry<Integer, Integer> e : exec_instance.entrySet()){
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
	
	private Map<Integer,Integer> installState(){
		Map<Integer,Integer> instances = new HashMap<Integer,Integer>();
		try {
			URL url = null;
			// local
			InputStreamReader isr = new InputStreamReader(new FileInputStream(state_file));
			BufferedReader bin = new BufferedReader(isr);
			String line;
			Map<Integer, Integer> state = new HashMap<Integer, Integer>();		
			while ((line = bin.readLine()) != null){
				String[] s = line.split("=");
				state.put(Integer.parseInt(s[0]),Integer.parseInt(s[1]));
			}
			bin.close();
//			// remote
//			for(GSQ-1? partition replicas){ //TODO: must ask get-state quorum (GSQ) servers
//				Map<Integer, Integer> nstate = URL get remote ...
//				if(nstate > state ...){
//					url = ...
//				}
//			}
			synchronized(db){
				InputStream in;
				if(url != null){
					HttpURLConnection con = (HttpURLConnection)url.openConnection();
					in = con.getInputStream();
				}else{
					in = new FileInputStream(snapshot_file);					
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
						instances.put(i,Integer.valueOf(new String(b)));
					}
				}
			}
		} catch (IOException | ClassNotFoundException e) {
			instances.put(partitions.getGlobalRing(),0);
			for(Partition p : partitions.getPartitions()){
				instances.put(p.getRing(),0);
			}
			logger.error(e);
		}
		logger.info("Replica installed snapshot instance: " + instances);
		return instances;
	}

	@Override
	public void receive(Message m) {
		List<Command> cmds = new ArrayList<Command>();
		
		//if(exec_instance[partition.getRing()] == 0){} -> already done in start(); 
		
		// skip already executed commands
		if(m.getInstnce() <= exec_instance.get(m.getRing())){
			return;
		}else if(m.isSkip()){ // skip skip-instances
			exec_instance.put(m.getRing(),m.getInstnce());
			return;
		}
		
		// recover if a not ascending instance arrives 
		if(m.getInstnce()-1 != exec_instance.get(m.getRing())){
			logger.info("Replica start recovery: " + exec_instance.get(m.getRing()) + " to " + (m.getInstnce()-1));
			while(m.getInstnce()-1 > exec_instance.get(m.getRing())){
				exec_instance = installState();
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
		exec_instance.put(m.getRing(),m.getInstnce());
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

    static class SendFile implements HttpHandler {
    	
    	private final File file;
    	
    	public SendFile(String file){
    		this.file = new File(file);
    	}
    	
        public void handle(HttpExchange t) throws IOException {
            FileInputStream fis = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fis);

        	byte[] b = new byte[(int)file.length()];
            bis.read(b, 0, b.length); //FIXME: keeps file in mem!
            bis.close();
            fis.close();

            t.sendResponseHeaders(200, b.length);
            OutputStream os = t.getResponseBody();
            os.write(b);
            os.close();
        }
    }

}
