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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

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
	
	public final int nodeID;

	public final String token;
	
	private final PartitionManager partitions;
	
	private volatile int min_token;
	
	private volatile int max_token; // if min_token > max_token : replica serves whole key space

	private final UDPSender udp;

	private final ABListener ab;

	private final SortedMap<String,byte[]> db;

	private final String snapshot_file = "/tmp/snapshot.ser";

	private final String state_file = "/tmp/snapshot.state"; // only used to quickly decide if remote snapshot is newer
	
	private final int max_ring = 20;
	
	private Map<Integer,Long> exec_instance = new HashMap<Integer,Long>();
	
	private long exec_cmd = 0;
	
	private int snapshot_modulo = 0; // disabled
	
	private final boolean use_thrift = false;
	
	public Replica(String token, int ringID, int nodeID, int snapshot_modulo, String zoo_host) throws Exception {
		this.nodeID = nodeID;
		this.token = token;
		this.snapshot_modulo = snapshot_modulo;
		this.partitions = new PartitionManager(zoo_host);
		final InetAddress ip = Client.getHostAddress(false);
		partitions.init();
		setPartition(partitions.register(nodeID, ringID, ip, token));
		udp = new UDPSender();
		if(use_thrift){
			ab = partitions.getThriftABListener(ringID,nodeID);
		}else{
			ab = partitions.getRawABListener(ringID,nodeID);
		}
		db = new  TreeMap<String,byte[]>();
		
		// remote snapshot transfer server
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
			server.createContext("/state", new SendFile(state_file));        
			server.createContext("/snapshot", new SendFile(snapshot_file));
			server.setExecutor(null); // creates a default executor
			server.start();
		}catch(Exception e){
			logger.error("Replica could not start http server: " + e);
		}
	}

	public void setPartition(Partition partition){
		logger.info("Replica update partition " + partition);
		min_token = partition.getLow();
		max_token = partition.getHigh();
	}

	public void start(){
		partitions.registerPartitionChangeNotifier(this);
		// install old state
		exec_instance = installState();
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
	}

	public void close(){
		ab.close();
		partitions.deregister(nodeID,token);
	}
	
	public boolean checkpoint(){
		if(storeState(exec_instance)){
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
	
	private boolean storeState(Map<Integer,Long> instances){
		try {
			synchronized(db){
				logger.info("Replica start storing state ... ");
				for(Entry<Integer,Long> e : instances.entrySet()){
					db.put("r:" + e.getKey(),e.getValue().toString().getBytes());
				}
				FileOutputStream fs = new FileOutputStream(snapshot_file);
		        ObjectOutputStream os = new ObjectOutputStream(fs);
		        os.writeObject(db);
		        os.flush();
		        fs.getChannel().force(false); // fsync
		        os.close();
				fs = new FileOutputStream(state_file);
				for(Entry<Integer, Long> e : exec_instance.entrySet()){
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
	
	private Map<Integer,Long> installState(){
		Map<Integer,Long> instances = new HashMap<Integer,Long>();
		try {
			String host = null;
			// local
			InputStreamReader isr = new InputStreamReader(new FileInputStream(state_file));
			BufferedReader bin = new BufferedReader(isr);
			String line;
			Map<Integer, Long> state = new HashMap<Integer, Long>();		
			while ((line = bin.readLine()) != null){
				String[] s = line.split("=");
				state.put(Integer.parseInt(s[0]),Long.parseLong(s[1]));
			}
			logger.info("Replica found local snapshot: " + state);
			bin.close();
			// remote
			for(String h : partitions.getReplicas(token)){
				URL url = new URL("http://" + h + ":8080/state"); //TODO: must only ask GSQ replicas
				HttpURLConnection con = (HttpURLConnection)url.openConnection();
				isr = new InputStreamReader(con.getInputStream());
				BufferedReader in = new BufferedReader(isr);
				Map<Integer, Long> nstate = new HashMap<Integer, Long>();
				while ((line = in.readLine()) != null){
					String[] s = line.split("=");
					nstate.put(Integer.parseInt(s[0]),Long.parseLong(s[1]));
				}
				logger.info("Replica found remote snapshot: " + nstate + " (" + h + ")");
				in.close();
				if(newerState(nstate,state)){
					state = nstate;
					host = h;
				}
			}
			synchronized(db){
				InputStream in;
				if(host != null){
					URL url = new URL(host + "/state");
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
						instances.put(i,Long.valueOf(new String(b)));
					}
				}
			}
		} catch (IOException | ClassNotFoundException e) {
			if(!exec_instance.isEmpty()){
				return exec_instance;
			}else{ // init to 0
				instances.put(partitions.getGlobalRing(),0L);
				for(Partition p : partitions.getPartitions()){
					instances.put(p.getRing(),0L);
				}				
			}
			logger.error(e);
		}
		logger.info("Replica installed snapshot instance: " + instances);
		return instances;
	}

	private boolean newerState(Map<Integer, Long> nstate, Map<Integer, Long> state) {
		for(Entry<Integer, Long> e : state.entrySet()){
			long i = e.getValue();
			long ni = nstate.get(e.getKey());
			if(ni > i){
				return true;
			}
		}
		return false;
	}

	@Override
	public void receive(Message m) {

		// skip already executed commands
		if(m.getInstnce() <= exec_instance.get(m.getRing())){
			return;
		}else if(m.isSkip()){ // skip skip-instances
			exec_instance.put(m.getRing(),m.getInstnce());
			return;
		}

		logger.debug("Replica received ring " + m.getRing() + " instnace " + m.getInstnce() + " (" + m + ")");

		List<Command> cmds = new ArrayList<Command>();

		// recover if a not ascending instance arrives 
		if(m.getInstnce()-1 != exec_instance.get(m.getRing())){
			while(m.getInstnce()-1 > exec_instance.get(m.getRing())){
				logger.info("Replica start recovery: " + exec_instance.get(m.getRing()) + " to " + (m.getInstnce()-1));				
				exec_instance = installState();
			}
		}
		
		// snapshot
		exec_cmd++;
		if(snapshot_modulo > 0 && exec_cmd % snapshot_modulo == 0){
			checkpoint(); //TODO: can we write this asynchronous?
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
					String end_key = new String(c.getValue());
					int count = c.getCount();
					//logger.debug("getrange " + start_key + " -> " + end_key + " (" + MurmurHash.hash32(start_key) + "->" + MurmurHash.hash32(end_key) + ")");
					//logger.debug("tailMap:" + db.tailMap(start_key).keySet() + " count:" + count);
					int msg = 0;
					for(Entry<String,byte[]> e : db.tailMap(start_key).entrySet()){
						if(msg >= count || (!end_key.isEmpty() && e.getKey().compareTo(end_key) > 0)){ break; }
			    		Command cmd = new Command(c.getID(),CommandType.RESPONSE,e.getKey(),e.getValue());
			    		cmds.add(cmd);
			    		msg++;
					}
					if(msg == 0){
			    		Command cmd = new Command(c.getID(),CommandType.RESPONSE,"",null);
			    		cmds.add(cmd);						
					}
					break;
				default:
					System.err.println("Receive RESPONSE as Command!"); break;
		    	}
			}
		}
		exec_instance.put(m.getRing(),m.getInstnce());
		Message msg = new Message(m.getID(),token,m.getFrom(),cmds);
		udp.send(msg);
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
