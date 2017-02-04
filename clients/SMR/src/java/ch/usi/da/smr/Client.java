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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.Util;
import ch.usi.da.paxos.message.Control;
import ch.usi.da.paxos.message.ControlType;
import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.Receiver;
import ch.usi.da.smr.transport.Response;
import ch.usi.da.smr.transport.UDPListener;

/**
 * Name: Client<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Client implements Receiver {
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
	
	private final static Logger logger = Logger.getLogger(Client.class);

	private final PartitionManager partitions;
				
	private final UDPListener udp;
	
	private Map<Integer,Response> commands = new ConcurrentHashMap<Integer,Response>();

	private Map<Integer,List<Command>> responses = new ConcurrentHashMap<Integer,List<Command>>();

	private Map<Integer,List<String>> await_response = new ConcurrentHashMap<Integer,List<String>>();
	
	private final List<Long> latency = Collections.synchronizedList(new ArrayList<Long>());
	
	private Map<Integer, BlockingQueue<Response>> send_queues = new HashMap<Integer, BlockingQueue<Response>>();
	
	// we need only one response per replica group
	Set<Long> delivered = Collections.newSetFromMap(new LinkedHashMap<Long, Boolean>(){
		private static final long serialVersionUID = -5674181661800265432L;
		protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
	        return size() > 50000;
	    }
	});

	private final InetAddress ip;
	
	private final int port;
	
	private final Map<Integer,Integer> connectMap;
	
	private int controlID = 0;
	
	public Client(PartitionManager partitions,Map<Integer,Integer> connectMap) throws IOException {
		this.partitions = partitions;
		this.connectMap = connectMap;
		ip = Util.getHostAddress();
		port = 5000 + new Random().nextInt(15000);
		udp = new UDPListener(port);
		Thread t = new Thread(udp);
		t.setName("UDPListener");
		t.start();
	}

	public void init() {
		udp.registerReceiver(this);		
	}

	public void readStdin() throws Exception {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	    String s;
	    try {
	    	int id = 0;
	    	Command cmd = null;
		    while((s = in.readLine()) != null && s.length() != 0){
		    	// read input
		    	String[] line = s.split("\\s+");
		    	if(s.startsWith("sub")){
		    		prepareGlobal(controlID++,1);
		    		Thread.sleep(8000);
		    		subscribeGlobal(controlID++,1);
		    		cmd = null;
		    	}else if(s.startsWith("unsub")){
		    		unsubscribeGlobal(controlID++,1);
		    		cmd = null;
		    	}else if(s.startsWith("start")){
		    		cmd = null;
		    		final int concurrent_cmd; // # of threads
		    		final int send_per_thread;
		    		final int value_size;
		    		final int key_count = 50000; // 50k * 1k byte memory needed at replica
		    		String[] sl = s.split(" ");
		    		if(sl.length > 1){
			    		concurrent_cmd = Integer.parseInt(sl[1]);
			    		send_per_thread = Integer.parseInt(sl[2]);
			    		value_size = Integer.parseInt(sl[3]);		    			
		    		}else{
			    		concurrent_cmd = 70; //10;
			    		send_per_thread = 15000;
			    		value_size = 1024;		    					    			
		    		}
		    		final AtomicInteger send_id = new AtomicInteger(0);
		    		final AtomicLong stat_latency = new AtomicLong();
		    		final AtomicLong stat_command = new AtomicLong();		    		
		    		latency.clear();
		    		final CountDownLatch await = new CountDownLatch(concurrent_cmd);
		    		final Thread stats = new Thread("ClientStatsWriter"){		    			
		    			private long last_time = System.nanoTime();
		    			private long last_sent_count = 0;
		    			private long last_sent_time = 0;
		    			@Override
		    			public void run() {
		    				while(await.getCount() > 0){
		    					try {
		    						long time = System.nanoTime();
		    						long sent_count = stat_command.get() - last_sent_count;
		    						long sent_time = stat_latency.get() - last_sent_time;
		    						float t = (float)(time-last_time)/(1000*1000*1000);
		    						float count = sent_count/t;
		    						logger.info(String.format("Client sent %.1f command/s avg. latency %.0f ns",count,sent_time/count));
		    						logger.debug("commands " + commands.size() + " responses " + responses.size());
		    						last_sent_count += sent_count;
		    						last_sent_time += sent_time;
		    						last_time = time;
		    						Thread.sleep(100);
		    					} catch (InterruptedException e) {
		    						Thread.currentThread().interrupt();
		    						break;				
		    					}
		    				}
		    			}
		    		};
		    		stats.start();
		    		logger.info("Start performance testing with " + concurrent_cmd + " threads.");
		    		logger.info("(values_per_thread:" + send_per_thread + " value_size:" + value_size + ")");
	    			Thread c = new Thread("Experiemnt controller"){
						@Override
						public void run(){
							try {
								
								String token1 = "0";
								String token2 = "7FFFFFFF";
								String token3 = "3FFFFFFF";
								String token4 = "-3FFFFFFF";
								
								Thread.sleep(12000);
								
								int prepare_time = 5000;
								int wait_before_getrange = 0;
								int wait_before_subscribe = 1000;
								int cmd_timeout = 0;
							
								// 1,2
								long time1 = System.nanoTime();
								prepareGlobal(1,1);
								prepareGlobal(2,2);
								Thread.sleep(prepare_time);
								subscribeGlobal(1,1);
								subscribeGlobal(2,2);
								Thread.sleep(wait_before_getrange);
								int id = send_id.incrementAndGet();
								Command cmd = new Command(id,CommandType.GETRANGE,"user1,2",("user2;" + token1 + ";" + token2).getBytes(),5);
								Response r = null;
								long time2 = System.nanoTime();
								if((r = send(cmd)) != null){
									r.getResponse(cmd_timeout); // wait response
									long lat1 = System.nanoTime() - time1;
									long lat2 = System.nanoTime() - time2;									
									logger.info("GETRANGE 1 " + lat1 + " " + lat2);
								}
								Thread.sleep(wait_before_subscribe);
								unsubscribeGlobal(3,1);
								unsubscribeGlobal(4,2);
								Thread.sleep(1000);
								
								// 1,3
								Thread.sleep(6000);
								time1 = System.nanoTime();
								prepareGlobal(5,1);
								prepareGlobal(6,3);
								Thread.sleep(prepare_time);
								subscribeGlobal(5,1);
								subscribeGlobal(6,3);
								Thread.sleep(wait_before_getrange);
								id = send_id.incrementAndGet();
								cmd = new Command(id,CommandType.GETRANGE,"user1,3",("user2;" + token1 + ";" + token3).getBytes(),5);
								r = null;
								time2 = System.nanoTime();
								if((r = send(cmd)) != null){
									r.getResponse(cmd_timeout); // wait response
									long lat1 = System.nanoTime() - time1;
									long lat2 = System.nanoTime() - time2;									
									logger.info("GETRANGE 1,3 " + lat1 + " " + lat2);
								}
								Thread.sleep(wait_before_subscribe);
								unsubscribeGlobal(7,1);
								unsubscribeGlobal(8,3);
								Thread.sleep(1000);
								
								// 1,4
								Thread.sleep(6000);
								time1 = System.nanoTime();
								prepareGlobal(9,1);
								prepareGlobal(10,4);
								Thread.sleep(prepare_time);
								subscribeGlobal(9,1);
								subscribeGlobal(10,4);
								Thread.sleep(wait_before_getrange);
								id = send_id.incrementAndGet();
								cmd = new Command(id,CommandType.GETRANGE,"user1,4",("user2;" + token1 + ";" + token4).getBytes(),5);
								r = null;
								time2 = System.nanoTime();
								if((r = send(cmd)) != null){
									r.getResponse(cmd_timeout); // wait response
									long lat1 = System.nanoTime() - time1;
									long lat2 = System.nanoTime() - time2;									
									logger.info("GETRANGE 1,4 " + lat1 + " " + lat2);
								}
								Thread.sleep(wait_before_subscribe);
								unsubscribeGlobal(11,1);
								unsubscribeGlobal(12,4);
								Thread.sleep(1000);
								
								// 2,3
								Thread.sleep(6000);
								time1 = System.nanoTime();
								prepareGlobal(13,2);
								prepareGlobal(14,3);
								Thread.sleep(prepare_time);
								subscribeGlobal(13,2);
								subscribeGlobal(14,3);
								Thread.sleep(wait_before_getrange);
								id = send_id.incrementAndGet();
								cmd = new Command(id,CommandType.GETRANGE,"user2,3",("user2;" + token2 + ";" + token3).getBytes(),5);
								r = null;
								time2 = System.nanoTime();
								if((r = send(cmd)) != null){
									r.getResponse(cmd_timeout); // wait response
									long lat1 = System.nanoTime() - time1;
									long lat2 = System.nanoTime() - time2;									
									logger.info("GETRANGE 2,3 " + lat1 + " " + lat2);
								}
								Thread.sleep(wait_before_subscribe);
								unsubscribeGlobal(15,2);
								unsubscribeGlobal(16,3);
								Thread.sleep(1000);
								
								// 2,4
								
								// 3,4
								
								// 1,2,3
								Thread.sleep(6000);
								time1 = System.nanoTime();
								prepareGlobal(17,1);
								prepareGlobal(18,2);
								prepareGlobal(19,3);
								Thread.sleep(prepare_time);
								subscribeGlobal(17,1);
								subscribeGlobal(18,2);
								subscribeGlobal(19,3);								
								Thread.sleep(wait_before_getrange);
								id = send_id.incrementAndGet();
								cmd = new Command(id,CommandType.GETRANGE,"user1,2,3",("user2;" + token1 + ";" + token2 + ";" + token3).getBytes(),5);
								r = null;
								time2 = System.nanoTime();
								if((r = send(cmd)) != null){
									r.getResponse(cmd_timeout); // wait response
									long lat1 = System.nanoTime() - time1;
									long lat2 = System.nanoTime() - time2;									
									logger.info("GETRANGE 1,2,3 " + lat1 + " " + lat2);
								}
								Thread.sleep(wait_before_subscribe);
								unsubscribeGlobal(20,1);
								unsubscribeGlobal(21,2);								
								unsubscribeGlobal(22,3);
								Thread.sleep(1000);
								
								// 1,2,4
								// 1,3,4
								// 2,3,4
								
								// 1,2,3,4
								Thread.sleep(6000);
								time1 = System.nanoTime();
								prepareGlobal(23,1);
								prepareGlobal(24,2);
								prepareGlobal(25,3);
								prepareGlobal(26,4);
								Thread.sleep(prepare_time);
								subscribeGlobal(23,1);
								subscribeGlobal(24,2);
								subscribeGlobal(25,3);
								subscribeGlobal(26,4);
								Thread.sleep(wait_before_getrange);
								id = send_id.incrementAndGet();
								cmd = new Command(id,CommandType.GETRANGE,"user1,2,3,4",("user2;" + token1 + ";" + token2 + ";" + token3 + ";" + token4).getBytes(),5);
								r = null;
								time2 = System.nanoTime();
								if((r = send(cmd)) != null){
									r.getResponse(cmd_timeout); // wait response
									long lat1 = System.nanoTime() - time1;
									long lat2 = System.nanoTime() - time2;									
									logger.info("GETRANGE 1,2,3,4 " + lat1 + " " + lat2);
								}
								Thread.sleep(wait_before_subscribe);
								unsubscribeGlobal(27,1);
								unsubscribeGlobal(28,2);
								unsubscribeGlobal(29,3);
								unsubscribeGlobal(30,4);
								
							} catch (Exception e) {
							}
						}
					};
					c.start();
		    		for(int i=0;i<concurrent_cmd;i++){
		    			Thread t = new Thread("Command Sender " + i){
							@Override
							public void run(){
								int send_count = 0;
								while(send_count < send_per_thread){
									int id = send_id.incrementAndGet();
									Command cmd = new Command(id,CommandType.PUT,"user" + (id % key_count), new byte[value_size]);
									Response r = null;
									try{
										long time = System.nanoTime();
										if((r = send(cmd)) != null){
											r.getResponse(1000); // wait response
											long lat = System.nanoTime() - time;
											stat_latency.addAndGet(lat);
											stat_command.incrementAndGet();
											//TODO: latency.add(lat);
										}
									} catch (Exception e){
										logger.error("Error in send thread!",e);
									}
									send_count++;
								}
								await.countDown();
								logger.debug("Thread terminated.");
							}
						};
						t.start();
		    		}
		    		await.await(); // wait until finished
		    		id = send_id.incrementAndGet();
		    		Thread.sleep(5000);
		    		printHistogram();
		    	}else if(line.length > 3){
		    		try{
		    			String arg2 = line[2];
		    			if(arg2.equals(".")){ arg2 = ""; } // simulate empty string
		    			cmd = new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],arg2.getBytes(),Integer.parseInt(line[3]));
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(line.length > 2){
		    		try{
		    			cmd = new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],line[2].getBytes());
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(line.length > 1){
		    		try{
		    			cmd = new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],new byte[0]);
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else{
		    		System.out.println("Add command: <PUT|GET|GETRANGE|DELETE> key <value>");
		    	}
		    	// send a command
		    	if(cmd != null){
		    		Response r = null;
		        	if((r = send(cmd)) != null){
		        		List<Command> response = r.getResponse(10000); // wait response
		        		if(!response.isEmpty()){
		        			for(Command c : response){
		    	    			if(c.getType() == CommandType.RESPONSE){
		    	    				if(c.getValue() != null){
		    	    					System.out.println("  -> " + new String(c.getValue()));
		    	    				}else{
		    	    					System.out.println("<no entry>");
		    	    				}
		    	    			}			    				
		        			}
		        			id++; // re-use same ID if you run into a timeout
		        		}else{
		        			System.err.println("Did not receive response from replicas: " + cmd);
		        		}
		        	}else{
		        		System.err.println("Could not send command: " + cmd);
		        	}
		    	}
		    }
		    in.close();
	    } catch(IOException e){
	    	e.printStackTrace();
	    } catch (InterruptedException e) {
		}
	    stop();
	}

	public void stop(){
		udp.close();
	}

	public void prepareGlobal(int cmdID, int groupID) throws Exception {
		int oldRing = groupID;
		int newRing  = partitions.getGlobalRing();
    	Control c = new Control(cmdID,ControlType.Prepare,groupID,newRing);
		Response r1 = new Response(c);
		commands.put(c.getID(),r1);
		Message m = new Message(1,ip.getHostAddress() + ";" + port,"",null);
		m.setControl(r1.getControl());
		partitions.sendRing(oldRing,m);
		Response r2 = new Response(c);
		commands.put(c.getID(),r2);
		Message m2 = new Message(1,ip.getHostAddress() + ";" + port,"",null);
		m2.setControl(r2.getControl());
		partitions.sendRing(newRing,m2);
	}

	public void subscribeGlobal(int cmdID, int groupID) throws Exception {
		int oldRing = groupID;
		int newRing  = partitions.getGlobalRing();
    	Control c = new Control(cmdID,ControlType.Subscribe,groupID,newRing);
		Response r1 = new Response(c);
		commands.put(c.getID(),r1);
		Message m = new Message(1,ip.getHostAddress() + ";" + port,"",null);
		m.setControl(r1.getControl());
		partitions.sendRing(oldRing,m);
		Response r2 = new Response(c);
		commands.put(c.getID(),r2);
		Message m2 = new Message(1,ip.getHostAddress() + ";" + port,"",null);
		m2.setControl(r2.getControl());
		partitions.sendRing(newRing,m2);
	}
	
	public void unsubscribeGlobal(int cmdID, int groupID) throws Exception {
		int removeRing  = partitions.getGlobalRing();
    	Control c = new Control(cmdID,ControlType.Unsubscribe,groupID,removeRing);
		Response r1 = new Response(c);
		commands.put(c.getID(),r1);
		Message m = new Message(1,ip.getHostAddress() + ";" + port,"",null);
		m.setControl(r1.getControl());
		partitions.sendRing(removeRing,m);
	}

	
	/**
	 * Send a command (use same ID if your Response ended in a timeout)
	 * 
	 * (the commands will be batched to larger Paxos instances)
	 * 
	 * @param cmd The command to send
	 * @return A Response object on which you can wait
	 * @throws Exception
	 */
	public Response send(Command cmd) throws Exception {
		Response r = new Response(cmd);
		commands.put(cmd.getID(),r);
		int partition = -1;
    	if(cmd.getType() == CommandType.GETRANGE){
    		List<String> await = new ArrayList<String>();
    		for(Partition p : partitions.getPartitions()){
    			await.add(p.getID());
    		}
    		//TODO: also subset of partition await_response.put(cmd.getID(),await);
    	}else{
    		partition = partitions.getPartition(cmd.getKey());
			// special case for EC2 inter-region app;
			String single_part = System.getenv("PART");
			if(single_part != null){
				partition = Integer.parseInt(single_part);
			}
    	}
    	synchronized(send_queues){
		if(!send_queues.containsKey(partition)){
    			send_queues.put(partition,new LinkedBlockingQueue<Response>());
    			Thread t = new Thread(new BatchSender(partition,this));
    			t.setName("BatchSender-" + partition);
    			t.start();
    	}}
    	send_queues.get(partition).add(r);
    	return r;		
	}

	@Override
	public synchronized void receive(Message m) {
		logger.debug("Client received ring " + m.getRing() + " instnace " + m.getInstnce() + " (" + m + ")");
		
		// filter away already received replica answers
		long hash = MurmurHash.hash64(m.getID() + "-" + m.getInstnce());
		if(delivered.contains(hash)){
			//logger.debug("dublicate " + m);
			return;
		}else{
			delivered.add(hash);
		}
		
		// un-batch response (multiple responses per command_id)
		for(Command c : m.getCommands()){
			if(!responses.containsKey(c.getID())){
				List<Command> l = new ArrayList<Command>();
				responses.put(c.getID(),l);
			}
			List<Command> l = responses.get(c.getID());
			if(!c.getKey().isEmpty() && !l.contains(c)){
				l.add(c);
			}
		}
		
		// set responses to open commands
		Iterator<Entry<Integer, List<Command>>> it = responses.entrySet().iterator();
		while(it.hasNext()){
			Entry<Integer, List<Command>> e = it.next();
			if(commands.containsKey(e.getKey())){
				if(await_response.containsKey(e.getKey())){ // handle GETRANGE responses from different partitions
					await_response.get(e.getKey()).remove(m.getFrom());
					if(await_response.get(e.getKey()).isEmpty()){
						commands.get(e.getKey()).setResponse(e.getValue());
						commands.remove(e.getKey());
						await_response.remove(e.getKey());
						it.remove();
					}
				}else{
					commands.get(e.getKey()).setResponse(e.getValue());
					commands.remove(e.getKey());
					it.remove();
				}
			}else{
				it.remove();
			}
		}
	}

	public PartitionManager getPartitions() {
		return partitions;
	}

	public Map<Integer, BlockingQueue<Response>> getSendQueues() {
		return send_queues;
	}

	public InetAddress getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public Map<Integer, Integer> getConnectMap() {
		return connectMap;
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
			System.err.println("Plese use \"Client\" \"ring ID,node ID[;ring ID,node ID]\"");
		} else {
			final Map<Integer,Integer> connectMap = parseConnectMap(args[0]);
			try {
				final PartitionManager partitions = new PartitionManager(zoo_host,connectMap);
				partitions.init();
				final Client client = new Client(partitions,connectMap);
				Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
					@Override
					public void run(){
						client.stop();
					}
				});
				client.init();
				client.readStdin();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	private void printHistogram(){
		Map<Long,Long> histogram = new HashMap<Long,Long>();
		int a = 0,b = 0,b2 = 0,c = 0,d = 0,e = 0,f = 0;
		long sum = 0;
		for(Long l : latency){
			sum = sum + l;
			if(l < 1000000){ // <1ms
				a++;
			}else if (l < 10000000){ // <10ms
				b++;
			}else if (l < 25000000){ // <25ms
				b2++;
			}else if (l < 50000000){ // <50ms
				c++;
			}else if (l < 75000000){ // <75ms
				f++;
			}else if (l < 100000000){ // <100ms
				d++;
			}else{
				e++;
			}
			Long key = new Long(Math.round(l/1000));
			if(histogram.containsKey(key)){
				histogram.put(key,histogram.get(key)+1);
			}else{
				histogram.put(key,1L);
			}
		}
		float avg = (float)sum/latency.size()/1000/1000;
		logger.info("client latency histogram: <1ms:" + a + " <10ms:" + b + " <25ms:" + b2 + " <50ms:" + c + " <75ms:" + f + " <100ms:" + d + " >100ms:" + e + " avg:" + avg);
		for(Entry<Long, Long> bin : histogram.entrySet()){ // details for CDF
			logger.info(bin.getKey() + "," + bin.getValue());
		}
	}

	public static Map<Integer, Integer> parseConnectMap(String arg) {
		Map<Integer,Integer> connectMap = new HashMap<Integer,Integer>();
		for(String s : arg.split(";")){
			connectMap.put(Integer.valueOf(s.split(",")[0]),Integer.valueOf(s.split(",")[1]));
		}
		return connectMap;
	}

	@Override
	public boolean is_ready(Integer ring, Long instance) {
		return true;
	}
}
