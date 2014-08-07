package ch.usi.da.dlog;
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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.dlog.message.Command;
import ch.usi.da.dlog.message.CommandType;
import ch.usi.da.dlog.message.Message;
import ch.usi.da.dlog.transport.Receiver;
import ch.usi.da.dlog.transport.Response;
import ch.usi.da.dlog.transport.UDPListener;
import ch.usi.da.paxos.lab.DummyWatcher;

/**
 * Name: Client<br>
 * Description: <br>
 * 
 * Creation date: Apr 07, 2014<br>
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
			byte [] b = new byte[in.available()];
			in.read(b);
			in.close();
			host = new String(b).replace("\n","");
		} catch (IOException e) {
		}
		int pid = 0;
		try {
			pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
		} catch (NumberFormatException | IOException e) {
		}
		System.setProperty("logfilename", host + "-" + pid + ".log");
	}
	
	private final static Logger logger = Logger.getLogger(Client.class);

	private final int logID;
	
	private final int globalID;
	
	private final ZooKeeper zoo;
	
	private final UDPListener udp;
	
	//private final Random rnd = new Random();
	
	private Map<Integer,Response> commands = new HashMap<Integer,Response>();

	private Map<Integer,List<Command>> responses = new HashMap<Integer,List<Command>>();

	private final List<Long> latency = Collections.synchronizedList(new ArrayList<Long>());
	
	private Map<Integer, BlockingQueue<Response>> send_queues = new HashMap<Integer, BlockingQueue<Response>>();
	
	// we need only one response per replica group
	Set<Integer> delivered = Collections.newSetFromMap(new LinkedHashMap<Integer, Boolean>(){
		private static final long serialVersionUID = -5674181661800265432L;
		protected boolean removeEldestEntry(Map.Entry<Integer, Boolean> eldest) {
	        return size() > 50000;
	    }
	});

	private final InetAddress ip;
	
	private final int port;
	
	private final Map<Integer,Integer> connectMap;
	
	public Client(ZooKeeper zoo, Map<Integer,Integer> connectMap) throws IOException {
		this.logID = (int) connectMap.keySet().toArray()[0];
		this.globalID = (int) connectMap.keySet().toArray()[1];
		this.zoo = zoo;
		this.connectMap = connectMap;
		ip = getHostAddress();
		port = 5000 + new Random().nextInt(15000);
		udp = new UDPListener(port);
		Thread t = new Thread(udp);
		t.setName("UDPListener");
		t.start();
	}

	public void init() {
		udp.registerReceiver(this);		
	}

	public ZooKeeper getZooKeeper(){
		return zoo;
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
		    	if(s.startsWith("start")){
		    		final int concurrent_cmd; // # of threads
		    		final int value_size;
		    		final int send_per_thread = 20000000;
		    		String[] sl = s.split(" ");
		    		if(sl.length > 1){
			    		concurrent_cmd = Integer.parseInt(sl[1]);
			    		value_size = Integer.parseInt(sl[2]);		    			
			    		//send_per_thread = Integer.parseInt(sl[3]); // not needed with fixed timeout	    			
		    		}else{
			    		concurrent_cmd = 10;
			    		value_size = 1024;		    			
			    		//send_per_thread = 20000000;		    			
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
		    						last_sent_count += sent_count;
		    						last_sent_time += sent_time;
		    						last_time = time;
		    						Thread.sleep(1000);
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
		    		for(int i=0;i<concurrent_cmd;i++){
		    			Thread t = new Thread("Command Sender " + i){
							@Override
							public void run(){
								int send_count = 0;
								while(send_count < send_per_thread){
									Command cmd = new Command(send_id.incrementAndGet(),CommandType.APPEND,-1,new byte[value_size]);
									Response r = null;
									try{
										long time = System.nanoTime();
										if((r = send(cmd)) != null){
											r.getResponse(20000); // wait response
											long lat = System.nanoTime() - time;
											stat_latency.addAndGet(lat);
											stat_command.incrementAndGet();
											latency.add(lat);
										}
									} catch (Exception e){
									}
									send_count++;
								}
								await.countDown();
							}
						};
						t.start();
		    		}
		    		//await.await(); // wait until finished
		    		Thread.sleep(100000);
		    		printHistogram();
		    		stop();
		    		System.exit(0);
		    	}else if(s.startsWith("append")){
		    		try{
		    			cmd = new Command(id,CommandType.APPEND,-1,line[1].getBytes());
		    		}catch (Exception e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(s.startsWith("multiappend")){
		    		try{
		    			cmd = new Command(id,CommandType.MULTIAPPEND,-1,line[1].getBytes());
		    		}catch (Exception e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(s.startsWith("read")){
		    		try{
		    			if(line.length > 2){
		    				cmd = new Command(id,CommandType.READ,Long.parseLong(line[1]),line[2].getBytes());		    				
		    			}else{
		    				cmd = new Command(id,CommandType.READ,Long.parseLong(line[1]),new byte[0]);
		    			}
		    		}catch (Exception e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(s.startsWith("trim")){
		    		try{
		    			cmd = new Command(id,CommandType.TRIM,Long.parseLong(line[1]),new byte[0]);
		    		}catch (Exception e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else{
		    		System.out.println("Add command: <append|multiappend|read|trim> <value|position>");
		    	}
		    	// send a command
		    	if(cmd != null){
		    		Response r = null;
		        	if((r = send(cmd)) != null){
		        		List<Command> response = r.getResponse(20000); // wait response
		        		if(!response.isEmpty()){
		        			for(Command c : response){
		    	    			if(c.getType() == CommandType.RESPONSE){
		    	    				if(c.getValue().length > 0){
		    	    					System.out.println(new String(c.getValue()));
		    	    				}else{
		    	    					System.out.println("  -> " + c.getPosition());
		    	    				}
		    	    			}			    				
		        			}
		        			id++; // re-use same ID if you run into a timeout
		        		}else{
		        			System.err.println("Did not receive response from server: " + cmd);
		        		}
		        	}else{
		        		System.err.println("Could not send command: " + cmd);
		        	}
		        	cmd = null;
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

	/**
	 * Send a command (use same ID if your Response ended in a timeout)
	 * 
	 * (the commands will be batched to larger Paxos instances)
	 * 
	 * @param cmd The command to send
	 * @return A Response object on which you can wait
	 * @throws Exception
	 */
	public synchronized Response send(Command cmd) throws Exception {
		Response r = new Response(cmd);
		commands.put(cmd.getID(),r);
		int ring = -1;
    	if(cmd.getType() == CommandType.MULTIAPPEND){
    		ring  = globalID;
    	}else{
    		ring = logID;
    		//ring = rnd.nextInt(connectMap.size())+1; // random testing
    	}
    	if(!send_queues.containsKey(ring)){
    		send_queues.put(ring,new LinkedBlockingQueue<Response>());
    		Thread t = new Thread(new BatchSender(ring,this));
    		t.setName("BatchSender-" + ring);
    		t.start();
    	}
    	send_queues.get(ring).add(r);
    	return r;		
	}

	@Override
	public synchronized void receive(Message m) {
		// filter away already received replica answers
		if(delivered.contains(m.getID())){
			return;
		}else{
			delivered.add(m.getID());
		}
		
		// un-batch response (multiple responses per command_id)
		for(Command c : m.getCommands()){
			if(!responses.containsKey(c.getID())){
				List<Command> l = new ArrayList<Command>();
				responses.put(c.getID(),l);
			}
			List<Command> l = responses.get(c.getID());
			if(!l.contains(c)){
				l.add(c);
			}
		}
		
		// set responses to open commands
		Iterator<Entry<Integer, List<Command>>> it = responses.entrySet().iterator();
		while(it.hasNext()){
			Entry<Integer, List<Command>> e = it.next();
			if(commands.containsKey(e.getKey())){
				commands.get(e.getKey()).setResponse(e.getValue());
				commands.remove(e.getKey());
				it.remove();
			}
		}
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
			System.err.println("Plese use \"Client\" \"ring ID,node ID;global ring ID,node ID\"");
		} else {
			final Map<Integer,Integer> connectMap = parseConnectMap(args[0]);
			try {
				final Client client = new Client(new ZooKeeper(zoo_host,3000,new DummyWatcher()),connectMap);
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

	private synchronized void printHistogram(){
		Map<Long,Long> histogram = new HashMap<Long,Long>();
		int a = 0,b = 0,b2 = 0,c = 0,d = 0,e = 0,f = 0;
		long sum = 0;
		synchronized(latency){
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
			Long key = new Long(Math.round(l/1000/1000));
			if(histogram.containsKey(key)){
				histogram.put(key,histogram.get(key)+1);
			}else{
				histogram.put(key,1L);
			}
		}
		}
		float avg = (float)sum/latency.size()/1000/1000;
		logger.info("client latency histogram: <1ms:" + a + " <10ms:" + b + " <25ms:" + b2 + " <50ms:" + c + " <75ms:" + f + " <100ms:" + d + " >100ms:" + e + " avg:" + avg);
		if(logger.isDebugEnabled()){
			for(Entry<Long, Long> bin : histogram.entrySet()){ // details for CDF
				logger.debug(bin.getKey() + "," + bin.getValue());
			}
		}
	}

	public static Map<Integer, Integer> parseConnectMap(String arg) {
		Map<Integer,Integer> connectMap = new HashMap<Integer,Integer>();
		for(String s : arg.split(";")){
			connectMap.put(Integer.valueOf(s.split(",")[0]),Integer.valueOf(s.split(",")[1]));
		}
		return connectMap;
	}

	/**
	 * Get the host IP address
	 * 
	 * Use env(IFACE) to select an interface or
	 * env(IP) to select a specific address
	 * 
	 * to prefer IPv6 use: java.net.preferIPv6Stack=true
	 * 
	 * @return return the host IP address or 127.0.0.1 (::1)
	 */
	public static InetAddress getHostAddress(){
		boolean ipv6 = false;
		String pv4 = System.getProperty("java.net.preferIPv4Stack");
		String pv6 = System.getProperty("java.net.preferIPv6Stack");
		if(pv4 != null && pv4.equals("false")){
			ipv6 = true;
		}		
		if(pv6 != null && pv6.equals("true")){
			ipv6 = true;
		}
		try {
			String iface = System.getenv("IFACE");			
			String public_ip = System.getenv("IP");
			if(public_ip != null){
				return InetAddress.getByName(public_ip);
			}
			Enumeration<NetworkInterface> ni = NetworkInterface.getNetworkInterfaces();
			while (ni.hasMoreElements()){
				NetworkInterface n = ni.nextElement();
				if(iface == null || n.getDisplayName().equals(iface)){
					Enumeration<InetAddress> ia = n.getInetAddresses();
					while(ia.hasMoreElements()){
						InetAddress addr = ia.nextElement();
						if(!(addr.isLinkLocalAddress() || addr.isLoopbackAddress() || addr.toString().contains("192.168.122"))){
							if(addr instanceof Inet6Address && ipv6){
								return addr;
							}else if (addr instanceof Inet4Address && !ipv6){
								return addr;
							}
						}
					}
				}
			}
			return InetAddress.getLoopbackAddress();
		} catch (SocketException | UnknownHostException e) {
			return InetAddress.getLoopbackAddress();
		}
	}
}

