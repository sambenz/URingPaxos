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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.thrift.transport.TTransportException;

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
 * @author Samuel Benz <benz@geoid.ch>
 */
public class Client implements Receiver {

	private final PartitionManager partitions;
				
	private final UDPListener udp;
	
	private Map<Integer,Response> open_cmd = new HashMap<Integer,Response>();
	
	private Map<Integer, BlockingQueue<Response>> send_queues = new HashMap<Integer, BlockingQueue<Response>>();
	
	private final InetAddress ip;
	
	private final int port;
	
	private final Map<Integer,Integer> connectMap;
	
	public Client(PartitionManager partitions,Map<Integer,Integer> connectMap) throws IOException {
		this.partitions = partitions;
		this.connectMap = connectMap;
		ip = getHostAddress(false);
		port = 5000 + new Random(Thread.currentThread().getId()).nextInt(15000);
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
		    	if(line.length > 2){
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
		        		List<Command> response = r.getResponse(20000); // wait response
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
		
	/**
	 * Send a command (use same ID your Response ended in a timeout)
	 * 
	 * (the commands will be batched to larger Paxos instances)
	 * 
	 * @param cmd The command to send
	 * @return A Response object on which you can wait
	 * @throws TTransportException
	 */
	public synchronized Response send(Command cmd) throws Exception {
		Response r = new Response(cmd);
		open_cmd.put(cmd.getID(),r);
    	if(cmd.getType() == CommandType.GETRANGE){
    		int ring  = partitions.getGlobalRing();
    		if(!send_queues.containsKey(ring)){
    			send_queues.put(ring,new LinkedBlockingQueue<Response>());
    			Thread t = new Thread(new BatchSender(ring,null,this));
    			t.setName("BatchSender-" + ring);
    			t.start();
    		}
    		send_queues.get(ring).add(r);
    	}else{
		    Partition p = partitions.getPartition(cmd.getKey());
    		if(p == null){ System.err.println("No partition found for key " + cmd.getKey()); return null; };
    		int ring = partitions.getRing(p);
    		if(!send_queues.containsKey(ring)){
    			send_queues.put(ring,new LinkedBlockingQueue<Response>());
    			Thread t = new Thread(new BatchSender(ring,p,this));
    			t.setName("BatchSender-" + ring);
    			t.start();
    		}
    		send_queues.get(ring).add(r);
    	}
    	return r;		
	}
	
	@Override
	public void receive(Message m) {
		//TODO: how handle GETRANGE responses from different partitions?
		// un-batch response
		Map<Integer,List<Command>> ml = new HashMap<Integer,List<Command>>();
		for(Command c : m.getCommands()){
			if(!ml.containsKey(c.getID())){
				List<Command> l = new ArrayList<Command>();
				ml.put(c.getID(),l);
			}
			ml.get(c.getID()).add(c);
		}
		// set response
		for(Entry<Integer, List<Command>> e : ml.entrySet()){
			if(open_cmd.containsKey(e.getKey())){
				open_cmd.get(e.getKey()).setResponse(e.getValue());
				open_cmd.remove(e.getKey());
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
				final PartitionManager partitions = new PartitionManager(zoo_host);
				partitions.init();
				final Client client = new Client(partitions,connectMap);
				Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
					@Override
					public void run(){
						client.stop();
					}
				});
				client.init();
				
				//for(int i=0;i<1000000;i++){
				//	Command cmd = new Command(i,CommandType.PUT,"user" + i,new byte[1000]);
				//	client.send(cmd);
				//}
				client.readStdin();
				
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
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
	 * @param ipv6 include IPv6 addresses in search
	 * @return return the host IP address or null
	 */
	public static InetAddress getHostAddress(boolean ipv6){
		try {
			Enumeration<NetworkInterface> ni = NetworkInterface.getNetworkInterfaces();
			while (ni.hasMoreElements()){
				NetworkInterface n = ni.nextElement();
				if(n.getDisplayName().equals("eth0")){
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
		} catch (SocketException e) {
			return InetAddress.getLoopbackAddress();
		}
	}

}
