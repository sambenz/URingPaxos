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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.ABSender;
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
	
	private final Map<Integer,Integer> connectMap;
			
	private final UDPListener udp;
	
	private Map<Integer,Response> responses = new HashMap<Integer,Response>();
	
	private final InetAddress ip;
	
	private final int port;
	
	private AtomicInteger msg_id = new AtomicInteger(0); 
		
	public Client(PartitionManager partitions,Map<Integer,Integer> connectMap) throws IOException {
		this.partitions = partitions;
		this.connectMap = connectMap;
		ip = getHostAddress(false);
		port = 3000 + new Random().nextInt(1000);
		udp = new UDPListener(port);
		Thread t = new Thread(udp);
		t.start();
	}

	public void init() {
		udp.registerReceiver(this);		
	}
	
	public void readStdin() throws TTransportException {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	    String s;
	    try {
	    	int id = 0;
	    	List<Command> cmds = new ArrayList<Command>();
		    while((s = in.readLine()) != null && s.length() != 0){
		    	// read input
		    	String[] line = s.split("\\s+");
		    	if(line.length > 2){
		    		try{
		    			cmds.add(new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],line[2].getBytes()));
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(line.length > 1){
		    		try{
		    			cmds.add(new Command(id,CommandType.valueOf(line[0].toUpperCase()),line[1],new byte[0]));
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else{
		    		System.out.println("Add command: <PUT|GET|GETRANGE|DELETE> key <value>");
		    	}
		    	// send one command
		    	if(cmds.size() > 0){
		    		Response r = null;
		        	if((r = send(cmds)) != null){ // is abroadcasted
		        		Message response = r.getResponse(20000); // wait response
		        		if(response != null){
		        			for(Command c : response.getCommands()){
		    	    			if(c.getType() == CommandType.RESPONSE){
		    	    				System.out.println("  -> " + new String(c.getValue()));
		    	    			}			    				
		        			}
		        		}else{
		        			System.err.println("Did not receive response from replicas: " + cmds);
		        			responses.remove(id);
		        		}
		        	}else{
		        		System.err.println("Could not send command: " + cmds);
		        		responses.remove(id);
		        	}
		        	id++;
			    	cmds.clear();
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
	
	public Response send(List<Command> cmds) throws TTransportException {
		//TODO: batching with Map<Ring,List<Command>> 
		Response r = new Response();
		int id = msg_id.incrementAndGet();
		responses.put(id,r);
		Message m = new Message(id,ip.getHostAddress() + ":" + port,cmds);
    	long ret = 0;
    	if(cmds.get(0).getType() == CommandType.GETRANGE){
    		ABSender sender = partitions.getABSender(null,connectMap.get(partitions.getGlobalRing()));
    		if(sender != null){
    			ret = sender.abroadcast(m);
    		}
    	}else{
		    Partition p = partitions.getPartition(cmds.get(0).getKey());
    		if(p == null){ System.err.println("No partition found for key " + cmds.get(0).getKey()); return null; };
    		ABSender sender = partitions.getABSender(p,connectMap.get(p.getRing()));
    		if(sender != null){
    			ret = sender.abroadcast(m);
    		}
    	}
    	if(ret > 0){
    		return r;
    	}
    	return null;
	}
	
	@Override
	public void receive(Message m) {
		//TODO: how handle GETRANGE responses from different partitions?
		if(responses.containsKey(m.getID())){
			Response r = responses.get(m.getID());
			r.setResponse(m);
			responses.remove(m.getID());
		}		
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
				final ZooKeeper zoo = new ZooKeeper(zoo_host,3000,null);
				final PartitionManager partitions = new PartitionManager(zoo);
				partitions.init();
				final Client client = new Client(partitions,connectMap);
				Runtime.getRuntime().addShutdownHook(new Thread(){
					@Override
					public void run(){
						client.stop();
						try {
							zoo.close();
						} catch (InterruptedException e) {
						}
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
