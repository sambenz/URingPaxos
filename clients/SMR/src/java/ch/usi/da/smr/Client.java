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

import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.ZooKeeper;

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
	
	private final int clientID;
			
	private final UDPListener udp;
	
	private Map<Integer,Response> responses = new HashMap<Integer,Response>();
	
	private final InetAddress ip;
	
	private final int port;
	
	public Client(PartitionManager partitions,int clientID) throws IOException {
		this.partitions = partitions;
		this.clientID = clientID;
		ip = getHostAddress(false);
		port = 3000 + new Random().nextInt(1000);
		udp = new UDPListener(port);
		Thread t = new Thread(udp);
		t.start();
	}
	
	public void start() throws TTransportException {
		udp.registerReceiver(this);
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
		    			cmds.add(new Command(id,CommandType.valueOf(line[0]),line[1],line[2].getBytes()));
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else if(line.length > 1){
		    		try{
		    			cmds.add(new Command(id,CommandType.valueOf(line[0]),line[1],new byte[0]));
		    		}catch (IllegalArgumentException e){
		    			System.err.println(e.getMessage());
		    		}
		    	}else{
		    		System.out.println("Add command: <PUT|GET|GETRANGE|DELETE> key value");
		    	}
		    	// send one command
		    	if(cmds.size() > 0){
		    		Response r = new Response();
		    		responses.put(id,r);
		    		Message m = new Message(id,ip.getHostAddress() + ":" + port,cmds);
			    	int ret = 0;
			    	if(cmds.get(0).getType() == CommandType.GETRANGE){
			    		ret = partitions.getABSender(null,clientID).abroadcast(m);
			    	}else{
			    		Partition p = partitions.getPartition(Integer.valueOf(cmds.get(0).getKey()));
			    		ret = partitions.getABSender(p,clientID).abroadcast(m);
			    	}
			    	if(ret > 0){ // is abroadcasted
			    		Message response = r.getResponse(5000); // wait response
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
			    	cmds.clear();
			    	id++;	    		
		    	}		    	
		    }
		    in.close();
	    }catch(IOException e){
	    	e.printStackTrace();
	    } catch (InterruptedException e) {
			e.printStackTrace();
		}
	    stop();
	}

	public void stop(){
		udp.close();
	}
	
	@Override
	public void receive(Message m) {
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
			System.err.println("Plese use \"Client\" \"client ID\"");
		} else {
			final int clientID = Integer.parseInt(args[0]);
			try {
				final ZooKeeper zoo = new ZooKeeper(zoo_host,3000,null);
				final PartitionManager partitions = new PartitionManager(zoo);
				partitions.init();
				final Client client = new Client(partitions,clientID);
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
				client.start();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
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
