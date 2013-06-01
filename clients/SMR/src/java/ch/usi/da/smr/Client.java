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

	private final ABSender ab;
	
	private final UDPListener udp;
	
	private Map<Integer,Response> responses = new HashMap<Integer,Response>();
	
	private final InetAddress ip;
	
	private final int port;
	
	public Client(String abhost,int abport) throws SocketException, TTransportException {
		ip = getHostAddress(false);
		port = 3000 + new Random().nextInt(1000);
		udp = new UDPListener(port);
		ab = new ABSender(abhost,abport);
		Thread t = new Thread(udp);
		t.start();
	}
	
	public void start() throws TTransportException{
		udp.registerReceiver(this);
		Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
            	udp.close();
				ab.close();
            }
        });
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	    String s;
	    try {
	    	int id = 0;
	    	List<Command> cmds = new ArrayList<Command>();
		    while((s = in.readLine()) != null && s.length() != 0){
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
		    	}else if(s.startsWith(".") && cmds.size() > 0){
		    		Response r = new Response();
		    		responses.put(id,r);
		    		Message m = new Message(id,ip.getHostAddress() + ":" + port,cmds);
			    	int ret = ab.abroadcast(m);
			    	if(ret > 0){
			    		Message response = r.getResponse(5000);
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
		    	}else{
		    		System.out.println("Add command: <PUT|GET|DELETE> key value\nSend with \".\"");
		    	}
		    	
		    }
		    in.close();
	    }catch(IOException e){
	    	e.printStackTrace();
	    } catch (InterruptedException e) {
			e.printStackTrace();
		}
		udp.close();
		ab.close();
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
		String host = "localhost";
		int port = 9081;
		if(args.length > 0){
			String[] s = args[0].split(":");
			host = s[0];
			port = Integer.parseInt(s[1]);
		}
    	Client client;
		try {
			client = new Client(host,port);
	    	client.start();	
		} catch (SocketException | TTransportException e) {
			e.printStackTrace();
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
