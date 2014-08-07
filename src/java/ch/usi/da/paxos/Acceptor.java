package ch.usi.da.paxos;
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

import java.io.IOException;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.storage.Decision;

/**
 * Name: Acceptor<br>
 * Description: <br>
 * 
 * Creation date: Apr 1, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Acceptor{

	private final int threadCount = 10;
	
	private final int ID;
	
	private final DatagramChannel channel;
	
	private final Map<Long,Paxos> instanceList = new ConcurrentHashMap<Long,Paxos>(1000);

	/*
	 * The stable storage ...
	 * 
	 * (since the map is initialized with 50'000 values,
	 * this behaves more like an array with nice concurrent
	 * control)
	 * (Java hash of Integer is the integer value!)
	 */
	private final Map<Long,Decision> history = new LinkedHashMap<Long, Decision>(10000,0.75F,false){
		private static final long serialVersionUID = -3708800228030327063L;
		protected boolean removeEldestEntry(Map.Entry<Long, Decision> eldest) {  
			return size() > 15000;                               
	}};

	private ExecutorService  executer = Executors.newFixedThreadPool(threadCount);
	
	/**
	 * Public constructor
	 * 
	 * @param id proposer id
	 * @param config path to config file
	 * @throws IOException 
	 */
	public Acceptor(int id,String config) throws IOException{
		this.ID = id;
		if(Configuration.getConfiguration().isEmpty()){
			Configuration.read(config);
		}

		NetworkInterface i = NetworkInterface.getByName(Configuration.getInterface());
	    channel = DatagramChannel.open(StandardProtocolFamily.INET)
	         .setOption(StandardSocketOptions.SO_REUSEADDR, true)
	         .bind(Configuration.getGroup(PaxosRole.Acceptor))
	         .setOption(StandardSocketOptions.IP_MULTICAST_IF, i);
	    channel.configureBlocking(false);
	    channel.join(Configuration.getGroup(PaxosRole.Acceptor).getAddress(), i);

		for(int n=0;n<threadCount;n++){
			executer.execute(new AcceptorListener(this));
		}
	}
	
	/**
	 * Get the proposer ID
	 * 
	 * @return the ID
	 */
	public int getID() {
		return ID;
	}
	
	/**
	 * Return the local running paxos instance List
	 * 
	 * @return the local instance list
	 */
	public Map<Long,Paxos> getInstanceList(){
		return instanceList;
	}

	/**
	 * Get the history of this acceptor
	 * 
	 * @return the history
	 */
	public Map<Long,Decision> getHistory(){
		return history;
	}
		
	/**
	 * Get the DatagramChannel
	 * 
	 * @return the channel
	 */
	public DatagramChannel getChannel(){
		return channel;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length > 1){
			new Acceptor(Integer.parseInt(args[0]),args[1]);
		}else{
			System.err.println("Use acceptor.sh <id> <config>!");
			System.exit(1);
		}
	}

}
