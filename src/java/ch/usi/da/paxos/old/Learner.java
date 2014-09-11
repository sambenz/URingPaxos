package ch.usi.da.paxos.old;
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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.storage.Decision;


/**
 * Name: Learner<br>
 * Description: <br>
 * 
 * Creation date: Apr 9, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Learner {

	private final int threadCount = 5;
	
	private final int ID;
	
	private final DatagramChannel channel;
	
	private final Map<Long,Majority> instanceList = new ConcurrentHashMap<Long,Majority>(1000);

	private final AtomicLong instance = new AtomicLong(1);
	
	private final BlockingQueue<Decision> decisions = new LinkedBlockingQueue<Decision>();

	private final BlockingQueue<Long> requests = new LinkedBlockingQueue<Long>();
	
	private ExecutorService executer = Executors.newFixedThreadPool(threadCount);
	
	private ExecutorService writer = Executors.newFixedThreadPool(1);	
	
	/**
	 * Public constructor
	 * 
	 * @param id proposer id
	 * @param config path to config file
	 * @throws IOException 
	 */
	public Learner(int id,String config) throws IOException{
		this.ID = id;
		if(Configuration.getConfiguration().isEmpty()){
			Configuration.read(config);
		}
		NetworkInterface i = NetworkInterface.getByName(Configuration.getInterface());
	    this.channel = DatagramChannel.open(StandardProtocolFamily.INET)
	         .setOption(StandardSocketOptions.SO_REUSEADDR, true)
	         .bind(Configuration.getGroup(PaxosRole.Learner))
	         .setOption(StandardSocketOptions.IP_MULTICAST_IF, i);
	    this.channel.configureBlocking(false);
	    this.channel.join(Configuration.getGroup(PaxosRole.Learner).getAddress(), i);
		for(int n=0;n<threadCount;n++){
			executer.execute(new LearnerListener(this));
		}
		writer.execute(new LearnerWriter(this));
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
	 * Get the datagram channel
	 * 
	 * @return the channel
	 */
	public DatagramChannel getChannel(){
		return channel;
	}
	
	/**
	 * Return the instance list
	 * 
	 * @return the instance list
	 */
	public AtomicLong getInstance(){
		return instance;
	}
	
	/**
	 * Return the decision list
	 * 
	 * @return the decision list
	 */
	public BlockingQueue<Decision> getDecisions(){
		return decisions;
	}
	
	/**
	 * Stores a list of missing instances
	 * 
	 * @return the requests list
	 */
	public BlockingQueue<Long> getRequests(){
		return requests;
	}
	
	/**
	 * Return the local running majority instance List
	 * 
	 * @return the local instance list
	 */
	public Map<Long,Majority> getInstanceList(){
		return instanceList;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length > 1){
			new Learner(Integer.parseInt(args[0]),args[1]);
		}else{
			System.err.println("Use learn.sh <id> <config>!");
			System.exit(1);
		}
	}

}
