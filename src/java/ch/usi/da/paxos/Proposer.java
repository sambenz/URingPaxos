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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.Promise;

/**
 * Name: Proposer<br>
 * Description: <br>
 * 
 * Creation date: Apr 1, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Proposer implements Watcher {

	private final int threadCount = 5;
	
	/**
	 * Server start time
	 */
	public final long start_time = System.currentTimeMillis();
	
	private final int ID;
	
	private volatile boolean leader = false;
	
	private final ZooKeeper zoo;
	
	private final String path = "/paxos";
	
	private AtomicLong globalInstance = new AtomicLong();

	private final ExecutorService executer = Executors.newFixedThreadPool(threadCount);
	
	private final ExecutorService leader_sender = Executors.newFixedThreadPool(threadCount);
		
	private final ExecutorService reserver = Executors.newFixedThreadPool(1);
	
	private final ExecutorService leader_listener = Executors.newFixedThreadPool(1);
	
	private final BlockingQueue<Value> values = new LinkedBlockingQueue<Value>(500);
	
	private final BlockingQueue<Promise> promises = new LinkedBlockingQueue<Promise>(10);

	private volatile boolean perf_test = false;

	private final AtomicLong send_count = new AtomicLong(0);
	
	private final AtomicLong recv_count = new AtomicLong(0);
	
	private final AtomicLong recv_bytes = new AtomicLong(0);
	
	/**
	 * Public constructor
	 * 
	 * @param id proposer id
	 * @param config path to config file
	 * @throws IOException 
	 */
	public Proposer(int id,String config) throws IOException{
		this.ID = id;
		if(Configuration.getConfiguration().isEmpty()){
			Configuration.read(config);
		}
		zoo = new ZooKeeper(Configuration.zookeeper,3000,this);
		init();
	}
	
	private void init() throws IOException{
		setLeader(true);  // initially everybody is a leader
		
		// register the leader selection watcher
		try {
			if(zoo.exists(path,false) == null){
				zoo.create(path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}
			zoo.getChildren(path, true); // start watching
			zoo.create(path + "/" + this.getID(),null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("Running without zookeeper. So no leader election!");
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
	 * @return the send counter
	 */
	public AtomicLong getSendCount(){
		return send_count;
	}

	/**
	 * @return the receive counter
	 */
	public AtomicLong getRecvCount(){
		return recv_count;
	}

	/**
	 * @return the receive counter
	 */
	public AtomicLong getRecvBytes(){
		return recv_bytes;
	}

	/**
	 * @return if performance test is enabled
	 */
	public boolean isPerfTest(){
		return perf_test;
	}
	
	/**
	 * @param b
	 */
	public void setPerfTest(boolean b){
		perf_test = b;
	}
	
	/**
	 * Is this proposer a leader
	 * 
	 * @return is leader
	 */
	public boolean isLeader(){
		return leader;
	}
	
	/**
	 * Set the leader state
	 * 
	 * @param l true if this is a leader
	 * @throws IOException 
	 */
	public synchronized void setLeader(boolean l) throws IOException{
		boolean old = leader;
		leader = l;
		if(old == false && l == true){ // proposer -> leader
			leader_listener.execute(new LeaderListener(this));
			for(int n=0;n<threadCount;n++){
				executer.execute(new ProposerListener(this));
			}
			reserver.execute(new ProposerReserver(this));
		}else if(old == true && l == false){ // leader -> proposer
			for(int n=0;n<threadCount;n++){
				leader_sender.execute(new LeaderSender(this));
			}		
		}
	}
	
	/**
	 * Get the paxos instance counter
	 * 
	 * @return the instance
	 */
	public AtomicLong getInstance(){
		return globalInstance;
	}

	/**
	 * This is the blocking queue for the values to propose
	 * 
	 * @return the values queue
	 */
	public BlockingQueue<Value> getValueQueue(){
		return values;
	}
	
	/**
	 * Get the blocking queue with the reserved promises
	 * 
	 * @return the promise queue
	 */
	public BlockingQueue<Promise> getPromiseQueue(){
		return promises;
	}

	@Override
	public void process(WatchedEvent event) {
		int max = -1;
		try {
			if(event.getType() == EventType.NodeChildrenChanged){
				List<String> l = zoo.getChildren(path, true);
				for(String s : l){
					int i = Integer.parseInt(s);
					if(i > max){
						max = i; 
					}
				}
				if(max == this.getID()){
					System.out.println("I'm the new leader :-)!");
					setLeader(true);
				}else{
					System.out.println("I'm a dumb follower :-(!");
					setLeader(false);
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length > 1){
			Proposer p = new Proposer(Integer.parseInt(args[0]),args[1]);
			if(Configuration.getConfiguration().isEmpty()){
				Configuration.read(args[1]);
			}
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		    String s;
		    while ((s = in.readLine()) != null && s.length() != 0){
		    	try{
			    	if(s.contains("start")){
			    		if(!p.isPerfTest()){
			    			p.setPerfTest(true);
			    			Thread t = new Thread(new StatsOutputWriter(p));
			    			t.start();
			    			int conc_values = 80; 
			    			for(int i=0;i<conc_values;i++){
			    				p.getValueQueue().put(new Value(System.currentTimeMillis()+ "" + p.getID(),new byte[6000]));
			    			}
			    		}
			    	}
			    	else{
			    		p.getValueQueue().put(new Value(System.currentTimeMillis()+ "" + p.getID(),s.getBytes()));
			    	}
		    	}catch(NumberFormatException e){
					System.err.println("Error while parsing input as integer!");
				} catch (InterruptedException e) {
				}
		    }
		    System.exit(0);
		}else{
			System.err.println("Use propose.sh <id> <config>!");
			System.exit(1);
		}
	}
}
