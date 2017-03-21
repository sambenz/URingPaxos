package ch.usi.da.paxos.ring;
/* 
 * Copyright (c) 2015 Università della Svizzera italiana (USI)
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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.paxos.Util;
import ch.usi.da.paxos.api.Learner;
import ch.usi.da.paxos.api.PaxosNode;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.api.Proposer;
import ch.usi.da.paxos.lab.DummyWatcher;

/**
 * Name: Node<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Node implements PaxosNode {

	private final Logger logger;

	private final int nodeID;
	
	private final int groupID;
		
	private final InetAddress ip;

	private final List<ZooKeeper> zoos = new ArrayList<ZooKeeper>(); // hold refs to close
	
	private final String zoo_host;
	
	private final List<RingDescription> rings;
	
	private boolean running = false;

	private final Map<Integer, Proposer> ringProposer = new HashMap<Integer, Proposer>(); 

	private Learner learner = null;
	

	/**
	 * @param nodeID
	 * @param zoo_host
	 * @param rings
	 */
	public Node(int nodeID,String zoo_host,List<RingDescription> rings) {
		this(nodeID,-1,zoo_host,rings);
	}

	/**
	 * @param nodeID
	 * @param groupID
	 * @param zoo_host
	 * @param rings
	 */
	public Node(int nodeID,int groupID,String zoo_host,List<RingDescription> rings) {
		this.logger = Logger.getLogger(Node.class);
		this.nodeID = nodeID;
		this.groupID = groupID;
		this.zoo_host = zoo_host;
		this.rings = rings;
		this.ip = Util.getHostAddress();
	}

	public void start() throws IOException, KeeperException, InterruptedException {
		try {
			int pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
			logger.info("PID: " + pid);
		} catch (NumberFormatException | IOException e) {
		}
		for(RingDescription ring : rings){
			update(ring);
		}
		if(isMultiLearner(rings)){
			logger.error("MultiRingLearner will be replaced by ElasticLearner!");
			@SuppressWarnings("deprecation")
			MultiLearnerRole mr = new MultiLearnerRole(rings);
			learner = mr;
			Thread t = new Thread(mr);
			t.setName("MultiRingLearner");
			t.start();
		}
		running = true;
	}

	private void update(RingDescription ring) throws IOException, KeeperException, InterruptedException {
		// ring socket port
		Random rand = new Random();
		int port = 2000 + rand.nextInt(1000); // assign port between 2000-3000
		InetSocketAddress addr = new InetSocketAddress(ip,port);
		// create ring manager
		ZooKeeper zoo = new ZooKeeper(zoo_host,3000,new DummyWatcher());
		zoos.add(zoo);
		RingManager rm = new RingManager(this,ring.getRingID(),addr,zoo,"/ringpaxos");
		ring.setRingManager(rm);
		rm.init();
		// register and start roles
		for(PaxosRole role : ring.getRoles()){
			if(role.equals(PaxosRole.Proposer)){
				ProposerRole r = new ProposerRole(rm);
				logger.debug("Node register role: " + role + " at node " + nodeID + " in ring " + ring.getRingID());
				rm.registerRole(role);
				ringProposer.put(ring.getRingID(), r);
				Thread t = new Thread(r);
				t.setName(role.toString());
				t.start();
			}else if(role.equals(PaxosRole.Acceptor)){
				Role r = new AcceptorRole(rm);
				logger.debug("Node register role: " + role + " at node " + nodeID + " in ring " + ring.getRingID());
				rm.registerRole(role);		
				Thread t = new Thread(r);
				t.setName(role.toString());
				t.start();						
			}else if(role.equals(PaxosRole.Learner) && learner == null && !isMultiLearner(rings)){
				ElasticLearnerRole r = new ElasticLearnerRole(ring);
				logger.debug("Node register role: " + role + " at node " + nodeID + " in ring " + ring.getRingID());
				learner = r;
				Thread t = new Thread(r);
				t.setName("ElasticLearner");
				t.start();
			}
		}		
	}
	
	public void stop() throws InterruptedException{
		for(RingDescription r : rings){
			RingManager ring = r.getRingManager();
			if(ring.getNetwork().getAcceptor() != null){
		    	((AcceptorRole)ring.getNetwork().getAcceptor()).getStableStorage().close();
		    }
        	ring.getNetwork().disconnectClient();
        	ring.getNetwork().closeServer();
		}
		for(ZooKeeper zoo : zoos){
			zoo.close();
		}
		running = false;
	}
	
	private boolean isMultiLearner(List<RingDescription> rings) {
		int learner_count = 0;
		for(RingDescription ring : rings){
			if(ring.getRoles().contains(PaxosRole.Learner)){
				learner_count++;
			}
		}
		return learner_count > 1 ? true : false;
	}

	@Override
	public int getNodeID(){
		return nodeID;
	}

	@Override
	public int getGroupID(){
		return groupID;
	}

	@Override
	public InetAddress getInetAddress(){
		return ip;
	}
	
	@Override
	public List<RingDescription> getRings(){
		return rings;
	}
						
	@Override
	public Learner getLearner() {
		if (!running) {
			throw new RuntimeException("Paxos node is not running. Call node.start()");
		}
		return learner;
	}

	@Override
	public Proposer getProposer(int ringID) {
		if (!running) {
			throw new RuntimeException("Paxos node is not running. Call node.start()");
		}
		return ringProposer.get(ringID);
	}

	@Override
	public boolean updateRing(RingDescription ring) {
		try {
			for(RingDescription r : rings){
				if(r.getRingID() == ring.getRingID()){
					r.getRoles().addAll(ring.getRoles());
					ring.setRingManager(r.getRingManager());
					ring.getRoles().clear();
					ring.getRoles().addAll(r.getRoles());
					return true;
				}
			}
			update(ring);
			rings.add(ring);
			return true;
		} catch (Exception e){
			logger.debug("Error updating ring " + ring);
		}
		return false;
	}
	
}
