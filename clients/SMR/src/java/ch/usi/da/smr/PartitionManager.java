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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.smr.transport.ABListener;
import ch.usi.da.smr.transport.ABSender;

/**
 * Name: PartitionManager<br>
 * Description: <br>
 * 
 * Creation date: Aug 28, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class PartitionManager implements Watcher {
	
	private final static Logger logger = Logger.getLogger(PartitionManager.class);
	
	private final ZooKeeper zoo;
	
	private final String prefix = "/smr";
	
	private final String path = prefix + "/partitions";
	
	private final List<Partition> partitions = new LinkedList<Partition>();
	
	private final Map<String,ABSender> proposers = new HashMap<String,ABSender>();
	
	private int global_ring = 16;
	
	public PartitionManager(ZooKeeper zoo) throws IOException {
		this.zoo = zoo;
	}

	public void init() throws KeeperException, InterruptedException{
		zoo.register(this);
		// create path
		String p = "";
		for(String s : path.split("/")){
			if(s.length() > 0){
				p = p + "/" + s;
				if(zoo.exists(p,false) == null){
					zoo.create(p,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				}
			}
		}
		// set default global ring = 16
		if(zoo.exists(path + "/all",false) == null){
			zoo.create(path + "/all","16".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		}
		readPartitions();
	}

	public Partition register(Partition partition, int replicaID){
		String partitionID = Integer.toString(partition.getHigh());
		try {
			if(zoo.exists(path + "/" + partitionID,false) == null){
				zoo.create(path + "/" + partitionID,Integer.toString(partition.getRing()).getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}else{
				zoo.setData(path + "/" + partitionID, Integer.toString(partition.getRing()).getBytes(), -1);
			}
			zoo.create(path + "/" + partitionID + "/" + Integer.toString(replicaID),null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			logger.error(e);
		} catch (InterruptedException e) {
		}
		readPartitions();
		for(Partition p : partitions){
			if(p.getRing() == partition.getRing() && p.getHigh() == partition.getHigh()){
				return p;
			}
		}
		return null;
	}

	public void deregister(Partition partition, int replicaID){
		String partitionID = Integer.toString(partition.getHigh());
		try {
			zoo.delete(path + "/" + partitionID + "/" + Integer.toString(replicaID),-1);
			zoo.delete(path + "/" + partitionID,-1);
		} catch (KeeperException | InterruptedException e) {
		}
	}
	
	public List<Partition> getPartitions(){
		return Collections.unmodifiableList(partitions);
	}

	public int getGlobalRing(){
		return global_ring;
	}
	
	public Partition getPartition(int key){
		for(Partition p : partitions){
			if(key <= p.getHigh()){
				return p;
			}
		}
		return null;
	}

	public int getRing(Partition partition){
		for(Partition p : partitions){
			if(p.equals(partition)){
				return p.getRing();
			}
		}
		return -1;
	}

	public ABListener getABListener(Partition partition, int replicaID) throws TTransportException {
		int ring = getRing(partition);
		String host = "127.0.0.1";
		try {
			host = new String(zoo.getData("/ringpaxos/ring" + ring + "/nodes/" + replicaID,false, null));
			host = host.replaceAll("(;.*)","");
			// Sanity check: is replicaID learner in ring(partition) && global_ring
			if(zoo.exists("/ringpaxos/ring" + ring + "/learners/" + replicaID,false) != null &&
			   zoo.exists("/ringpaxos/ring" + global_ring + "/learners/" + replicaID,false) != null	){
				logger.info("ABListener check for ring " + ring + " and " + global_ring + ": OK!");
			}else{
				logger.warn("ABListener check for ring " + ring + " and " + global_ring + ": Fail!");
			}			
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
		logger.info("ABListener host: " + host + ":" + (9090+replicaID));
		return new ABListener(host,9090+replicaID);
	}
	
	public ABSender getABSender(Partition partition, int clientID) throws TTransportException {
		int ring = global_ring;
		if(partition != null){
			ring = partition.getRing();
		}
		if(proposers.containsKey(ring + "-" + clientID)){
			return proposers.get(ring + "-" + clientID);
		}else{
			String host = "127.0.0.1";
			try {
				host = new String(zoo.getData("/ringpaxos/ring" + ring + "/nodes/" + clientID,false, null));
				host = host.replaceAll("(;.*)","");
				if(zoo.exists("/ringpaxos/ring" + ring + "/proposers/" + clientID,false) != null){
					logger.info("ABSender check for ring " + ring + ": OK!");
				}else{
					logger.warn("ABSender check for ring " + ring + ": Fail!");
				}			
			} catch (KeeperException | InterruptedException e) {
				logger.error(e);
			}
			logger.info("ABSender host: " + host + ":" + (9080+clientID));
			ABSender proposer = new ABSender(host,9080+clientID);
			proposers.put(ring + "-" + clientID, proposer);
			return proposer;
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getType() == EventType.NodeChildrenChanged && event.getPath().startsWith(path)){
			readPartitions();
		}
	}

	private void readPartitions(){
		partitions.clear();
		try {
			List<String> ls = zoo.getChildren(path, true);
			List<Integer> li = new ArrayList<Integer>();
			for(String s : ls){
				if(s.equals("all")){
					global_ring = Integer.parseInt(new String(zoo.getData(path + "/all", false, null)));
				}else{
					li.add(Integer.valueOf(s));
				}
			}
			Collections.sort(li);
			int min = 0;
			for(Integer i : li){
				int ring = Integer.parseInt(new String(zoo.getData(path + "/" + i, false, null)));
				partitions.add(new Partition(ring,min,i));
				min = i+1;
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}

}
