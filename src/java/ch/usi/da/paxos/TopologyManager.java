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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.paxos.api.ConfigKey;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.ring.NetworkManager;


/**
 * Name: TopologyManager<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012 / Aug 08, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class TopologyManager implements Watcher {
	
	private final static Logger logger = Logger.getLogger(TopologyManager.class);

	protected final InetSocketAddress addr;

	protected final ZooKeeper zoo;
	
	protected final String prefix;
	
	protected final String path;
	
	protected final String id_path = "nodes";
	
	protected final String topo_path = "rings";

	protected final String proposer_path = "proposers";

	protected final String acceptor_path = "acceptors";

	protected final String learner_path = "learners";
	
	protected final String config_path = "config";
	
	protected final Map<String,String> configuration = new ConcurrentHashMap<String,String>();
	
	protected final List<Integer> nodes = new ArrayList<Integer>();

	protected final List<Integer> proposers = new ArrayList<Integer>();

	protected final List<Integer> acceptors = new ArrayList<Integer>();
	
	protected final List<Integer> learners = new ArrayList<Integer>();

	protected final Set<PaxosRole> roles = new HashSet<PaxosRole>();
	
	protected final int nodeID;
	
	protected final int topologyID;

	protected NetworkManager network;
	
	protected InetSocketAddress currentConnection = null;
	
	protected volatile int coordinator = 0;
		
	protected int quorum = 2; // default value
	
	/**
	 * @param topologyID
	 * @param nodeID
	 * @param addr
	 * @param zoo
	 */
	public TopologyManager(int topologyID,int nodeID,InetSocketAddress addr,ZooKeeper zoo) {
		this(topologyID,nodeID,addr,zoo,"/ringpaxos");
	}

	/**
	 * @param topologyID
	 * @param nodeID
	 * @param addr
	 * @param zoo
	 * @param prefix zookeeper prefix
	 */
	public TopologyManager(int topologyID,int nodeID,InetSocketAddress addr,ZooKeeper zoo,String prefix) {
		this.topologyID = topologyID;
		this.nodeID = nodeID;
		this.addr = addr;
		this.zoo = zoo;
		this.prefix = prefix;
		this.path = prefix + "/topology" + topologyID;
	}

	/**
	 * Init the topology manger
	 * 
	 * (we need this init() because of the "this" references) 
	 * 
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void init() throws IOException, KeeperException, InterruptedException {
		zoo.register(this);
		registerNode();
	}
	
	/**
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	protected void registerNode() throws KeeperException, InterruptedException {
		
		// create prefix
		String p = "";
		for(String s : path.split("/")){
			if(s.length() > 0){
				p = p + "/" + s;
				if(zoo.exists(p,false) == null){
					zoo.create(p,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				}
			}
		}

		// register and watch topologyID
		if(zoo.exists(prefix + "/" + topo_path,false) == null){
			zoo.create(prefix + "/" + topo_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		}
		List<String> l = zoo.getChildren(prefix + "/" + topo_path, true);
		for(String s : l){
			zoo.getChildren(prefix + "/" + topo_path + "/" + s, true);
		}
		if(zoo.exists(prefix + "/" + topo_path + "/" + topologyID,false) == null){
			zoo.create(prefix + "/" + topo_path + "/" + topologyID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		}
		try {
			zoo.create(prefix + "/" + topo_path + "/" + topologyID + "/" + nodeID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
		} catch (NodeExistsException e){
			logger.error("Node ID " + nodeID + " in topology " + topologyID + " already registred!");
		}

		// load/set basic configuration
		if(zoo.exists(path + "/" + config_path,false) == null){
			zoo.create(path + "/" + config_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.p1_preexecution_number,"5000".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.p1_resend_time,"1000".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);	
			zoo.create(path + "/" + config_path + "/" + ConfigKey.concurrent_values,"20".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.value_size,"32768".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.value_count,"900000".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.batch_policy,"none".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.value_resend_time,"3000".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.quorum_size,"2".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.stable_storage,"ch.usi.da.paxos.storage.BufferArray".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.tcp_nodelay,"1".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.tcp_crc,"0".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);	
			zoo.create(path + "/" + config_path + "/" + ConfigKey.buffer_size,"2097152".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.learner_recovery,"1".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.trim_modulo,"0".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.trim_quorum,"2".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.auto_trim,"0".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		}
		l = zoo.getChildren(path + "/" + config_path,false);
		for(String k : l){
			String v = new String(zoo.getData(path + "/" + config_path + "/" + k,false,null));
			configuration.put(k,v);
		}
		quorum = Integer.parseInt(configuration.get(ConfigKey.quorum_size));

		// load/set multi ring paxos configuration
		if(zoo.exists(prefix + "/" + config_path,false) == null){
			zoo.create(prefix + "/" + config_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(prefix + "/" + config_path + "/" + ConfigKey.multi_ring_m,"1".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(prefix + "/" + config_path + "/" + ConfigKey.multi_ring_lambda,"0".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(prefix + "/" + config_path + "/" + ConfigKey.multi_ring_delta_t,"100".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(prefix + "/" + config_path + "/" + ConfigKey.deliver_skip_messages,"1".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(prefix + "/" + config_path + "/" + ConfigKey.multi_ring_start_time,"0".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(prefix + "/" + config_path + "/" + ConfigKey.reference_ring,"0".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		}
		l = zoo.getChildren(prefix + "/" + config_path,false);
		for(String k : l){
			String v = new String(zoo.getData(prefix + "/" + config_path + "/" + k,false,null));
			configuration.put(k,v);
		}

		// register and watch node ID
		if(zoo.exists(path + "/" + id_path,false) == null){
			zoo.create(path + "/" + id_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		}
		l = zoo.getChildren(path + "/" + id_path, true); // start watching
		byte[] b = (addr.getHostString() + ";" + addr.getPort()).getBytes(); // store the SocketAddress
		// special case for EC2 inter-region ring; publish public IP
		String public_ip = System.getenv("EC2");
		if(public_ip != null){
			b = (public_ip + ";" + addr.getPort()).getBytes(); // store the SocketAddress
			logger.warn("Publish env(EC2) in zookeeper: " + new String(b) + "!");
		}
		try {
			zoo.create(path + "/" + id_path + "/" + nodeID,b,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
		} catch (NodeExistsException e){
			logger.error("Node ID " + nodeID + " in topology " + topologyID + " already registred!");
		}
		
		// get last_acceptor and current coordinator
		try {
			l = zoo.getChildren(path + "/" + acceptor_path, true);
			int min = nodeID+1;
			int max = 0;
			for(String s : l){
				int i = Integer.valueOf(s);
				if(i < min){
					min = i;
				}
				if(i > max){
					max = i; 
				}
			}
			coordinator = min;
		} catch (NoNodeException e){
		}
	}
	
	private void notifyTopologyChanged(){
	}
	
	private void notifyNewCoordinator() {
	}

	/**
	 * @return the number of acceptors needed for a quorum decision
	 */
	public int getQuorum(){
		return quorum;
	}
	
	/**
	 * @return the nodes
	 */
	public List<Integer> getNodes(){
		return Collections.unmodifiableList(nodes);
	}

	/**
	 * @return the proposers
	 */
	public List<Integer> getProposers(){
		return Collections.unmodifiableList(proposers);
	}

	/**
	 * @return the acceptors
	 */
	public List<Integer> getAcceptors(){
		return Collections.unmodifiableList(acceptors);
	}

	/**
	 * @return the role set
	 */
	public Set<PaxosRole> getRoleSet(){
		return Collections.unmodifiableSet(roles);
	}

	/**
	 * @return the learners
	 */
	public List<Integer> getLearners(){
		return Collections.unmodifiableList(learners);
	}

	/**
	 * @return the node id
	 */
	public int getNodeID(){
		return nodeID;
	}

	/**
	 * @return the topology id
	 */
	public int getTopologyID(){
		return topologyID;
	}

	/**
	 * @return the ID of the current coordinator
	 */
	public int getCoordinatorID(){
		return coordinator;
	}
	
	/**
	 * @return return true if this node is the coordinator
	 */
	public boolean isNodeCoordinator(){
		if(coordinator == nodeID){
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * Use for example: getNodeAddress(getRingSuccessor());
	 * 
	 * @param id the node ID
	 * @return the server address from the specific node or NULL
	 */
	public InetSocketAddress getNodeAddress(int id){
		if(nodeID == id){
			return addr;
		}else{
			try {
				byte[] b = zoo.getData(path + "/" + id_path + "/" + id,false,null);
				String s = new String(b);
				InetAddress ip = InetAddress.getByName(s.split(";")[0]);
				return new InetSocketAddress(ip,Integer.parseInt(s.split(";")[1]));
			} catch (KeeperException e) {
				logger.error(e);
			} catch (InterruptedException e) {
			} catch (UnknownHostException e) {
			}
			return null;
		}
	}

	/**
	 * @return the address of this node
	 */
	public InetSocketAddress getNodeAddress(){
		return addr;
	}

	/**
	 * @return the NetworkManager
	 */
	public NetworkManager getNetwork(){
		return network;
	}
	
	/**
	 * @return the configuration map
	 */
	public Map<String,String> getConfiguration(){
		return Collections.unmodifiableMap(configuration);
	}
	
	@Override
	public void process(WatchedEvent event) {
		try {
			if(event.getType() == EventType.NodeChildrenChanged){
				if(event.getPath().startsWith(path + "/" + id_path)){
					nodes.clear();
					List<String> l = zoo.getChildren(path + "/" + id_path, true);
					for(String s : l){
						nodes.add(Integer.valueOf(s));
					}
					notifyTopologyChanged();
				}else if(event.getPath().startsWith(path + "/" + acceptor_path)){
					acceptors.clear();
					List<String> l = zoo.getChildren(path + "/" + acceptor_path, true);
					int min = nodeID+1;
					int old_coordinator = coordinator;
					for(String s : l){
						int i = Integer.valueOf(s);
						acceptors.add(i);
						if(i < min){
							min = i;
						}
					}
					coordinator = min;
					if(nodeID == min && old_coordinator != coordinator){
						notifyNewCoordinator();
					}
				}else if(event.getPath().startsWith(path + "/" + proposer_path)){
					proposers.clear();
					List<String> l = zoo.getChildren(path + "/" + proposer_path, true);
					for(String s : l){
						proposers.add(Integer.valueOf(s));
					}
				}else if(event.getPath().startsWith(path + "/" + learner_path)){
					learners.clear();
					List<String> l = zoo.getChildren(path + "/" + learner_path, true);
					for(String s : l){
						learners.add(Integer.valueOf(s));
					}
				}
			}
		} catch (KeeperException e) {
			logger.error(e);
		} catch (InterruptedException e) {
		}		

	}

	/**
	 * register the roles of the node which this TopologyManager belongs to
	 * @param role
	 */
	public void registerRole(PaxosRole role) {
		try {
			if (role.equals(PaxosRole.Proposer)){
				if(zoo.exists(path + "/" + proposer_path,false) == null){
					zoo.create(path + "/" + proposer_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				}
				zoo.getChildren(path + "/" + proposer_path, true);
				zoo.create(path + "/" + proposer_path + "/" + nodeID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
				roles.add(role);
			}else if (role.equals(PaxosRole.Acceptor)){ 
				if(zoo.exists(path + "/" + acceptor_path,false) == null){
					zoo.create(path + "/" + acceptor_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				}
				zoo.getChildren(path + "/" + acceptor_path, true);
				zoo.create(path + "/" + acceptor_path + "/" + nodeID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
				roles.add(role);
			}else if (role.equals(PaxosRole.Learner)){
				if(zoo.exists(path + "/" + learner_path,false) == null){
					zoo.create(path + "/" + learner_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				}
				zoo.getChildren(path + "/" + learner_path, true);
				zoo.create(path + "/" + learner_path + "/" + nodeID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
				roles.add(role);
			}
		} catch (KeeperException e) {
			logger.error(e);
		} catch (InterruptedException e) {
		}
	}
	
}
