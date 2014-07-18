package ch.usi.da.paxos.ring;
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

import ch.usi.da.paxos.api.PaxosRole;


/**
 * Name: RingManager<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class RingManager implements Watcher {
	
	private final static Logger logger = Logger.getLogger(RingManager.class);

	private final InetSocketAddress addr;

	private final ZooKeeper zoo;
	
	private final String prefix;
	
	private final String path;
	
	private final String id_path = "nodes";
	
	private final String rid_path = "rings";

	private final String proposer_path = "proposers";

	private final String acceptor_path = "acceptors";

	private final String learner_path = "learners";
	
	private final String config_path = "config";
	
	private final Map<String,String> configuration = new ConcurrentHashMap<String,String>();
	
	private final List<Integer> ring = new ArrayList<Integer>();

	private final List<Integer> allrings = new ArrayList<Integer>();
	
	private final Set<PaxosRole> roles = new HashSet<PaxosRole>();
	
	private final int nodeID;
	
	private final int ringID;

	private NetworkManager network;
	
	private InetSocketAddress currentConnection = null;

	private volatile int last_acceptor = 0;
	
	private volatile int coordinator = 0;
		
	private int quorum = 2; // default value
	
	/**
	 * @param ringID
	 * @param nodeID
	 * @param addr
	 * @param zoo
	 */
	public RingManager(int ringID,int nodeID,InetSocketAddress addr,ZooKeeper zoo) {
		this(ringID,nodeID,addr,zoo,"/ringpaxos");
	}

	/**
	 * @param ringID
	 * @param nodeID
	 * @param addr
	 * @param zoo
	 * @param prefix zookeeper prefix
	 */
	public RingManager(int ringID,int nodeID,InetSocketAddress addr,ZooKeeper zoo,String prefix) {
		this.ringID = ringID;
		this.nodeID = nodeID;
		this.addr = addr;
		this.zoo = zoo;
		this.prefix = prefix;
		this.path = prefix + "/ring" + ringID;
	}

	/**
	 * Init the ring manger
	 * 
	 * (we need this init() because of the "this" references) 
	 * 
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void init() throws IOException, KeeperException, InterruptedException {
		network = new NetworkManager(this);
		zoo.register(this);
		registerNode();
		network.startServer();
	}
	
	/**
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	private void registerNode() throws KeeperException, InterruptedException {
		
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

		// register and watch ring ID
		if(zoo.exists(prefix + "/" + rid_path,false) == null){
			zoo.create(prefix + "/" + rid_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		}
		List<String> l = zoo.getChildren(prefix + "/" + rid_path, true);
		for(String s : l){
			zoo.getChildren(prefix + "/" + rid_path + "/" + s, true);
		}
		if(zoo.exists(prefix + "/" + rid_path + "/" + ringID,false) == null){
			zoo.create(prefix + "/" + rid_path + "/" + ringID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		}
		try {
			zoo.create(prefix + "/" + rid_path + "/" + ringID + "/" + nodeID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
		} catch (NodeExistsException e){
			logger.error("Node ID " + nodeID + " in ring " + ringID + " already registred!");
		}

		// load/set ringpaxos configuration
		if(zoo.exists(path + "/" + config_path,false) == null){
			zoo.create(path + "/" + config_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.p1_preexecution_number,"5000".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.p1_resend_time,"1000".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);	
			zoo.create(path + "/" + config_path + "/" + ConfigKey.concurrent_values,"20".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.value_size,"32768".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.value_count,"900000".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zoo.create(path + "/" + config_path + "/" + ConfigKey.batch_size,"0".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);			
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
			logger.error("Node ID " + nodeID + " in ring " + ringID + " already registred!");
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
			last_acceptor = max;
			coordinator = min;
		} catch (NoNodeException e){
		}
	}

	private void notifyRingChanged(){
		InetSocketAddress saddr = getNodeAddress(getRingSuccessor(nodeID));
		logger.info("RingManager ring " + ringID + " changed: " + ring + " (succsessor: " + getRingSuccessor(nodeID) + " at " + saddr + ")");
		if(saddr != null && currentConnection == null || !currentConnection.equals(saddr)){
			network.disconnectClient();
			network.connectClient(saddr);
			currentConnection = saddr;
		}
	}
	
	private void notifyNewCoordinator(){
		logger.info("RingManger this node is the new coordinator for ring " + ringID + "!");
		Thread c = new Thread(new CoordinatorRole(this));
		c.setName("Coordinator");
		c.start();
	}
	
	/**
	 * @return the number of acceptors needed for a quorum decision
	 */
	public int getQuorum(){
		return quorum; //(int) Math.ceil((double)(acceptor_count+1)/2);
	}
	
	/**
	 * @return the ring
	 */
	public List<Integer> getRing(){
		return Collections.unmodifiableList(ring);
	}

	/**
	 * @return all ring ID's
	 */
	public List<Integer> getAllRings(){
		return Collections.unmodifiableList(allrings);
	}

	/**
	 * @return the role set
	 */
	public Set<PaxosRole> getRoleSet(){
		return Collections.unmodifiableSet(roles);
	}

	/**
	 * @return the node id
	 */
	public int getNodeID(){
		return nodeID;
	}

	/**
	 * @return the ring id
	 */
	public int getRingID(){
		return ringID;
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
	 * @param id 
	 * @return the node id of the ring successor
	 */
	public int getRingSuccessor(int id){
		int pos = ring.indexOf(new Integer(id));
		if(pos+1 >= ring.size()){
			return ring.get(0);
		}else{
			return ring.get(pos+1);
		}
	}
	
	/**
	 * @param id 
	 * @return the node id of the ring predecessor
	 */
	public int getRingPredecessor(int id){
		int pos = ring.indexOf(new Integer(id));
		if(pos-1 < 0){
			return ring.get(ring.size()-1);
		}else{
			return ring.get(pos-1);
		}
	}
	
	/**
	 * @return the id of the last acceptor in the ring
	 */
	public int getLastAcceptor(){
		return last_acceptor;
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
		// ls nodes/ forms the ring
		try {
			if(event.getType() == EventType.NodeChildrenChanged){
				if(event.getPath().startsWith(path + "/" + id_path)){
					ring.clear();
					List<String> l = zoo.getChildren(path + "/" + id_path, true);
					for(String s : l){
						ring.add(Integer.valueOf(s));
					}
					Collections.sort(ring);
					notifyRingChanged();
				}else if(event.getPath().startsWith(path + "/" + acceptor_path)){
					List<String> l = zoo.getChildren(path + "/" + acceptor_path, true);
					int min = nodeID+1;
					int max = 0;
					int old_coordinator = coordinator;
					for(String s : l){
						int i = Integer.valueOf(s);
						if(i < min){
							min = i;
						}
						if(i > max){
							max = i; 
						}
					}
					last_acceptor = max;
					coordinator = min;
					if(nodeID == min && old_coordinator != coordinator){
						notifyNewCoordinator();
					}
				}else if(event.getPath().startsWith(prefix + "/" + rid_path)){
					allrings.clear();
					List<String> l = zoo.getChildren(prefix + "/" + rid_path, true);
					for(String s : l){
						List<String> l2 = zoo.getChildren(prefix + "/" + rid_path + "/" + s, true);
						if(l2.size() > 0) {
							try {
								allrings.add(Integer.valueOf(s));
							} catch (Exception e){ // only add numbers
							}
						}
					}
					Collections.sort(allrings);
					//logger.debug("nr of rings in system changed to: " + allrings + " (" + ringID + ")");
				}
			}
		} catch (KeeperException e) {
			logger.error(e);
		} catch (InterruptedException e) {
		}		
	}

	/**
	 * register the roles of the node which this RingManager belongs to
	 * @param role
	 */
	public void registerRole(PaxosRole role) {
		try {
			if (role.equals(PaxosRole.Proposer)){
				if(zoo.exists(path + "/" + proposer_path,false) == null){
					zoo.create(path + "/" + proposer_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				}
				zoo.create(path + "/" + proposer_path + "/" + nodeID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
				roles.add(role);
			}else if (role.equals(PaxosRole.Acceptor)){ 
				if(zoo.exists(path + "/" + acceptor_path,false) == null){
					zoo.create(path + "/" + acceptor_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				}
				zoo.getChildren(path + "/" + acceptor_path, true); // for leader election
				zoo.create(path + "/" + acceptor_path + "/" + nodeID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
				roles.add(role);
			}else if (role.equals(PaxosRole.Learner)){
				if(zoo.exists(path + "/" + learner_path,false) == null){
					zoo.create(path + "/" + learner_path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
				}
				zoo.create(path + "/" + learner_path + "/" + nodeID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
				roles.add(role);
			}
		} catch (KeeperException e) {
			logger.error(e);
		} catch (InterruptedException e) {
		}
	}
	
}
