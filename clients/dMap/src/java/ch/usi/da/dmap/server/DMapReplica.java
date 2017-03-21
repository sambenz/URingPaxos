package ch.usi.da.dmap.server;
/* 
 * Copyright (c) 2017 Università della Svizzera italiana (USI)
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.dmap.thrift.gen.Command;
import ch.usi.da.dmap.thrift.gen.Dmap;
import ch.usi.da.dmap.thrift.gen.Dmap.Iface;
import ch.usi.da.dmap.thrift.gen.MapError;
import ch.usi.da.dmap.thrift.gen.RangeCommand;
import ch.usi.da.dmap.thrift.gen.RangeResponse;
import ch.usi.da.dmap.thrift.gen.Response;
import ch.usi.da.dmap.thrift.gen.WrongPartition;
import ch.usi.da.dmap.utils.Pair;
import ch.usi.da.dmap.utils.Utils;
import ch.usi.da.paxos.Util;
import ch.usi.da.paxos.lab.DummyWatcher;
import ch.usi.da.paxos.message.Control;
import ch.usi.da.paxos.message.ControlType;
import ch.usi.da.paxos.ring.ElasticLearnerRole;
import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingDescription;


/**
 * Name: DMapReplica<br>
 * Description: <br>
 * 
 * Creation date: Jan 28, 2017<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class DMapReplica<K,V> {

	private final static Logger logger = Logger.getLogger(DMapReplica.class);
	
	private volatile SortedMap<K,V> db;
	
	private final Node node;
	
	public int default_ring;
	
	public int partition_ring;
		
	public long partition_version = 0;
	public final Map<Integer,Set<String>> partitions = new HashMap<Integer,Set<String>>();
	public final Map<Integer,Integer> rings = new HashMap<Integer,Integer>();
	
	private final Map<Long, List<Entry<K, V>>> snapshots = new LinkedHashMap<Long,List<Entry<K,V>>>(){
		private static final long serialVersionUID = -2704400124020327063L;
		protected boolean removeEldestEntry(Map.Entry<Long, List<Entry<K, V>>> eldest) {  
			return size() > 10; // hold only 10 snapshots in memory!                                 
		}};

	private final Map<Long,SortedMap<K,V>> snapshotsDB = new LinkedHashMap<Long,SortedMap<K,V>>(){
		private static final long serialVersionUID = -2704400124020327063L;
		protected boolean removeEldestEntry(Map.Entry<Long,SortedMap<K,V>> eldest) {  
			return size() > 10; // hold only 10 snapshots in memory!                                 
		}};

	private Map<Long,FutureResponse> responses = new ConcurrentHashMap<Long,FutureResponse>();
	
	public DMapReplica(int default_ring,Node node,Comparator<? super K> comparator) {
		this.default_ring = default_ring;
		this.node = node;
		db = new TreeMap<K,V>(comparator);
	}
	
	public DMapReplica(int default_ring,Node node) {
		this.default_ring = default_ring;
		this.node = node;
		db = new TreeMap<K,V>();
	}
	
	public Node getNode(){
		return node;
	}
	
	public Map<Long,FutureResponse> getResponses() {
		return responses;
	}
	
	public void registerPartition(int ring_id,String thrift_address,int token){
		// register (propose) this partition
		partition_ring = ring_id;
		String cmd = (ring_id + "," + thrift_address + "," + token);
		try {
			node.getProposer(default_ring).propose(Utils.getBuffer(cmd).array());
		} catch (IOException e) {
			logger.error(e);
		}
		// subscribe learner to partition
		if(node.getLearner() instanceof ElasticLearnerRole){
			Control c = new Control(node.getNodeID(),ControlType.Subscribe,node.getGroupID(),ring_id);
			node.getProposer(default_ring).control(c);
			node.getProposer(ring_id).control(c);
		}
	}
	
	public void receive(long instance, Object o) {
		if(o instanceof Command){
			Command cmd = (Command)o;
			logger.debug("DMapReplica execute " + cmd);
			Object r = null;
			try {
				r = execute(cmd);
			} catch (TException e) {
				r = e;
			}
			if(responses.containsKey(cmd.id)){
				responses.get(cmd.id).setResponse(r);
				responses.remove(cmd.id);
			}
		} else if(o instanceof RangeCommand){
			RangeCommand cmd = (RangeCommand)o;
			logger.debug("DMapReplica execute " + cmd);
			Object r = null;
			try {
				r = range(instance,cmd);
			} catch (TException e) {
				r = e;
			}
			if(responses.containsKey(cmd.id)){
				responses.get(cmd.id).setResponse(r);
				responses.remove(cmd.id);
			}
		/*} else if(o instanceof Long){ // get partition
			Partition p = new Partition();
			p.setVersion(partition_version);
			p.setPartitions(partitions);
			responses.get((Long)o).setResponse(p);
			responses.remove((Long)o);*/
		} else if(o instanceof String){ // set partition
			String[] cmd = ((String)o).split(",");
			int ring = Integer.parseInt(cmd[0]);
			String address = cmd[1];
			int token = Integer.parseInt(cmd[2]);
			if(partitions.containsKey(token)){
				partitions.get(token).add(address);
			}else{
				Set<String> s = new HashSet<String>();
				s.add(address);
				partitions.put(token,s);
			}
			partition_version = instance;			
			rings.put(token,ring);
			logger.info("Install new partition map " + partition_version + ":" + partitions + "(" + rings + ")");
		}		
	}

	@SuppressWarnings("unchecked")
	public Response execute(Command cmd) throws MapError, TException {
		Response response = new Response();
		response.setId(cmd.id);
		response.setCount(0);
		if(cmd.getPartition_version() != partition_version){
			WrongPartition p = new WrongPartition();
			p.setErrorMsg(cmd.getPartition_version() + "!=" + partition_version);
			throw p;
		}
		try {
			K key = null;
			if(cmd.isSetKey()){
				key = (K) Utils.getObject(cmd.getKey());
			}
			V value = null;
			if(cmd.isSetValue()){
				value = (V) Utils.getObject(cmd.getValue());
			}
			K retK = null;
			V retV = null; 

			SortedMap<K,V> snapshotDB = db;
			if(cmd.isSetSnapshot()){
				long snapshot = cmd.getSnapshot();
				if(snapshotsDB.containsKey(snapshot)){
					snapshotDB = snapshotsDB.get(snapshot);
				}else{
					MapError e = new MapError();
					e.setErrorMsg("Snaphost " + cmd.getSnapshot() + " does not exist!");
					throw e;
				}
			}
			switch(cmd.type){
			case CLEAR:
				snapshotDB.clear();
				break;
			case CONTAINSVALUE:
				if(snapshotDB.containsValue(value)){
					response.setCount(1);
				}
				break;
			case GET:
				retV = snapshotDB.get(key);
				break;
			case PUT:
				retV = snapshotDB.put(key,value);
				break;
			case REMOVE:
				retV = snapshotDB.remove(key);
				break;
			case SIZE:
				response.setCount(snapshotDB.size());
				break;
			case FIRSTKEY:
				retK = snapshotDB.firstKey();
				break;
			case LASTKEY:
				retK = snapshotDB.lastKey();
				break;	
			default:
				break;
			}
			if(retK != null){
				response.setKey(Utils.getBuffer(retK));
				response.setCount(1);
			}
			if(retV != null){
				response.setValue(Utils.getBuffer(retV));
				response.setCount(1);
			}
		} catch (ClassNotFoundException | IOException e) {
			logger.error("DMapReplica error: ",e);
			MapError error = new MapError();
			error.setErrorMsg(e.getMessage());
			throw error;
		}
		return response;
	}

	@SuppressWarnings("unchecked")
	public RangeResponse range(long instance, RangeCommand cmd) throws MapError, TException {
		RangeResponse response = new RangeResponse();
		response.setId(cmd.getId());
		List<Entry<K,V>> snapshot;
		SortedMap<K,V> snapshotDB;
		/*if(cmd.getPartition_version() != partitions_version){ // that's ok on snapshots
			WrongPartition p = new WrongPartition();
			p.setErrorMsg(cmd.getPartition_version() + "!=" + partitions_version);
			throw p;
		}*/		
		try {
			switch(cmd.type){
			case PERSISTRANGE:
				if(cmd.isSetSnapshot() && snapshots.containsKey(cmd.getSnapshot())){
					//TODO: persist
				}
				break;
			case CREATERANGE:
				if(cmd.isSetFromkey() && cmd.isSetTokey()){
					K from = (K) Utils.getObject(cmd.getFromkey());
					K to = (K) Utils.getObject(cmd.getTokey());
					snapshotDB = new TreeMap<K,V>(db.subMap(from,to));
				}else if(cmd.isSetFromkey() && !cmd.isSetTokey()){
					K from = (K) Utils.getObject(cmd.getFromkey());
					snapshotDB = new TreeMap<K,V>(db.tailMap(from));
				}else if(!cmd.isSetFromkey() && cmd.isSetTokey()){
					K to = (K) Utils.getObject(cmd.getTokey());
					snapshotDB = new TreeMap<K,V>(db.headMap(to));
				}else{
					snapshotDB = new TreeMap<K,V>(db);
				}
				long id = instance;
				snapshots.put(id,new ArrayList<Entry<K,V>>(snapshotDB.entrySet()));
				snapshotsDB.put(id,snapshotDB);
				response.setCount(snapshotDB.size());
				response.setSnapshot(id);
				break;
			case DELETERANGE:
				if(cmd.isSetSnapshot()){
					if(snapshots.containsKey(cmd.getSnapshot())){
						snapshots.remove(cmd.getSnapshot());
						snapshotsDB.remove(cmd.getSnapshot());
						response.setCount(1);
					}else{
						MapError e = new MapError();
						e.setErrorMsg("Snaphost " + cmd.getSnapshot() + " does not exist!");
						throw e;
					}
				}
				break;
			case GETRANGE:
				id = cmd.getSnapshot();
				if(snapshots.containsKey(id)){
					snapshot = snapshots.get(id);  
					int from = 0;
					int size = snapshot.size();
					int to = size;
					if(cmd.isSetFromid() && cmd.getFromid() >= 0 && cmd.getFromid() <= size){
						from = cmd.getFromid();
						if(cmd.isSetToid() && cmd.getToid() > cmd.getFromid() && cmd.getToid() <= size){
							to = cmd.getToid();
						}
					}
					List<Pair<K,V>> list = new ArrayList<Pair<K,V>>(); //sublist and TreeMap.Entry are not serializable!
					for(Entry<K,V> e : snapshot.subList(from,to)){
						list.add(new Pair<K,V>(e.getKey(),e.getValue()));
					}
					response.setCount(list.size());
					response.setValues(Utils.getBuffer(list));
				}else{
					MapError e = new MapError();
					e.setErrorMsg("Snaphost " + cmd.getSnapshot() + " does not exist!");
					throw e;			
				}
				break;
			default:
				break;
			}
		} catch (ClassNotFoundException | IOException e) {
			logger.error("DMapReplica error: ",e);
			MapError error = new MapError();
			error.setErrorMsg(e.getMessage());
			throw error;
		}
		return response;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String mapID = "";
			int nodeID = 1;
			int groupID = 1;
			int default_ring = 1;
			int partition_ring = 2;
			String roles = "1:PAL;2:PA";
			int token = 0;
			if(args.length > 6){
				mapID = args[0];
				nodeID = Integer.parseInt(args[1]);
				groupID = Integer.parseInt(args[2]);
				default_ring = Integer.parseInt(args[3]);
				partition_ring = Integer.parseInt(args[4]);
				roles = args[5];
				token = Integer.parseInt(args[6]);
			}else{
				System.err.println("Plese use \"DMapReplica\" \"map ID\" \"node ID\" \"group ID\" \"default ring\" \"partition ring\" \"roles\" \"token\" \"[zookeeper]\"");
				System.exit(1);
			}
			String zoo_host = "127.0.0.1:2181";
			if (args.length > 7) {
				zoo_host = args[7];
			}
						
			//register this node at zookeeper
			final Random rand = new Random();
			final int port = 5000 + rand.nextInt(1000); // assign port between 5000-6000
			final InetAddress ip = Util.getHostAddress();			
			final InetSocketAddress addr = new InetSocketAddress(ip,port);
			final String addrs = addr.getHostString() + ";" + addr.getPort();
			final byte[] b = addrs.getBytes(); // store the SocketAddress
			final ZooKeeper zoo = new ZooKeeper(zoo_host,3000,new DummyWatcher());
			Util.checkThenCreateZooNode("/dmap/" + mapID,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,zoo);
			Util.checkThenCreateZooNode("/dmap/" + mapID + "/node",b,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL,zoo);
			
			//start URingPaxos node
			List<RingDescription> rings = Util.parseRingsArgument(roles);
			final Node node = new Node(nodeID,groupID,zoo_host,rings);
			node.start();

			//create replica
			DMapReplica<Object,Object> replica = new DMapReplica<Object,Object>(default_ring,node);
			Thread.sleep(5000);
			replica.registerPartition(partition_ring,addrs,token);
			
			//start thrift server (proposer)
			final Dmap.Processor<Iface> processor = new Dmap.Processor<Iface>(new ABSender(replica));
			final TServerTransport serverTransport = new TServerSocket(port);
			final TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport).processor(processor);
			serverArgs.maxWorkerThreads(5000);
			serverArgs.minWorkerThreads(5);
			final TServer server = new TThreadPoolServer(serverArgs);
			Thread s = new Thread() {
				@Override
				public void run() {
					server.serve();
				};
			};
			s.start();
			
			//start receiver (learner)
			Thread receiver = new Thread(new ABReceiver(replica));
			receiver.setName("ABReceiver");
			receiver.start();
			
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			in.readLine();
			node.stop();
			zoo.close();
			server.stop();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
