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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.dmap.thrift.gen.Dmap;
import ch.usi.da.dmap.thrift.gen.MapError;
import ch.usi.da.dmap.thrift.gen.Partition;
import ch.usi.da.dmap.thrift.gen.RangeCommand;
import ch.usi.da.dmap.thrift.gen.RangeResponse;
import ch.usi.da.dmap.thrift.gen.RangeType;
import ch.usi.da.dmap.thrift.gen.Replica;
import ch.usi.da.dmap.thrift.gen.WrongPartition;
import ch.usi.da.dmap.utils.Pair;
import ch.usi.da.dmap.utils.Utils;
import ch.usi.da.paxos.lab.DummyWatcher;


/**
 * Name: RecoveryClient<br>
 * Description: <br>
 * 
 * Creation date: Apr 11, 2017<br>
 * $Id$
 * 
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class RecoveryClient<K,V> {
	
	private final static Logger logger = Logger.getLogger(RecoveryClient.class);
		
	private final Random rand = new Random();
		
	private ZooKeeper zoo;
		
	private final int get_range_size = 5000;

	private long partition_version = 0;
	
	private SortedMap<Integer,Set<Replica>> partitions = new TreeMap<Integer,Set<Replica>>();
	
	private Map<Integer,List<Dmap.Client>> clients = new HashMap<Integer,List<Dmap.Client>>();

	public final String mapID;
	
	public RecoveryClient(String mapID, String zookeeper_host) {
		this.mapID = mapID;
		final String path = "/dmap/" + mapID;
		try {
			zoo = new ZooKeeper(zookeeper_host,3000,new DummyWatcher());
			while(partitions.isEmpty()){
				// lookup one replica to initialize the partitions map
				List<String> replicas = zoo.getChildren(path,false);
				if(replicas.isEmpty()){
					logger.warn(this + " can not locate any replica!");
					Thread.sleep(1000);
				}else{
					int pos = rand.nextInt(replicas.size());
					byte[] a = zoo.getData(path + "/" + replicas.get(pos),false,null);
					String[] as = new String(a).split(";");
					String ip = as[0];
					int port = Integer.parseInt(as[1]);
					try {
						TTransport transport = new TSocket(ip,port);
						TProtocol protocol = new TBinaryProtocol(transport);
						Dmap.Client client = new Dmap.Client(protocol);
						transport.open();
						readPartitions(client);
					} catch (TTransportException e) {
					}
				}
			}
		} catch (IOException | KeeperException | InterruptedException e) {
			logger.error(this + " ZooKeeper init error!",e);
		}

	}
	
	private Dmap.Client getClient(){
		return getClient(null);
	}

	private Dmap.Client getClient(Object key){
		if(key == null){ // random partition
			return getClient(rand.nextInt());
		}else{
			return getClient(key.hashCode());
		}
	}
	
	private Dmap.Client getClient(int hash){
		Dmap.Client client = null;
		int partition = 0;
		SortedMap<Integer,Set<Replica>> tailMap = partitions.tailMap(hash);
		partition = tailMap.isEmpty() ? partitions.firstKey() : tailMap.firstKey();

		if(!clients.containsKey(partition)){
			clients.put(partition,new ArrayList<Dmap.Client>());
		}
		List<Dmap.Client> c = clients.get(partition);
		if(c.isEmpty()){
			Set<Replica> replicas = partitions.get(partition);
			for(Replica r : replicas){
				try {
					client = createClient(r.address);
					c.add(client);									
				} catch (TTransportException e) {
					logger.warn(this + " server connection error to " + r.address);
				}
			}
		}else{
			int pos = rand.nextInt(c.size());			
			client = c.get(pos);
		}
		return client;
	}
	
	private Dmap.Client createClient(String addr) throws TTransportException{
		Dmap.Client client;
		String[] as = new String(addr).split(";");
		String ip = as[0];
		int port = Integer.parseInt(as[1]);
		TSocket socket = new TSocket(ip,port);
		//socket.getSocket().getTcpNoDelay();
		TTransport transport = socket;
		TProtocol protocol = new TBinaryProtocol(transport);
		client = new Dmap.Client(protocol);
		transport.open();
		return client;
	}

	private void removeClient(Dmap.Client client){
		for(Entry<Integer, List<Dmap.Client>> e : clients.entrySet()){
			if(e.getValue().contains(client)){
				e.getValue().remove(client);
				logger.warn(this + " server connection error. Remove this client.");
				if(e.getValue().isEmpty()){
					readPartitions(getClient());
				}
				break;
			}
		}
	}
	
	private long getCmdID(){
		return rand.nextLong();
	}
	
	private void readPartitions(Dmap.Client client){
		try {
			Partition p = client.partition(getCmdID());
			if(p.getVersion() != partition_version){
				partitions.clear();
				partitions.putAll(p.getPartitions());
				partition_version = p.getVersion();
			}
		} catch (TException e) {
			logger.error(this,e);
		}
	}
	
	public long getPartitionVersion(){
		return partition_version;
	}
	
	public SortedMap<Integer,Set<Replica>> getPartitions(){
		return partitions;
	}
	
	public long snapshot() {
		long snapshotID = 0;
		RangeResponse ret = null;
		Dmap.Client client = getClient();
		try {
			RangeCommand cmd = new RangeCommand();
			cmd.setId(getCmdID());
			cmd.setType(RangeType.CREATERANGE);
			cmd.setPartition_version(partition_version);
			ret = client.range(cmd);
			if(ret.isSetSnapshot()){
				snapshotID = ret.getSnapshot();
			}
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (WrongPartition p){
			readPartitions(getClient());
			return snapshot();
		} catch (TTransportException e){
			removeClient(client);
			return snapshot();
		} catch (TException e) {
			logger.error(this,e);
		}
		return snapshotID;
	}

	public void removeSnapshot(Long snapshotID){
		RangeCommand cmd = new RangeCommand();
		Dmap.Client client = getClient();
		cmd.setId(getCmdID());
		cmd.setType(RangeType.DELETERANGE);
		cmd.setSnapshot(snapshotID);
		cmd.setPartition_version(partition_version);
		try {
			client.range(cmd);
			logger.debug(this + " released snapshot " + snapshotID);	
		} catch (MapError e) {
			logger.error(this + " error!",e);
		} catch (WrongPartition p){
			readPartitions(getClient());
			removeSnapshot(snapshotID);
		} catch (TTransportException e){
			removeClient(client);
			removeSnapshot(snapshotID);
		} catch (TException e) {
			logger.error(this + " error!",e);
		}				
	}

	public long partitionSize(int token,long snapshotID){
		Dmap.Client client = getClient(token);
		RangeCommand s = new RangeCommand();
		s.setId(getCmdID());
		s.setType(RangeType.PARTITIONSIZE);
		s.setPartition_version(partition_version);
		s.setSnapshot(snapshotID);
		try{
			RangeResponse r = client.range(s);
			return r.getCount();
		} catch (MapError e) {
			logger.error(this + " error!",e);
			return partitionSize(token,snapshotID); // must exist eventually
		} catch (WrongPartition p){
			readPartitions(getClient());
			return partitionSize(token,snapshotID);
		} catch (TTransportException e){
			removeClient(client);
			return partitionSize(token,snapshotID);
		} catch (TException e) {
			logger.error(this + " error!",e);
		}
		return 0;
	}
	
	public Iterator<Map.Entry<K,V>> iterator(int token,long snapshotID) {
		long size = partitionSize(token,snapshotID);
		LinkedBlockingQueue<Entry<K, V>> queue = new LinkedBlockingQueue<Map.Entry<K,V>>();
		Thread t  = new Thread(new QueueFiller(token,snapshotID,queue,size));
		t.start();
        return new EntryIterator<Map.Entry<K,V>>(size,queue);
    }


    class EntryIterator<T> implements Iterator<T> {
        
    	private final long size;
    	
    	private long delivered = 0;
    	
        private final BlockingQueue<T> queue;
        
        T last = null;
        
        EntryIterator(Long size, BlockingQueue<T> queue) {
        	this.size = size;
        	this.queue = queue;
        }

        public final boolean hasNext() {
        	return delivered < size ? true : false;
        }

        public void remove() {
       		throw new IllegalStateException();
        }
        
		@Override
		public T next() {
			if(delivered < size){
				delivered++;
				try {
					return queue.take();
				} catch (InterruptedException e) {
					return null;
				}
			}else{
				throw new NoSuchElementException();
			}
		}
    }
    
    class QueueFiller implements Runnable {

    	private final BlockingQueue<Map.Entry<K,V>> queue;
    	
    	private final long snapshotID;
    	
    	private final int token;
    	
    	private final long size;
    	
    	public QueueFiller(int token,long snapshotID,BlockingQueue<Map.Entry<K,V>> queue,long size){
    		this.token = token;
    		this.snapshotID = snapshotID;
    		this.queue = queue;
    		this.size = size;
    	}
    	
    	@Override
    	public void run() {
    		Set<Replica> replicas = partitions.get(token);
    		Dmap.Client client = null;
    		while(client == null){
	    		try {
	    			Replica replica = (Replica)replicas.toArray()[rand.nextInt(replicas.size())];
					client = createClient(replica.address);
				} catch (TTransportException e1) {
				}
    		}
    		long retreived = 0;
    		int from = 0;
    		do{
				try {
	    			RangeCommand cmd = new RangeCommand();
	    			cmd.setId(getCmdID());
	    			cmd.setType(RangeType.GETRANGE);
	    			cmd.setSnapshot(snapshotID);
	    			cmd.setFromid(from);
	    			cmd.setToid(from+get_range_size);
	    			cmd.setPartition_version(partition_version);
	    			from = from+get_range_size;
	    			if(client == null){
	    				throw new TTransportException();
	    			}
	    			RangeResponse ret = client.range(cmd); //Idea: ask multiple replicas with different offset
	    			if(ret != null){
	    				if(ret.isSetValues()){
		    				@SuppressWarnings("unchecked")
							List<Pair<K,V>> sublist = (List<Pair<K,V>>) Utils.getObject(ret.getValues());
		    				for(Pair<K,V> e : sublist){
		    					queue.put(e);
		    					retreived++;
		    				}	    					
	    				}
	    			}
				} catch (MapError e){
					/*if(!view.closed){
						logger.error(view + " error!",e);
					}*/
				} catch (WrongPartition p){
				} catch (TTransportException e){
					removeClient(client);
					try {
						Replica replica = (Replica)replicas.toArray()[rand.nextInt(replicas.size())];
						client = createClient(replica.address);
					} catch (TTransportException e1) {
					}
				} catch (TException | ClassNotFoundException | IOException e) {
					logger.error("QueueFiller error!",e);
				} catch (InterruptedException e){
					Thread.currentThread().interrupt();
					break;
				}
    		}while(retreived < size);
    		client.getInputProtocol().getTransport().close();
    		client.getOutputProtocol().getTransport().close();
    	}
    }

}
