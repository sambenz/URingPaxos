package ch.usi.da.dmap;
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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import ch.usi.da.dmap.thrift.gen.Command;
import ch.usi.da.dmap.thrift.gen.CommandType;
import ch.usi.da.dmap.thrift.gen.Dmap;
import ch.usi.da.dmap.thrift.gen.MapError;
import ch.usi.da.dmap.thrift.gen.Response;
import ch.usi.da.paxos.lab.DummyWatcher;



/**
 * Name: DistributedOrderedMap<br>
 * Description: <br>
 * 
 * Creation date: Jan 28, 2017<br>
 * $Id$
 * 
 * Notes:
 * - Not using AbstractMap because it implements some methods inefficient for distributed operations.
 *
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class DistributedOrderedMap<K,V> implements SortedMap<K,V>, Cloneable, java.io.Serializable {
	
	private final static long serialVersionUID = -8575201903369745596L;

	private final static Logger logger = Logger.getLogger(DistributedOrderedMap.class);
	
	private final Comparator<? super K> comparator;
		
	private ZooKeeper zoo;
		
	private final AtomicLong cmdCount = new AtomicLong(0);
	
	//TODO: private SortedMap<Integer, Integer> partitions = new TreeMap<Integer, Integer>();

	public final String mapID;
	
	//map of connected thrift clients...??
	Dmap.Client client
;	
	public DistributedOrderedMap(String mapID, String zookeeper_host) {
		this(mapID,zookeeper_host,null);
	}
	
	public DistributedOrderedMap(String mapID, String zookeeper_host, Comparator<? super K> comparator) {
		this.comparator = comparator; //TODO: comparator should be at replica?
		this.mapID = mapID;
		final String path = "/dmap/" + mapID;
		try {
			//TODO: later ....
			zoo = new ZooKeeper(zookeeper_host,3000,new DummyWatcher());
			// lookup one replica to initialize the partitions map
			List<String> replicas = zoo.getChildren(path,false);
			if(replicas.isEmpty()){
				// logger.error(this + " can not locate any replica!");
			}else{
				for(String r : replicas){
					System.out.println(r);
					// readPartitions();
					// if download partitions ok; break;				
					// logger.info(this + " initialized.");
					// else ERROR
				}				
			}

			TTransport transport = new TFramedTransport(new TSocket("127.0.0.1",5800));
			TProtocol protocol = new TBinaryProtocol(transport);
		    client = new Dmap.Client(protocol);
		    transport.open();

		} catch (IOException | KeeperException | InterruptedException e) {
			logger.error(this + " ZooKeeper init error!",e);
		} catch (TTransportException e) {
			logger.error(this + " Thrift init error!",e);
		}

	}
	
	// single partition commands

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		Response ret = null;
		try {
			Command cmd = new Command();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(CommandType.GET);
			cmd.setKey(Utils.getBuffer(key));
			ret = client.execute(cmd);
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException | IOException e) {
			logger.error(this,e);
		}
		if(ret != null && ret.isSetValue()){
			try {
				return (V) Utils.getObject(ret.getValue());
			} catch (ClassNotFoundException | IOException e) {
				logger.error(this,e);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V put(K key, V value) {
		Response ret = null;
		try {
			Command cmd = new Command();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(CommandType.PUT);
			cmd.setKey(Utils.getBuffer(key));
			cmd.setValue(Utils.getBuffer(value));
			ret = client.execute(cmd);
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException | IOException e) {
			logger.error(this,e);
		}
		if(ret != null && ret.isSetValue()){
			try {
				return (V) Utils.getObject(ret.getValue());
			} catch (ClassNotFoundException | IOException e) {
				logger.error(this,e);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		Response ret = null;
		try {
			Command cmd = new Command();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(CommandType.REMOVE);
			cmd.setKey(Utils.getBuffer(key));
			ret = client.execute(cmd);
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException | IOException e) {
			logger.error(this,e);
		}
		if(ret != null && ret.isSetValue()){
			try {
				return (V) Utils.getObject(ret.getValue());
			} catch (ClassNotFoundException | IOException e) {
				logger.error(this,e);
			}
		}
		return null;
	}

	@Override
	public boolean containsKey(Object key) {
		V v = get(key);
		if(v != null){
			return true;
		}else{
			return false;
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for(Map.Entry<? extends K, ? extends V> e : m.entrySet()){
			put(e.getKey(),e.getValue());
		}
	}

	
	// multi-partition commands
	
	
	@Override
	public int size() {
		Response ret = null;
		try {
			Command cmd = new Command();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(CommandType.SIZE);
			ret = client.execute(cmd);
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException e) {
			logger.error(this,e);
		}
		if(ret != null && ret.isSetCount()){
				return (int) ret.getCount();
		}
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsValue(Object value) {
		Response ret = null;
		try {
			Command cmd = new Command();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(CommandType.CONTAINSVALUE);
			cmd.setValue(Utils.getBuffer(value));
			ret = client.execute(cmd);
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException | IOException e) {
			logger.error(this,e);
		}
		if(ret != null && ret.getCount() > 0){
			return true;
		}
		return false;
	}

	@Override
	public void clear() {
		try {
			Command cmd = new Command();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(CommandType.CLEAR);
			client.execute(cmd);
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException e) {
			logger.error(this,e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public K firstKey() {
		Response ret = null;
		try {
			Command cmd = new Command();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(CommandType.FIRSTKEY);
			ret = client.execute(cmd);
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException e) {
			logger.error(this,e);
		}
		if(ret != null && ret.isSetKey()){
			try {
				return (K) Utils.getObject(ret.getKey());
			} catch (ClassNotFoundException | IOException e) {
				logger.error(this,e);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public K lastKey() {
		Response ret = null;
		try {
			Command cmd = new Command();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(CommandType.LASTKEY);
			ret = client.execute(cmd);
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException e) {
			logger.error(this,e);
		}
		if(ret != null && ret.isSetKey()){
			try {
				return (K) Utils.getObject(ret.getKey());
			} catch (ClassNotFoundException | IOException e) {
				logger.error(this,e);
			}
		}
		return null;
	}

	
	// global snapshot/iterator commands

	
	@Override
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedMap<K, V> headMap(K toKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedMap<K, V> tailMap(K fromKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	
	// others
	
	
	@Override
	public Comparator<? super K> comparator() {
		return comparator;
	}
	
	@Override
	public String toString(){
		return "Distributed Ordered Map: " + mapID;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DistributedOrderedMap<?,?>){
            if(this.mapID.equals(((DistributedOrderedMap<?,?>) obj).mapID)){
                    return true;
            }
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return mapID.hashCode();
	}

}
