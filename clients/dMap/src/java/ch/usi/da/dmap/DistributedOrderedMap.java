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
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
import ch.usi.da.dmap.thrift.gen.RangeCommand;
import ch.usi.da.dmap.thrift.gen.RangeResponse;
import ch.usi.da.dmap.thrift.gen.RangeType;
import ch.usi.da.dmap.thrift.gen.Response;
import ch.usi.da.dmap.utils.Pair;
import ch.usi.da.dmap.utils.Utils;
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

	private final int get_range_size = 1;

	//private SortedMap<Integer, Integer> partitions = new TreeMap<Integer, Integer>();

	public final String mapID;
	
	Dmap.Client client;

	public DistributedOrderedMap(String mapID, String zookeeper_host) {
		this(mapID,zookeeper_host,null);
	}
	
	public DistributedOrderedMap(String mapID, String zookeeper_host, Comparator<? super K> comparator) {
		this.comparator = comparator; // important at replica
		this.mapID = mapID;
		final String path = "/dmap/" + mapID;
		try {
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
		if(key == null){ throw new NullPointerException(); }
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
		if(key == null){ throw new NullPointerException(); }
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
		if(key == null){ throw new NullPointerException(); }
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
		if(key == null){ throw new NullPointerException(); }
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
	

	public long sizeLong() {
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
				return ret.getCount();
		}
		return 0;
	}

	@Override
	public int size() {
		return (int) sizeLong();
	}
	
	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsValue(Object value) {
		if(value == null){ throw new NullPointerException(); }
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
		throw new NoSuchElementException();
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
		throw new NoSuchElementException();
	}

	
	// global snapshot/iterator commands


	@Override
	public SortedMap<K,V> subMap(K fromKey, K toKey) {
		RangeResponse ret = null;
		SortedMap<K,V> submap = null;		
		try {
			RangeCommand cmd = new RangeCommand();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(RangeType.CREATERANGE);
			if(fromKey != null){
				cmd.setFromkey(Utils.getBuffer(fromKey));
			}
			if(toKey != null){
				cmd.setTokey(Utils.getBuffer(toKey));
			}
			ret = client.range(cmd);
			long snapshotID = 0;
			if(ret.isSetSnapshot()){
				snapshotID = ret.getSnapshot();
				submap = new SnapshotView(snapshotID, ret.getCount());
				logger.debug(this + " created iterator " + snapshotID);
			}
		} catch (MapError e){
			logger.error(this + " " + e.errorMsg);
		} catch (TException | IOException e) {
			logger.error(this,e);
		}
		return submap;
	}

	@Override
	public SortedMap<K,V> headMap(K toKey) {
		return subMap(null,toKey);
	}

	@Override
	public SortedMap<K,V> tailMap(K fromKey) {
		return subMap(fromKey,null);
	}

	@Override
	public Set<K> keySet() {
		return subMap(null,null).keySet();
	}

	@Override
	public Collection<V> values() {
		return subMap(null,null).values();
	}

	@Override
	public Set<java.util.Map.Entry<K,V>> entrySet() {
		return subMap(null,null).entrySet();
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

	
	// view and iterators on a snapshot
	
	
	class SnapshotView implements SortedMap<K,V> {

		public final long snapshotID;
		
		public final long size;
		
		public SnapshotView(long snapshotID, long size){
			this.snapshotID = snapshotID;
			this.size = size;
		}
		
		@Override
		public int size() {
			return (int) size;
		}

		@Override
		public boolean isEmpty() {
			return size > 0 ? false : true;
		}

		@Override
		public boolean containsKey(Object key) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean containsValue(Object value) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public V get(Object key) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public V put(K key, V value) {
			throw new IllegalArgumentException();
		}

		@Override
		public V remove(Object key) {
			throw new IllegalArgumentException();
		}

		@Override
		public void putAll(Map<? extends K, ? extends V> m) {
			throw new IllegalArgumentException();
		}

		@Override
		public void clear() {
			RangeCommand cmd = new RangeCommand();
			cmd.setId(cmdCount.incrementAndGet());
			cmd.setType(RangeType.DELETERANGE);
			cmd.setSnapshot(snapshotID);
			try {
				client.range(cmd);
				logger.debug(this + " released iterator " + snapshotID);	
			} catch (MapError e) {
				logger.error(this + " error!",e);
			} catch (TException e) {
				logger.error(this + " error!",e);
			}				
		}

		@Override
		public Comparator<? super K> comparator() {
			return DistributedOrderedMap.this.comparator();
		}

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
		public K firstKey() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public K lastKey() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<K> keySet() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection<V> values() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<java.util.Map.Entry<K,V>> entrySet() {
			return new EntrySet(this);
		}

		@Override
		public String toString(){
			return DistributedOrderedMap.this + " snapshot: " + snapshotID + " size: " + size;
		}
	}

	
    class EntrySet extends AbstractSet<Map.Entry<K,V>> {

    	private final SnapshotView view;
    	
    	private final BlockingQueue<Map.Entry<K,V>> queue = new LinkedBlockingQueue<Map.Entry<K,V>>();
    		
    	public EntrySet(SnapshotView view){
    		this.view = view;
    		Thread t = new Thread(new QueueFiller(view,queue));
    		t.start();
    	}
    	
        public Iterator<Map.Entry<K,V>> iterator() {
            return new EntryIterator<Map.Entry<K,V>>(this,queue);
        }

        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> entry = (Map.Entry<?,?>) o;
            Object value = entry.getValue();
            V p = view.get(entry.getKey());
            return p != null && p.equals(value);
        }

        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> entry = (Map.Entry<?,?>) o;
            Object value = entry.getValue();
            V p = view.get(entry.getKey());
            if (p != null && p.equals(value)) {
                view.remove(entry.getKey());
                return true;
            }
            return false;
        }

        public int size() {
            return view.size();
        }

        public void clear() {
        	view.clear();
        }

        public Spliterator<Map.Entry<K,V>> spliterator() {
        	throw new UnsupportedOperationException();
        }
    }

    class EntryIterator<T> implements Iterator<T> {
        
    	private final EntrySet set;
    	
        private final BlockingQueue<T> queue;
        
        T last = null;

        EntryIterator(EntrySet set, BlockingQueue<T> queue) {
        	this.set = set;
        	this.queue = queue;
        }

        public final boolean hasNext() {
        	try {
				Thread.sleep(100); //FIXME: improve!!
			} catch (InterruptedException e) {
			}
        	return queue.peek() != null ? true : false;
        }

        public void remove() {
        	if(last == null){
        		throw new IllegalStateException();
        	}
        	set.remove(last);
        }
        
		@Override
		public T next() {
			T o = queue.poll(); //FIXME: improve!!
			last = o;
			if(o != null){
				return o;
			}else{
				throw new NoSuchElementException();
			}
		}
    }
    
    class QueueFiller implements Runnable {
    	
    	private final SnapshotView view;

    	private final BlockingQueue<Map.Entry<K,V>> queue;
    	
    	public QueueFiller(SnapshotView view,BlockingQueue<Map.Entry<K,V>> queue){
    		this.view = view;
    		this.queue = queue;
    	}
    	
    	@Override
    	public void run() {
    		long snapshotID = view.snapshotID;
    		long size = view.size;
    		long retreived = 0;
    		int from = 0;
    		while(retreived < size){
				try {
	    			RangeCommand cmd = new RangeCommand();
	    			cmd.setId(cmdCount.incrementAndGet());
	    			cmd.setType(RangeType.GETRANGE);
	    			cmd.setSnapshot(snapshotID);
	    			cmd.setFromid(from);
	    			cmd.setToid(from+get_range_size);
	    			from = from+get_range_size;
	    			RangeResponse ret = client.range(cmd);
	    			if(ret != null && ret.isSetValues()){
	    				@SuppressWarnings("unchecked")
						List<Pair<K,V>> sublist = (List<Pair<K,V>>) Utils.getObject(ret.getValues());
	    				for(Pair<K,V> e : sublist){
	    					queue.add(e);
	    					retreived++;
	    				}
	    			}
				} catch (MapError e){
					logger.error(view + " error!",e);
				} catch (TException | ClassNotFoundException | IOException e) {
					logger.error(view + " error!",e);
				}
    		}
    	}
    }

}
