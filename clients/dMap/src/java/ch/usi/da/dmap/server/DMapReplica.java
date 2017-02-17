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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import ch.usi.da.dmap.thrift.gen.Command;
import ch.usi.da.dmap.thrift.gen.Dmap;
import ch.usi.da.dmap.thrift.gen.Dmap.Iface;
import ch.usi.da.dmap.thrift.gen.Dmap.Processor;
import ch.usi.da.dmap.utils.Pair;
import ch.usi.da.dmap.utils.Utils;
import ch.usi.da.dmap.thrift.gen.MapError;
import ch.usi.da.dmap.thrift.gen.RangeCommand;
import ch.usi.da.dmap.thrift.gen.RangeResponse;
import ch.usi.da.dmap.thrift.gen.Response;


/**
 * Name: DMapReplica<br>
 * Description: <br>
 * 
 * Creation date: Jan 28, 2017<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class DMapReplica<K,V> implements Dmap.Iface {

	private final static Logger logger = Logger.getLogger(DMapReplica.class);
	
	private volatile SortedMap<K,V> db;
	
	private final AtomicLong snapID = new AtomicLong(0); //TODO: will be replaced by instance
	
	private volatile Map<Long, List<Entry<K, V>>> snapshots = new LinkedHashMap<Long,List<Entry<K,V>>>(){
		private static final long serialVersionUID = -2704400124020327063L;
		protected boolean removeEldestEntry(Map.Entry<Long, List<Entry<K, V>>> eldest) {  
			return size() > 10; // hold only 10 snapshots in memory!                                 
		}};
	
	public DMapReplica(Comparator<? super K> comparator) {
		//this.nodeID = nodeID;
		//this.token = token;
		//setPartition(partitions.register(nodeID, ringID, ip, token));
		db = new  TreeMap<K,V>(comparator);
	}
	
	public DMapReplica() {
		db = new  TreeMap<K,V>();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Response execute(Command cmd) throws MapError, TException {
		logger.info("DMapReplica received " + cmd);
		Response response = new Response();
		response.setId(cmd.id);
		response.setCount(0);
		
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

			switch(cmd.type){
			case CLEAR:
				db.clear();
				break;
			case CONTAINSVALUE:
				db.containsValue(value);
				response.setCount(1);
				break;
			case GET:
				retV = db.get(key);
				break;
			case PUT:
				retV = db.put(key,value);
				break;
			case REMOVE:
				retV = db.remove(key);
				break;
			case SIZE:
				response.setCount(db.size());
				break;
			case FIRSTKEY:
				retK = db.firstKey();
				break;
			case LASTKEY:
				retK = db.lastKey();
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
	@Override
	public RangeResponse range(RangeCommand cmd) throws MapError, TException {
		logger.info("DMapReplica received " + cmd);
		RangeResponse response = new RangeResponse();
		response.setId(cmd.getId());
		List<Entry<K,V>> snapshot;
		
		try {
			switch(cmd.type){
			case PERSISTRANGE:
				if(cmd.isSetIdetifier() && snapshots.containsKey(cmd.getIdetifier())){
					//TODO: persist
				}
				break;
			case CREATERANGE:
				if(cmd.isSetFromkey() && cmd.isSetTokey()){
					K from = (K) Utils.getObject(cmd.getFromkey());
					K to = (K) Utils.getObject(cmd.getTokey());
					snapshot = new ArrayList<Entry<K,V>>(db.subMap(from,to).entrySet());
				}else if(cmd.isSetFromkey() && !cmd.isSetTokey()){
					K from = (K) Utils.getObject(cmd.getFromkey());
					snapshot = new ArrayList<Entry<K,V>>(db.tailMap(from).entrySet());
				}else if(!cmd.isSetFromkey() && cmd.isSetTokey()){
					K to = (K) Utils.getObject(cmd.getTokey());
					snapshot = new ArrayList<Entry<K,V>>(db.headMap(to).entrySet());
				}else{
					snapshot = new ArrayList<Entry<K,V>>(db.entrySet());
				}
				long id = snapID.incrementAndGet(); //TODO: should be instance
				snapshots.put(id,snapshot);
				response.setCount(snapshot.size());
				response.setIdetifier(id);
				break;
			case DELETERANGE:
				if(cmd.isSetIdetifier()){
					if(snapshots.containsKey(cmd.getIdetifier())){
						snapshots.remove(cmd.getIdetifier());
						response.setCount(1);
					}else{
						MapError e = new MapError();
						e.setErrorMsg("Snaphost " + cmd.getIdetifier() + " does not exist!");
						throw e;
					}
				}
				break;
			case GETRANGE:
				id = cmd.getIdetifier();
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
					List<Pair<K,V>> list = new ArrayList<Pair<K,V>>(); //sublist an TreeMap.Entry are not serializable!
					for(Entry<K,V> e : snapshot.subList(from,to)){
						list.add(new Pair<K,V>(e.getKey(),e.getValue()));
					}
					response.setCount(list.size());
					response.setValues(Utils.getBuffer(list));
				}else{
					MapError e = new MapError();
					e.setErrorMsg("Snaphost " + cmd.getIdetifier() + " does not exist!");
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
			//TODO: replace with thread pool server! -> but what about size in concurrent DB?
			TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(5800);
			Dmap.Processor<Iface> processor = new Processor<Iface>(new DMapReplica<Object,Object>());
			final TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
			server.serve();
			Runtime.getRuntime().addShutdownHook(new Thread("ShutdownHook"){
				@Override
				public void run(){
					server.stop();
				}
			});
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			in.readLine();
			server.stop();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
