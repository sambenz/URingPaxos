package ch.usi.da.paxos.storage;
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

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.StableStorage;
import ch.usi.da.paxos.message.Value;


/**
 * Name: MemcachedStorage<br>
 * Description: <br>
 * 
 * Creation date: Apr 07, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class MemcachedStorage implements StableStorage {

	private final static Logger logger = Logger.getLogger(MemcachedStorage.class);

	private MemcachedClient cache;
	
	private final String prefix = "prefix"; // can be used to use one memcached for different acceptors
	
	private final int max = 15000;
	
	public MemcachedStorage(){
		try {
			System.getProperties().put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
			cache = new MemcachedClient(new BinaryConnectionFactory(),AddrUtil.getAddresses("localhost:11211"));
			cache.flush(); // delete at start
		} catch (IOException e) {
			logger.error("MemcachedStorage could not connect to memcached!",e);
		}
	}
	
	@Override
	public void put(Integer instance, Decision decision) {
		cache.set(prefix + "-" + instance%max,0,decision); // non-blocking
		/*OperationFuture<Boolean> r = cache.set(prefix + "-" + instance%max,0,decision);
		try {
			r.get();
		} catch (InterruptedException e){
		} catch (ExecutionException e) {
			logger.error("MemcachedStorage could not set instance " + instance%max,e);			
		}*/
	}

	@Override
	public Decision get(Integer instance) {
		return (Decision) cache.get(prefix + "-" + instance);
	}

	@Override
	public boolean contains(Integer instance) {
		return get(instance) != null ? true : false;
	}

	@Override
	public boolean trim(Integer instance) {
		cache.set("last_trim",0, instance);
		return true;
	}

	@Override
	public Integer getLastTrimInstance() {
		return (Integer) cache.get("last_trim");
	}

	@Override
	public void close(){
		cache.shutdown();
	}
	
	/**
	 * Debug method
	 */
	public static void main(String[] args){
		MemcachedStorage db = new MemcachedStorage();
		Decision d = new Decision(0,1,42,new Value("id","value".getBytes()));
		Decision d2 = new Decision(0,1,43,new Value("id","value".getBytes()));
		System.out.println(db.contains(1));
		db.put(1,d);
		db.put(15001,d2);		
		System.out.println(db.contains(1));
		System.out.println(db.get(1));
		db.close();
	}
}
