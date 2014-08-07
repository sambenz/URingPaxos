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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.api.StableStorage;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.Value;

/**
 * Name: CyclicArray<br>
 * Description: <br>
 * 
 * Creation date: Apr 23, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class CyclicArray implements StableStorage {

	private final static Logger logger = Logger.getLogger(CyclicArray.class);
	
	private long last_trimmed_instance = 0;
	
	private native int init();
	
	private native void nput(long i,byte[] b);
	
	private native byte[] nget(long i);
	
	private final Map<Long, Integer> promised = new LinkedHashMap<Long,Integer>(10000,0.75F,false){
		private static final long serialVersionUID = -2704400128020327063L;
			protected boolean removeEldestEntry(Map.Entry<Long, Integer> eldest) {  
				return size() > 15000; // hold only 15'000 values in memory !                                 
	}};

	public CyclicArray(){
		try{
			System.loadLibrary("paxos");
			this.init(); // native calloc
			logger.info("CyclicArray JNI native array enabled!");
		}catch(UnsatisfiedLinkError e){
			logger.error("CyclicArray init error:",e);
		}
	}

	@Override
	public void putBallot(Long instance, int ballot) {
		promised.put(instance, ballot);
	}

	@Override
	public int getBallot(Long instance) {
		return promised.get(instance);
	}

	@Override
	public synchronized boolean containsBallot(Long instance) {
		return promised.containsKey(instance);
	}
	
	@Override
	public void putDecision(Long instance, Decision decision) {
		// not optimal with this kind of serialization; but still fast ...
		Message m = new Message(instance, decision.getRing(), PaxosRole.Proposer, MessageType.Value, decision.getBallot(), decision.getBallot(), decision.getValue());
		nput(instance.longValue(),Message.toWire(m));
	}

	@Override
	public Decision getDecision(Long instance) {
		byte[] b = nget(instance.longValue());
		if(b.length > 0){
			Message m = Message.fromWire(b);
			if(m.getInstance() == instance){
				Decision d = new Decision(m.getSender(),m.getInstance(),m.getBallot(),m.getValue());
				return d;
			}else{
				return null;
			}
		}
		return null;
	}

	@Override
	public native boolean containsDecision(Long instance);

	@Override
	public boolean trim(Long instance) {
		last_trimmed_instance = instance;
		return true;
	}

	@Override
	public Long getLastTrimInstance() {
		return last_trimmed_instance;
	}

	@Override
	public void close(){
	
	}
	
	/**
	 * Debug method
	 */
	public static void main(String[] args){
		//FIXME: TODO: use it only once !!!!!
		CyclicArray db = new CyclicArray();
		CyclicArray db2 = new CyclicArray();

		Decision d = new Decision(0,Long.MAX_VALUE,42,new Value("id","value".getBytes()));
		Decision d2 = new Decision(0,15001L,43,new Value("id","value".getBytes()));

		db.putDecision(d.getInstance(),d);
		db2.putDecision(d2.getInstance(),d2);
		
		d = null;
		d2 = null;		
		System.gc();

		System.out.println(db.getDecision(Long.MAX_VALUE));
		System.out.println(db2.getDecision(15001L));

	}
	
}
