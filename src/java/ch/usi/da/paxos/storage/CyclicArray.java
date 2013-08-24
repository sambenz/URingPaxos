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
 * @author Samuel Benz <benz@geoid.ch>
 */
public class CyclicArray implements StableStorage {

	private final static Logger logger = Logger.getLogger(CyclicArray.class);
	
	private native int init();
	
	private native void nput(int i,byte[] b);
	
	private native byte[] nget(int i);
	
	public CyclicArray(){
		try{
			System.loadLibrary("paxos");
			this.init(); // native calloc
			logger.info("CyclicArray JNI native array enabled!");
		}catch(UnsatisfiedLinkError e){
			//TODO: what about a fallback to Unsafe.x?
			logger.error("CyclicArray init error:",e);
		}
	}

	@Override
	public void put(Integer instance, Decision decision) {
		// not optimal with this kind of serialization; but still fast ...
		Message m = new Message(instance, decision.getRing(), PaxosRole.Proposer, MessageType.Value, decision.getBallot(), decision.getValue());
		nput(instance.intValue(),Message.toWire(m));
	}

	@Override
	public Decision get(Integer instance) {
		byte[] b = nget(instance.intValue());
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
	public native boolean contains(Integer instance);

	@Override
	public boolean trim(Integer instance) {
		// not interesting since cyclic storage
		return true;
	}

	@Override
	public void close(){
		
	}
	
	/**
	 * Debug method
	 */
	public static void main(String[] args){
		CyclicArray db = new CyclicArray();
		Decision d = new Decision(0,1,42,new Value("id","value".getBytes()));
		Decision d2 = new Decision(0,15001,43,new Value("id","value".getBytes()));

		db.put(d.getInstance(),d);
		db.put(d2.getInstance(),d);
		d = null;
		d2 = null;		
		System.gc();

		System.out.println(db.get(1));
		System.out.println(db.get(15001));
		
		System.out.println(db.contains(1));	
		System.out.println(db.contains(15001));
	}

}
