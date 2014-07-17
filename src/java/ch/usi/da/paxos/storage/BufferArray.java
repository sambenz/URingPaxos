package ch.usi.da.paxos.storage;
/* 
 * Copyright (c) 2014 Università della Svizzera italiana (USI)
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

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.api.StableStorage;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;


/**
 * Name: BufferArray<br>
 * Description: <br>
 * 
 * Creation date: Jul 17, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class BufferArray implements StableStorage {

	private final static Logger logger = Logger.getLogger(StableStorage.class);
	
	private final int size = 64*1024;
	
	private final int max = 15000;
	
	private final Map<Long, Integer> promised = new LinkedHashMap<Long,Integer>(10000,0.75F,false){
		private static final long serialVersionUID = -2714400128020327063L;
			protected boolean removeEldestEntry(Map.Entry<Long, Integer> eldest) {  
				return size() > max;
	}};

	private long last_trimmed_instance = 0;
	
	private final ByteBuffer[] buffer = new ByteBuffer[max];
	
	private final Long[] instances = new Long[max];
	
	public BufferArray(){
		logger.info("BufferArray storage allocates " + ((max*size)/(1024*1024)) + " MB memory");
		for(int i=0;i<max;i++){
			buffer[i] = ByteBuffer.allocateDirect(size);
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
	public boolean containsBallot(Long instance) {
		return promised.containsKey(instance);
	}

	@Override
	public void putDecision(Long instance, Decision decision) {
		Message m = new Message(instance, decision.getRing(), PaxosRole.Proposer, MessageType.Value, decision.getBallot(), decision.getBallot(), decision.getValue());
		buffer[(int)(instance % max)].clear();
		Message.toBuffer(buffer[(int)(instance % max)],m);
		instances[(int)(instance % max)] = instance;
	}

	@Override
	public Decision getDecision(Long instance) {
		try {
			buffer[(int)(instance % max)].rewind();
			Message m = Message.fromBuffer(buffer[(int)(instance % max)]);
			if(m.getInstance() == instance){
				Decision d = new Decision(m.getSender(),m.getInstance(),m.getBallot(),m.getValue());
				return d;
			}else{
				return null;
			}
		} catch (Exception e) {
			logger.error("CyclicBuffer error:",e);
			return null;
		}
	}

	@Override
	public boolean containsDecision(Long instance) {
		if(instances[(int)(instance % max)] == null){
			return false;
		}else{
			if(instances[(int)(instance % max)].equals(instance)){
				return true;
			}
		}
		return false;
	}

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
	
}
