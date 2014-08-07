package ch.usi.da.smr.message;
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

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import ch.usi.da.smr.thrift.gen.Cmd;
import ch.usi.da.smr.thrift.gen.Decision;

/**
 * Name: Message<br>
 * Description: <br>
 * 
 * Creation date: Mar 13, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Message {

	private final int id;
	
	private final String from;
	
	private final String to;
		
	private final List<Command> commands;
	
	private long instance = 0;
	
	private int ring = 0;
	
	private boolean skip = false;
	
	public Message(int id,String from,String to,List<Command> commands){
		this.id = id;
		this.from = from;
		this.to = to;
		this.commands = commands;
	}

	public int getID(){
		return id;
	}
	
	public String getFrom(){
		return from;
	}
	
	public String getTo(){
		return to;
	}
	
	public List<Command> getCommands(){
		return commands;
	}
		
	public void setInstance(long instance){
		this.instance = instance;
	}
	
	public long getInstnce(){
		return instance;
	}
	
	public void setRing(int ring){
		this.ring = ring;
	}
	
	public int getRing(){
		return ring;
	}

	public void setSkip(boolean skip){
		this.skip = skip;
	}
	
	public boolean isSkip(){
		return skip;
	}
	
	public String toString(){
		return ("Message id:" + id + " from:" + from + " to:" + to + " " + commands);
	}
	
	public boolean equals(Object obj) {
		if(obj instanceof Message){
            if(this.id == ((Message) obj).getID()){
                    return true;
            }
		}
		return false;
	}
	
	public int hashCode() {
		return id;
	}

	public static byte[] toByteArray(Message m){
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
		ch.usi.da.smr.thrift.gen.Message msg = new ch.usi.da.smr.thrift.gen.Message();
		msg.setId(m.getID());
		msg.setFrom(m.getFrom());
		msg.setTo(m.getTo());
		List<Cmd> cmds = new ArrayList<Cmd>();
		for(Command c : m.getCommands()){
			cmds.add(Command.toCmd(c));
		}
		msg.setCommands(cmds);
		try {
			return serializer.serialize(msg);
		} catch (TException e) {
			return new byte[0];
		}
	}
	
	public static Message fromDecision(Decision decision){
		Message m = null;
		if(decision.isSetValue() && decision.getValue().isSkip()){
			m = new Message(0,"","",null);
			m.setSkip(true);
		}else{
			m = fromByteArray(decision.getValue().getCmd());
		}
		if(m != null){
			m.setInstance(decision.getInstance());
			m.setRing(decision.getRing());
		}
		return m;
	}

	public static Message fromDecision(ch.usi.da.paxos.storage.Decision decision){
		Message m = null;
		if(decision.getValue() != null && decision.getValue().isSkip()){
			m = new Message(0,"","",null);
			m.setSkip(true);
		}else{
			m = fromByteArray(decision.getValue().getValue());
		}
		if(m != null){
			m.setInstance(decision.getInstance());
			m.setRing(decision.getRing());
		}
		return m;
	}

	public static Message fromByteArray(byte[] b){
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		ch.usi.da.smr.thrift.gen.Message m = new ch.usi.da.smr.thrift.gen.Message();
		try {
			deserializer.deserialize(m, b);
			if(m.to == null){
				return null;
			}
		} catch (TException e) {
			return null;
		}
		List<Command> cmds = new ArrayList<Command>(); 
		for(Cmd c : m.getCommands()){
			cmds.add(Command.toCommand(c));
		}
		return new Message(m.getId(),m.getFrom(),m.getTo(),cmds);
	}
}
