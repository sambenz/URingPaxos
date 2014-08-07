package ch.usi.da.dlog.message;
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

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import ch.usi.da.dlog.thrift.gen.Cmd;
import ch.usi.da.dlog.thrift.gen.CmdType;

/**
 * Name: Command<br>
 * Description: <br>
 * 
 * Creation date: Apr 07, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Command {

	private final int id;
	
	private final CommandType type;
	
	private final long position;
	
	private final byte[] value;
	
	private final int count;
	
	public Command(int id,CommandType type,long position,byte[] value){
		this.id = id;
		this.type = type;
		this.position = position;
		this.value = value;
		this.count = -1;
	}

	public Command(int id,CommandType type,long position,byte[] value,int count){
		this.id = id;
		this.type = type;
		this.position = position;
		this.value = value;
		this.count = count;
	}

	public int getID(){
		return id;
	}
	
	public CommandType getType(){
		return type;
	}
	
	public long getPosition(){
		return position;
	}
	
	public byte[] getValue(){
		return value;
	}
	
	public int getCount(){
		return count;
	}
	
	public String toString(){
		return ("Command id:" + id + " type:" + type + " position:" + position);
	}
	
	public boolean equals(Object obj) {
		if(obj instanceof Command){
            if(this.hashCode() == ((Command) obj).hashCode()){
                    return true;
            }
		}
		return false;
	}
	
	public int hashCode() {
		return (int) (id + type.ordinal() + position);
	}

	public static Cmd toCmd(Command c){
		Cmd cmd = new Cmd();
		cmd.setId(c.getID());
		cmd.setCount(c.getCount());
		switch(c.getType()){
		case APPEND:
			cmd.setType(CmdType.APPEND); break;
		case MULTIAPPEND:
			cmd.setType(CmdType.MULTIAPPEND); break;
		case READ:
			cmd.setType(CmdType.READ); break;
		case TRIM:
			cmd.setType(CmdType.TRIM); break;			
		case RESPONSE:
			cmd.setType(CmdType.RESPONSE); break;
		}
		cmd.setPosition(c.getPosition());
		cmd.setValue(c.getValue());
		return cmd;
	}
	
	public static byte[] toByteArray(Command c){
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
		Cmd cmd = toCmd(c);
		try {
			return serializer.serialize(cmd);
		} catch (TException e) {
			return new byte[0];
		}
	}
	
	public static Command toCommand(Cmd c) {
		CommandType type = null;
		switch(c.getType()){
		case APPEND:
			type = CommandType.APPEND; break;
		case MULTIAPPEND:
			type = CommandType.MULTIAPPEND; break;
		case READ:
			type = CommandType.READ; break;
		case TRIM:
			type = CommandType.TRIM; break;
		case RESPONSE:
			type = CommandType.RESPONSE; break;
		}
		return new Command(c.getId(),type,c.getPosition(),c.getValue(),c.getCount());
	}
	
	public static Command fromByteArray(byte[] b){
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		Cmd c = new Cmd();
		try {
			deserializer.deserialize(c, b);
			if(c.type == null){
				return null;
			}
		} catch (TException e) {
			return null;
		}
		return toCommand(c);
	}
}
