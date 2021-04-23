package ch.usi.da.smr.message;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import ch.usi.da.smr.thrift.gen.Cmd;
import ch.usi.da.smr.thrift.gen.CmdType;
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


/**
 * Name: Command<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Command {

	private final int id;
	
	private final CommandType type;
	
	private final String key;
	
	private final byte[] value;
	
	private final int count;
	
	public Command(int id,CommandType type,String key,byte[] value){
		this.id = id;
		this.type = type;
		this.key = new String(key);
		this.value = value;
		this.count = 100;
	}

	public Command(int id,CommandType type,String key,byte[] value,int count){
		this.id = id;
		this.type = type;
		this.key = new String(key);
		this.value = value;
		this.count = count;
	}

	public int getID(){
		return id;
	}
	
	public CommandType getType(){
		return type;
	}
	
	public String getKey(){
		return key;
	}
	
	public byte[] getValue(){
		return value;
	}
	
	public int getCount(){
		return count;
	}
	
	public String toString(){
		return ("Command id:" + id + " type:" + type + " key:" + key);
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
		return (int) (id + type.ordinal() + key.hashCode());
	}

	public static Cmd toCmd(Command c){
		Cmd cmd = new Cmd();
		cmd.setId(c.getID());
		cmd.setCount(c.getCount());
		switch(c.getType()){
		case DELETE:
			cmd.setType(CmdType.DELETE); break;
		case PUT:
			cmd.setType(CmdType.PUT); break;
		case GET:
			cmd.setType(CmdType.GET); break;
		case GETRANGE:
			cmd.setType(CmdType.GETRANGE); break;			
		case RESPONSE:
			cmd.setType(CmdType.RESPONSE); break;
		}
		cmd.setKey(c.getKey());
		cmd.setValue(c.getValue());
		return cmd;
	}
	
	public static byte[] toByteArray(Command c){
		Cmd cmd = toCmd(c);
		try {
			TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
			return serializer.serialize(cmd);
		} catch (TException e) {
			return new byte[0];
		}
	}

	/*public static byte[] toByteArray(Command c){
		ByteBuffer b = ByteBuffer.allocate(65535);
		// int id
		// int type
		// String key
		// byte[] value
		// int count
		b.putInt(c.getID());
		b.putShort((short)c.getType().getId());
		b.putInt(c.getKey().getBytes().length);
		b.put(c.getKey().getBytes());
		b.putInt(c.getValue().length);
		b.put(c.getValue());
		b.putInt(c.getCount());
		
		byte[] a = new byte[b.position()];
		b.rewind();		
		b.get(a);
		return a;
	}*/

	public static Command toCommand(Cmd c) {
		CommandType type = null;
		switch(c.getType()){
		case DELETE:
			type = CommandType.DELETE; break;
		case GET:
			type = CommandType.GET; break;
		case PUT:
			type = CommandType.PUT; break;
		case RESPONSE:
			type = CommandType.RESPONSE; break;
		case GETRANGE:
			type = CommandType.GETRANGE; break;
		default:
			break;
		}
		return new Command(c.getId(),type,c.getKey(),c.getValue(),c.getCount());
	}
	
	public static Command fromByteArray(byte[] b){
		Cmd c = new Cmd();
		try {
			TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
			deserializer.deserialize(c, b);
			if(c.type == null){
				return null;
			}
		} catch (TException e) {
			return null;
		}
		return toCommand(c);
	}
	
	/*public static Command fromByteArray(byte[] b){
		ByteBuffer buffer = ByteBuffer.wrap(b);
		int id = buffer.getInt();
		CommandType type = CommandType.fromId(buffer.getShort());
		byte[] kb = new byte[buffer.getInt()];
		buffer.get(kb);
		String key = new String(kb);
		byte[] value = new byte[buffer.getInt()];
		buffer.get(value);
		int count = buffer.getInt();
		Command cmd = new Command(id,type,key,value,count);
		return cmd;
	}*/

}
