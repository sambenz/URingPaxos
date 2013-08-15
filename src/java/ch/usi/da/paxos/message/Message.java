package ch.usi.da.paxos.message;
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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import ch.usi.da.paxos.api.PaxosRole;


/**
 * Name: Message<br>
 * Description: <br>
 * 
 * Creation date: Mar 31, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class Message implements Serializable {

	private static final long serialVersionUID = -4938636847085992695L;
	
	private final int instance;
	
	private final int sender;
	
	private final PaxosRole receiver;
	
	private final MessageType type;
	
	private final int ballot;
	
	private final Value value;
	
	private int vote_count = 0;
	
	/**
	 * Public constructor
	 * 
	 * @param instance the instance number
	 * @param sender the sender id
	 * @param receiver the receiver id
	 * @param type the message type
	 * @param ballot the ballot number
	 * @param value the value (can be null)
	 */
	public Message(int instance,int sender,PaxosRole receiver,MessageType type,int ballot,Value value){
		this.instance = instance;
		this.sender = sender;
		this.receiver = receiver;
		this.type = type;
		this.ballot = ballot;
		this.value = value;
	}

	/**
	 * @return the instance
	 */
	public int getInstance() {
		return instance;
	}

	/**
	 * @return the sender
	 */
	public int getSender() {
		return sender;
	}

	/**
	 * @return the receiver
	 */
	public PaxosRole getReceiver() {
		return receiver;
	}

	/**
	 * @return the type
	 */
	public MessageType getType() {
		return type;
	}

	/**
	 * @return the ballot
	 */
	public int getBallot() {
		return ballot;
	}

	/**
	 * @return the value
	 */
	public Value getValue() {
		return value;
	}

	/**
	 * @return the vote counter
	 */
	public synchronized int getVoteCount(){
		return vote_count;
	}
	
	/**
	 * @param c set the vote counter
	 */
	public synchronized void setVoteCount(int c){
		vote_count = c;
	}
	
	/**
	 * Increment the vote counter
	 */
	public synchronized void incrementVoteCount(){
		vote_count = vote_count + 1;
	}
	
	public String toString(){
		if(vote_count > 0){
			return (this.getType() + " from:" + this.getSender() + " to:" + this.getReceiver() + " instance:" + this.instance + " ballot:" + this.getBallot() + " value:" + this.getValue() + " votes:" + vote_count);
		}else{
			return (this.getType() + " from:" + this.getSender() + " to:" + this.getReceiver() + " instance:" + this.instance + " ballot:" + this.getBallot() + " value:" + this.getValue());
		}
	}
	
	/**
	 * An convenient way for wire representation 
	 * 
	 * @param m
	 * @return message byte array
	 */
	public static byte[] toWire(Message m){
		ByteBuffer buffer = ByteBuffer.allocate(length(m));
		toBuffer(buffer,m);
		return buffer.array();
		
		/*ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null; 
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(m);
			return bos.toByteArray();
		} catch (IOException e) {
			return new byte[0];
		} finally {
			try {
				bos.close();
			} catch (IOException e) {
			}
			if(out != null){
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}*/
	}
	
	/**
	 * This method is not really efficient; but convenient for std. IO
	 * 
	 * @param b
	 * @return Message object
	 */
	public static Message fromWire(byte[] b) {
		ByteBuffer buffer = ByteBuffer.wrap(b);
		try {
			return fromBuffer(buffer);
		} catch (Exception e) {
			return null;
		}
		
		/*ByteArrayInputStream bis = new ByteArrayInputStream(b);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			return (Message) in.readObject();
		} catch (IOException e) {
			return null;
		} catch (ClassNotFoundException e) {
			return null;
		} finally {
			try {
				bis.close();
			} catch (IOException e) {
			}
			if(in != null){
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		}*/
	}

	/**
	 * Get the Message length (without length prefix)
	 * 
	 * @param m
	 * @return length of the message on the wire (without length prefix)
	 */
	public static int length(Message m){
		int length = 24;
		if(m.getValue() != null){
			length = length + m.getValue().getByteID().length + 4 + m.getValue().getValue().length;
		}
		return length;
	}

	/**
	 * Get the CRC32 of the Message (without the Value)
	 * 
	 * @param m
	 * @return crc32 of the message (without the Value)
	 */
	public static long getCRC32(Message m){
		if(m == null) return 0;
		CRC32 crc = new CRC32();
		crc.update(m.getInstance());
		crc.update(m.getSender());
		crc.update(m.getReceiver().getId());
		crc.update(m.getType().getId());
		crc.update(m.getBallot());
		crc.update(m.getVoteCount());
		return crc.getValue();
	}
	
	/**
	 * This is the recommended way to serialize and send a Message trough NIO
	 * 
	 * @param b
	 * @param m
	 */
	public static void toBuffer(ByteBuffer b,Message m){
		// int   instance
		// int   sender
		// short role
		// short type
		// int   ballot
		// int   vote count
		// int   ID length (or -1)
		//   byte[]ID
		//   int   value length
		//   byte[]value
		b.putInt(m.getInstance());
		b.putInt(m.getSender());
		b.putShort((short)m.getReceiver().getId());
		b.putShort((short)m.getType().getId());
		b.putInt(m.getBallot());
		b.putInt(m.getVoteCount());
		if(m.getValue() != null){
			b.putInt(m.getValue().getByteID().length);
			b.put(m.getValue().getByteID());
			b.putInt(m.getValue().getValue().length);
			b.put(m.getValue().getValue());
		}else{
			b.putInt(-1);
		}		
	}

	/**
	 * This is the recommended way de-serialize and receive a Message trough NIO
	 * 
	 * @param buffer
	 * @return Message object
	 */
	public static Message fromBuffer(ByteBuffer buffer) throws Exception {
		int instance = buffer.getInt();
		int sender = buffer.getInt();
		PaxosRole role = PaxosRole.fromId(buffer.getShort());
		MessageType type = MessageType.fromId(buffer.getShort());
		int ballot = buffer.getInt();
		int vote_count = buffer.getInt();
		int id_length = buffer.getInt();
		Value value = null;
		if(id_length >= 0){
			byte[] ib = new byte[id_length];
			buffer.get(ib);
			String id = new String(ib);
			int v_length = buffer.getInt();
			byte[] vb = new byte[v_length];
			buffer.get(vb);
			value = new Value(id,vb);
		}
		Message msg = new Message(instance,sender,role,type,ballot,value);
		msg.setVoteCount(vote_count);
		return msg;
	}
}
