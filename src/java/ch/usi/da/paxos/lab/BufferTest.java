package ch.usi.da.paxos.lab;
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

import java.nio.ByteBuffer;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.ring.NetworkManager;

public class BufferTest {

	boolean preamble = true;
	int msize;

	public void test(){
		ByteBuffer buffer = ByteBuffer.allocate(500);

		Value v    = new Value("ID","Value".getBytes());
		Message m  = new Message(1, 2, PaxosRole.Acceptor, MessageType.Phase1, 0, 0, v);
		Message m2 = new Message(2, 3, PaxosRole.Learner, MessageType.Value, 0, 0, v);
		
		//System.err.println(Message.getCRC32(m));
		//System.err.println(Message.getCRC32(Message.fromWire(Message.toWire(m))));
		
		if(buffer.remaining() >= Message.length(m)+16){
			buffer.putInt(NetworkManager.MAGIC_NUMBER);
			buffer.putInt(Message.length(m));
			Message.toBuffer(buffer, m);
			buffer.putLong(Message.getCRC32(m));
		}

		buffer.put(11,(byte) 0xff);

		buffer.putShort((short) 34);
		
		if(buffer.remaining() >= Message.length(m)+16){
			buffer.putInt(NetworkManager.MAGIC_NUMBER);
			buffer.putInt(Message.length(m2));
			Message.toBuffer(buffer, m2);
			buffer.putLong(Message.getCRC32(m2));
		}
		
		buffer.flip();
		readBuffer(buffer);		
		
		/*byte[] b2 = Message.toWire(m2);
		buffer.put(b2,0,10);
		//buffer.put(b2,10,b2.length-10);
		buffer.flip();

		readBuffer(buffer);
		
		System.out.println("3: " + buffer.position() + "," + buffer.limit() + "," + buffer.capacity());
		
		buffer.put(b2,10,b2.length-10);
		buffer.flip();

		System.out.println("4: " + buffer.position() + "," + buffer.limit() + "," + buffer.capacity());

		readBuffer(buffer);		
		System.out.println("5: " + buffer.position() + "," + buffer.limit() + "," + buffer.capacity());
		*/
	}

	public void readBuffer(ByteBuffer buffer){
		outerloop:
		while(buffer.hasRemaining()){
			if(preamble){
				if(buffer.limit()-buffer.position() >= 8){
					while(buffer.getInt() != NetworkManager.MAGIC_NUMBER){
						buffer.position(buffer.position()-3);
						if(buffer.limit()-buffer.position() < 4){
							break outerloop;
						}
					}
					msize = buffer.getInt();
					preamble = false;
				}else{
					break;
				}
			}
			if(!preamble){
				//System.err.println(buffer.limit()-buffer.position());
				if(buffer.limit()-buffer.position() >= msize){
					Message m = null;
					try {
						m = Message.fromBuffer(buffer);
						System.out.print(m);
					} catch (Exception e) {
						System.out.print("null");
					}
					long crc = buffer.getLong();
					if(crc == Message.getCRC32(m)){
						System.out.println(" CRC OK!");
					}else{
						System.out.println(" CRC FAIL!");						
					}
					preamble = true;
				}else{
					break;
				}
			}
			//System.out.println("in: " + buffer.position() + "," + buffer.limit() + "," + buffer.capacity());
		}
		buffer.compact();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferTest test = new BufferTest();
		test.test();
	}
}
