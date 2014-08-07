package ch.usi.da.paxos.ring;
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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.Message;

/**
 * Name: SessionHandler<br>
 * Description: <br>
 * 
 * Creation date: Apr 02, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class SessionHandler {
	
	private static final Logger logger = Logger.getLogger(SessionHandler.class);
	
	private final NetworkManager manager;
	
	private final ByteBuffer readBuffer;;
	
	private final ByteBuffer writeBuffer;

	private boolean preamble = true;
	
	private int msize;

	public SessionHandler(NetworkManager manager) {
		this.manager = manager;
		readBuffer = ByteBuffer.allocate(2097152);
		writeBuffer = ByteBuffer.allocate(2097152);
	}

	protected synchronized void handleReadable(SelectionKey key) {
		try {
			SocketChannel ch = (SocketChannel) key.channel();
			if (readBuffer.hasRemaining()) {
				int count = ch.read(readBuffer);
				readBuffer.flip();
				if(count < 0) {
					// client has close inputStream
					key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
					ch.socket().shutdownInput();
				}else if (count > 0) {
					outerloop:
					while(readBuffer.hasRemaining()){
						if(preamble){
							if(readBuffer.limit()-readBuffer.position() >= 8){
								while(readBuffer.getInt() != NetworkManager.MAGIC_NUMBER){
									readBuffer.position(readBuffer.position()-3);
									if(readBuffer.limit()-readBuffer.position() < 4){
										break outerloop;
									}
								}
								msize = readBuffer.getInt();
								if(manager.crc_32) msize += 8;
								preamble = false;
							}else{
								break;
							}
						}
						if(!preamble){
							if(readBuffer.limit()-readBuffer.position() >= msize){
								try{
									Message msg = Message.fromBuffer(readBuffer);
									if(manager.crc_32 && readBuffer.getLong() == Message.getCRC32(msg)){
										manager.recv_count++;
										manager.recv_bytes = manager.recv_bytes + Message.length(msg);
										manager.receive(msg);
									}else if(!manager.crc_32){
										manager.recv_count++;
										manager.recv_bytes = manager.recv_bytes + Message.length(msg);
										manager.receive(msg);										
									}else{
										logger.error("Error in SessionHandler: Message CRC fail!");
									}
								}catch(Exception e){
									logger.error("Error in SessionHandler during de-serializing!",e);
								}
								preamble = true;
							}else{
								break;
							}
						}
					}
					readBuffer.compact();
				}
			}
		} catch (ClosedChannelException e) {
			// an other thread closed this channel
		} catch (CancelledKeyException e) {
			// an other thread closed this channel
		} catch (Exception e) {
			logger.error("Error in SessionHandler during reading!",e);
		}
	}

	protected synchronized void handleWritable(SelectionKey key) {
		try {
			SocketChannel ch = (SocketChannel) key.channel();
			if (writeBuffer.capacity() - writeBuffer.remaining() > 0) {		
				writeBuffer.flip();
				ch.write(writeBuffer);
				writeBuffer.compact();
			}
		} catch (ClosedChannelException e) {
			// an other thread closed this channel
		} catch (CancelledKeyException e) {
			// an other thread closed this channel
		} catch (Exception e) {
			logger.error("Exception in SessionHandler during writing!",e);
		}
	}

}
