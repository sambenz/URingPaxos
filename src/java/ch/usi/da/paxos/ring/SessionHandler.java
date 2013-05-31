package ch.usi.da.paxos.ring;

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
 * @author benz@geoid.ch
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
					while(readBuffer.hasRemaining()){
						if(preamble){
							if(readBuffer.limit()-readBuffer.position() >= 4){
								msize = readBuffer.getInt();
								preamble = false;
							}else{
								break;
							}
						}
						if(!preamble){
							if(readBuffer.limit()-readBuffer.position() >= msize){
								Message msg = Message.fromBuffer(readBuffer);
								manager.recv_count++;
								manager.recv_bytes = manager.recv_bytes + Message.length(msg);
								manager.receive(msg);
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
