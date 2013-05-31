package ch.usi.da.paxos.ring;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.Message;

/**
 * Name: TCPSender<br>
 * Description: <br>
 * 
 * Creation date: Apr 3, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class TCPSender implements Runnable {

	private final static Logger logger = Logger.getLogger(TCPSender.class);
	
	private final NetworkManager manager;
	
	private final SocketChannel client;
	
	private final TransferQueue<Message> send_queue;
	
	private final ByteBuffer buffer = ByteBuffer.allocate(65536);
	
	/**
	 * @param manager
	 * @throws IOException 
	 */
	public TCPSender(NetworkManager manager,SocketChannel socket,TransferQueue<Message> queue) throws IOException{
		this.manager = manager;
		this.client = socket;
		this.send_queue = queue;
	}
	
	@Override
	public void run() {
		Message m = null;
		while(client.isConnected()){
			try {
				m = send_queue.poll(1000,TimeUnit.SECONDS);
				if(m != null){
					int lenght = Message.length(m);
					if(buffer.remaining() >= lenght+4){
						//TODO: should also include magic number to sync after fail!
						buffer.putInt(lenght);
						Message.toBuffer(buffer, m);
						buffer.flip();
						client.write(buffer); // client runs in blocking mode !
						buffer.compact();
						manager.send_count++;
						manager.send_bytes = manager.send_bytes + lenght;
					}else{
						logger.error("TCPSender buffer too small!");
						send_queue.add(m);
					}
				}
			} catch (SocketException | ClosedChannelException | CancelledKeyException e ) {
				if(m != null){ // put back if already closed
					send_queue.add(m);
				}
			} catch (IOException e) {
				logger.error("TCPSender send error",e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}
}
