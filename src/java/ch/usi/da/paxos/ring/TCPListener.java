package ch.usi.da.paxos.ring;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Name: TCPListener<br>
 * Description: <br>
 * 
 * Creation date: Feb 16, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class TCPListener implements Runnable {

	private final static Logger logger = Logger.getLogger(TCPListener.class);
	
	private final NetworkManager manager;
	
	private final ServerSocketChannel server;
	
	private final Selector selector;
		
	/**
	 * @param manager
	 * @throws IOException 
	 */
	public TCPListener(NetworkManager manager,ServerSocketChannel server,Selector selector) throws IOException{
		this.manager = manager;
		this.server = server;
		this.selector = selector;
	}
	
	@Override
	public void run() {
		while(selector.isOpen()){
			try{
				selector.select();
				Set<SelectionKey> readyKeys = selector.selectedKeys();
				synchronized (readyKeys) {
					Iterator<SelectionKey> it = readyKeys.iterator();
					while (it.hasNext()) {
						SelectionKey key = (SelectionKey) it.next();
						it.remove();
						if (!key.isValid()) {
							continue;
						}
						if (key.isAcceptable()){
							SocketChannel ch = server.accept();
							if (ch != null) {
								ch.configureBlocking(false);
								ch.socket().setSendBufferSize(manager.buf_size);
								ch.register(key.selector(), SelectionKey.OP_READ, new SessionHandler(manager));
							}
						}else if (key.isReadable() || key.isWritable()){
							SessionHandler handler = (SessionHandler) key.attachment();
							if (key.isWritable()) {
								handler.handleWritable(key);
							} 
							if (key.isReadable()) {
								handler.handleReadable(key);
							}				
							selector.wakeup(); 
						}
					}
				}
			} catch (CancelledKeyException e) {
				// do nothing; server or other worker closed the connection
			} catch (ClosedSelectorException e) {
				// do nothing; server or other worker closed the connection
			} catch (Exception e) {
				logger.error("TCPListener selector error",e);
			}
		}
	}

}
