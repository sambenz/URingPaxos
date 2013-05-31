package ch.usi.da.paxos.lab;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.ring.Node;

import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

/**
 * Name: SCTPListener<br>
 * Description: <br>
 * 
 * Creation date: Aug 15, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class SCTPListenerTest implements Runnable {

	private final static Logger logger = Logger.getLogger(SCTPListenerTest.class);
	
	private final SctpServerChannel server;
	
	/**
	 * @throws IOException 
	 */
	public SCTPListenerTest() throws IOException{
		server = SctpServerChannel.open();
		InetSocketAddress addr = new InetSocketAddress(Node.getHostAddress(false),2020);
		server.bind(addr);
		server.configureBlocking(true);
	}
	
	@Override
	public void run() {
		try {
			while (server.isOpen()){
		        SctpChannel channel = server.accept();
		        Thread t = new Thread(new SctpConnectionHandler(channel));
		        t.setName("ConnectionHandler");
		        t.start();
			}
		} catch (IOException e) {
			logger.error(e);
		}	
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException{
		Thread t = new Thread(new SCTPListenerTest());
		t.setName("SCTPListener");
	    t.start();
		Thread t2 = new Thread(new SCTPSenderTest());
		t2.setName("SCTPSender");
	    t2.start();	
	}
}
