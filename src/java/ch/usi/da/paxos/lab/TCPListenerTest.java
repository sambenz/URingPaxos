package ch.usi.da.paxos.lab;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

/**
 * Name: SCTPListener<br>
 * Description: <br>
 * 
 * Creation date: Aug 15, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class TCPListenerTest implements Runnable {

	private final static Logger logger = Logger.getLogger(TCPListenerTest.class);
	
	private final ServerSocket server;
	
	/**
	 * @throws IOException 
	 */
	public TCPListenerTest() throws IOException {
		server = new ServerSocket(2020);
	}
	
	@Override
	public void run() {
		try {
			while(true){
		        Socket socket = server.accept();
		        Thread t = new Thread(new TcpConnectionHandler(socket));
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
		Thread t = new Thread(new TCPListenerTest());
		t.setName("TCPListener");
	    t.start();
		Thread t2 = new Thread(new TCPSenderTest());
		t2.setName("TCPSender");
	    t2.start();	
	}
}
