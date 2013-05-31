package ch.usi.da.paxos.lab;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.message.Value;

/**
 * Name: SCTPListener<br>
 * Description: <br>
 * 
 * Creation date: Aug 15, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class TCPSenderTest implements Runnable {

	private final static Logger logger = Logger.getLogger(TCPSenderTest.class);
	
	private final Socket socket;
		
	/**
	 * @throws IOException 
	 */
	public TCPSenderTest() throws IOException{
		socket = new Socket("127.0.0.1", 2020);
		socket.setTcpNoDelay(true);
	}
	
	@Override
	public void run() {
		int send = 0;
		try {
			OutputStream out = socket.getOutputStream();
			while(send < 500000){
				Long t = System.nanoTime();
				Message m = new Message(0,0,PaxosRole.Leader,MessageType.Value,0,new Value(t.toString(),new byte[8912]));
				byte[] b = Message.toWire(m);
				out.write(intToByte(b.length));
				out.write(b);
				out.flush();
				send++;
			}
		} catch (IOException e) {
			logger.error(e);
		}	
	}
	
	/**
	 * @param value
	 * @return a byte[]
	 */
	public static final byte[] intToByte(int value) {
	    return new byte[] {
	            (byte)(value >>> 24),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
}
