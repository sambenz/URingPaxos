package ch.usi.da.paxos.lab;

import java.io.IOException;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;

import ch.usi.da.paxos.Configuration;
import ch.usi.da.paxos.message.PaxosRole;

/**
 * Name: Multicast<br>
 * Description: <br>
 * 
 * Creation date: Mar 31, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class Multicast {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(Configuration.getConfiguration().isEmpty()){
			Configuration.read(args[0]);
		}
		
		/** old fashion
		// open
		int port = Configuration.getGroup(MessageGroup.Acceptor).getPort();
		MulticastSocket socket = new MulticastSocket(port);
		InetAddress addr = Configuration.getGroup(MessageGroup.Acceptor).getAddress();
		socket.joinGroup(addr);
		socket.setSoTimeout(5000);
		
		// send
		Message test = new Message(0,1,MessageGroup.Acceptor,MessageType.Prepare, 01, 42);
		byte[] m = Message.toWire(test);
		DatagramPacket packet = new DatagramPacket(m,m.length,Configuration.getGroup(MessageGroup.Acceptor));
		socket.send(packet);

		// receive
		byte[] buffer = new byte[8192];
		DatagramPacket p = new DatagramPacket(buffer,0,buffer.length);
		socket.receive(p);
		System.out.println(Message.fromWire(p.getData()).getValue());

		// close
		socket.close();
		*/
		
		NetworkInterface i = NetworkInterface.getByName(Configuration.getInterface());
	    DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET)
	         .setOption(StandardSocketOptions.SO_REUSEADDR, true)
	         .bind(Configuration.getGroup(PaxosRole.Acceptor))
	         .setOption(StandardSocketOptions.IP_MULTICAST_IF, i);
	    channel.configureBlocking(false);
	    channel.join(Configuration.getGroup(PaxosRole.Acceptor).getAddress(), i);
	    
	    Thread t = new Thread(new MulticastListener("1",channel));
	    t.start();
	    
	    Thread t2 = new Thread(new MulticastListener("2",channel));
	    t2.start();

	}
}
