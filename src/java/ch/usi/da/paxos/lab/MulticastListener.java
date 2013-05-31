package ch.usi.da.paxos.lab;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;

import ch.usi.da.paxos.Configuration;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.PaxosRole;

/**
 * Name: MulticastListener<br>
 * Description: <br>
 * 
 * Creation date: Apr 10, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class MulticastListener implements Runnable {

	private DatagramChannel channel;
	
	private final String name;
	
	private final Selector selector;
	
	private ByteBuffer buffer = ByteBuffer.allocate(8192);
	
	/**
	 * @param name
	 * @param channel
	 * @throws IOException
	 */
	public MulticastListener(String name,DatagramChannel channel) throws IOException{
		//this.channel = channel;
		this.name = name;
		NetworkInterface i = NetworkInterface.getByName(Configuration.getInterface());
	    this.channel = DatagramChannel.open(StandardProtocolFamily.INET)
	         .setOption(StandardSocketOptions.SO_REUSEADDR, true)
	         .bind(Configuration.getGroup(PaxosRole.Acceptor))
	         .setOption(StandardSocketOptions.IP_MULTICAST_IF, i);
	    this.channel.configureBlocking(false);
	    this.channel.join(Configuration.getGroup(PaxosRole.Acceptor).getAddress(), i);
		selector = Selector.open();
	}
	
	@Override
	public void run() {
		try {
			channel.register(selector,SelectionKey.OP_READ);
			while (selector.isOpen()){
				selector.select(2000);
				Set<SelectionKey> keys = selector.selectedKeys();
				synchronized (keys){
					Iterator<SelectionKey> it = keys.iterator();
					while (it.hasNext()){
						SelectionKey key = (SelectionKey)it.next();
						it.remove();
						if (!key.isValid())
							continue;
						if (key.isReadable()){
							read(key);
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void read(SelectionKey key){
		DatagramChannel channel = (DatagramChannel)key.channel();
		try{
			buffer.clear();
			SocketAddress address = channel.receive(buffer);
			if (address == null)
				return;
			buffer.flip();
			int	count = buffer.remaining();
			if (count > 0){
				byte[] bytes = new byte[count];
				buffer.get(bytes);
				DatagramPacket in = new DatagramPacket(bytes, count, address);
				Message m = Message.fromWire(in.getData());
				if(m != null){
					System.out.println(name + " receive: " + m);
				}
			}
			selector.wakeup();
		}catch (IOException e){
			e.printStackTrace();
		}
	}
}
