package ch.usi.da.paxos.old;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.storage.Promise;


/**
 * Name: ProposerReserver<br>
 * Description: <br>
 * 
 * Creation date: Apr 10, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ProposerReserver implements Runnable {

	private final Proposer proposer;
	
	private final DatagramChannel channel;
	
	private ByteBuffer buffer = ByteBuffer.allocate(8192);
	
	private final List<DatagramPacket> out = new ArrayList<DatagramPacket>();
	
	private final Selector selector;
	
	private Majority majority;
	
	private long instance;
	
	private int ballot;
	
	private int bcounter;
	
	private long start;
			
	/**
	 * Public constructor
	 * 
	 * @param proposer
	 * @throws IOException 
	 */
	public ProposerReserver(Proposer proposer) throws IOException{
		this.proposer = proposer;
		NetworkInterface i = NetworkInterface.getByName(Configuration.getInterface());
	    this.channel = DatagramChannel.open(StandardProtocolFamily.INET)
	         .setOption(StandardSocketOptions.SO_REUSEADDR, true)
	         .bind(Configuration.getGroup(PaxosRole.Proposer))
	         .setOption(StandardSocketOptions.IP_MULTICAST_IF, i);
	    this.channel.configureBlocking(false);
	    this.channel.join(Configuration.getGroup(PaxosRole.Proposer).getAddress(), i);
		selector = Selector.open();
	}
	
	@Override
	public void run() {
		while(proposer.isLeader()){
			try {
				instance = proposer.getInstance().incrementAndGet();
				ballot = 0;
				bcounter = 0;
				boolean promise = false;
				while(!promise){
					// new ballot = 'ballot counter' + 'ID'
					ballot = 10*bcounter+proposer.getID();

					// send 1a
					Message m = new Message(instance,proposer.getID(),PaxosRole.Acceptor,MessageType.Prepare,ballot,null);
					byte[] b = Message.toWire(m);
					DatagramPacket packet = new DatagramPacket(b,b.length,Configuration.getGroup(m.getReceiver()));
					out.add(packet);
					channel.register(selector,SelectionKey.OP_READ|SelectionKey.OP_WRITE);
					
					// wait 1b majority
					majority = new Majority();
					start = System.currentTimeMillis();
					while (!majority.isQuorum() && (System.currentTimeMillis() - start < 2000)){
						selector.select(1000);
						Set<SelectionKey> keys = selector.selectedKeys();
						synchronized (keys){
							Iterator<SelectionKey> it = keys.iterator();
							while (it.hasNext()){
								SelectionKey key = (SelectionKey)it.next();
								it.remove();
								if (!key.isValid())
									continue;
								if (key.isWritable()){
									write(key);
								}
								if (key.isReadable()){
									read(key);
								}
							}
						}
					}
					bcounter++;
					promise = majority.isQuorum();
				}
				if(!proposer.isPerfTest()){
					System.out.println("reserve ballot " + ballot + " in instance " + instance);
				}
				proposer.getPromiseQueue().put(new Promise(new Long(instance),new Integer(ballot)));
			} catch (InterruptedException e) {
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			selector.close();
			channel.close();
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
				//System.out.println("receive " + m);
				if(m != null){
					if(m.getType() == MessageType.Nack && m.getInstance() == instance){
						start = 0; // skip waiting
					}else if(m.getType() == MessageType.Promise && m.getInstance() == instance){
						if(m.getValue() == null && m.getBallot() >= ballot){
							majority.addMessage(m);
						}else if (m.getValue() != null){
							instance = proposer.getInstance().incrementAndGet();
							ballot = 0;
							bcounter = 0;
							start = 0;
							//re-send 2a
							Message resend = new Message(m.getInstance(),proposer.getID(),PaxosRole.Acceptor,MessageType.Accept,m.getBallot(),m.getValue());
							byte[] b = Message.toWire(resend);
							DatagramPacket packet = new DatagramPacket(b,b.length,Configuration.getGroup(m.getReceiver()));
							out.add(packet);
							key.interestOps(SelectionKey.OP_READ|SelectionKey.OP_WRITE);
						}
					}
				}
			}
			selector.wakeup();
		}catch (IOException e){
			e.printStackTrace();
		}
	}
	
	private void write(SelectionKey key){
		DatagramChannel channel = (DatagramChannel)key.channel();
		try {
			while (!out.isEmpty()){
				DatagramPacket	packet = (DatagramPacket)out.get(0);
				buffer.clear();
				if(packet != null){
					buffer.put(packet.getData());
					buffer.flip();
					channel.send(buffer, packet.getSocketAddress());
					if (buffer.hasRemaining())
						return;
				}
				out.remove(0);
			}
			key.interestOps(SelectionKey.OP_READ);
			selector.wakeup();
		}catch (IOException e){
			e.printStackTrace();
		}
	}

}
