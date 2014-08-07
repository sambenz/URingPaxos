package ch.usi.da.paxos;
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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import ch.usi.da.paxos.message.Message;

/**
 * Name: AccepterListener<br>
 * Description: <br>
 * 
 * Creation date: Apr 1, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class AcceptorListener implements Runnable {

	private final Acceptor acceptor;
	
	private final DatagramChannel channel;
	
	private ByteBuffer buffer = ByteBuffer.allocate(8192);
	
	private List<DatagramPacket> out = new LinkedList<DatagramPacket>();
	
	private final Selector selector;
	
	/**
	 * Public constructor
	 * 
	 * @param acceptor
	 * @throws IOException 
	 */
	public AcceptorListener(Acceptor acceptor) throws IOException{
		this.acceptor = acceptor;
		this.channel = acceptor.getChannel();
		selector = Selector.open();
	}
	
	@Override
	public void run() {
		try{
			channel.register(selector, SelectionKey.OP_READ);
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
						if (key.isWritable()){
							write(key);
						}
					}
				}
			}
			selector.close();
		}catch(IOException e){
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
					// process paxos for the message instance
					Paxos paxos;
					synchronized(acceptor.getInstanceList()){
						if(!acceptor.getInstanceList().containsKey(m.getInstance())){
							paxos = new Paxos(acceptor,m.getInstance());
							acceptor.getInstanceList().put(m.getInstance(),paxos);
						}else{
							paxos = acceptor.getInstanceList().get(m.getInstance());
						}
					}
					if(paxos != null){
						synchronized (paxos){
							List<DatagramPacket> o = paxos.process(in);
							out.addAll(o);
							if(!o.isEmpty()){
								key.interestOps(SelectionKey.OP_READ|SelectionKey.OP_WRITE);
							}					
						}
						if(paxos.getValue() != null){
							acceptor.getInstanceList().remove(m.getInstance());
							//System.out.println(acceptor.getHistory().get(m.getInstance()));
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
				DatagramPacket packet = (DatagramPacket)out.get(0);
				buffer.clear();
				buffer.put(packet.getData());
				buffer.flip();
				channel.send(buffer, packet.getSocketAddress());
				//System.err.println("send " + Message.fromWire(packet.getData()));
				if (buffer.hasRemaining())
					return;
				out.remove(0);
			}
			key.interestOps(SelectionKey.OP_READ);
			selector.wakeup();
		}catch (IOException e){
			e.printStackTrace();
		}
	}
}
