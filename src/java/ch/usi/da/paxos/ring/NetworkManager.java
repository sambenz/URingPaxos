package ch.usi.da.paxos.ring;
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
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;

/**
 * Name: NetworkManager<br>
 * Description: <br>
 * 
 * Creation date: Aug 14, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class NetworkManager {

	public final static int MAGIC_NUMBER = 0x756d7270; // ASCII for "umrp"
	
	private final static Logger logger = Logger.getLogger(NetworkManager.class);

	private final static Logger stats = Logger.getLogger("ch.usi.da.paxos.Stats");

	private final RingManager ring;
	
	private ServerSocketChannel server;
	
	private Selector selector;
	
	private SocketChannel client;
	
	private final TransferQueue<Message> send_queue = new LinkedTransferQueue<Message>();
	
	private Role acceptor = null;

	private Role leader = null;

	private Role learner = null;

	private Role proposer = null;

	private boolean tcp_nodelay = false;
	
	public boolean crc_32 = false;
	
	public int buf_size = 131071;

	public long recv_count = 0;

	public long recv_bytes = 0;

	public long send_count = 0;

	public long send_bytes = 0;

	public final long[] messages_distribution = new long[MessageType.values().length];

	public final long[] messages_size = new long[MessageType.values().length];

	/**
	 * @param ring the ring manager
	 * @throws IOException
	 */
	public NetworkManager(RingManager ring) throws IOException {
		this.ring = ring;
		if(stats.isDebugEnabled()){
			for(MessageType t : MessageType.values()){
				messages_distribution[t.getId()] = 0;
				messages_size[t.getId()] = 0;
			}
		}
	}
	
	/**
	 * Start the TCP listener
	 * 
	 * @throws IOException
	 */
	public void startServer() throws IOException {
		if(ring.getConfiguration().containsKey(ConfigKey.tcp_nodelay)){
			if(Integer.parseInt(ring.getConfiguration().get(ConfigKey.tcp_nodelay)) == 1){
				tcp_nodelay = true;
			}
			logger.info("NetworkManager tcp_nodelay: " + tcp_nodelay);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.tcp_crc)){
			if(Integer.parseInt(ring.getConfiguration().get(ConfigKey.tcp_crc)) == 1){
				crc_32 = true;
			}
			logger.info("NetworkManager tcp_crc: " + crc_32);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.buffer_size)){
			buf_size = Integer.parseInt(ring.getConfiguration().get(ConfigKey.buffer_size));
			logger.info("NetworkManager buf_size: " + buf_size);
		}
		selector = Selector.open();
		server = ServerSocketChannel.open();
		server.setOption(StandardSocketOptions.SO_RCVBUF,buf_size);
		server.configureBlocking(false);
		server.socket().bind(ring.getNodeAddress());
		server.register(selector, SelectionKey.OP_ACCEPT);
		
		Thread t = new Thread(new TCPListener(this,server,selector));
		t.setName("TCPListener");
		t.start();
		logger.debug("NetworkManager listener started " + server.socket().getLocalSocketAddress() + " (buffer size: " + server.socket().getReceiveBufferSize() + ")");
		
		Thread t2 = new Thread(new NetworkStatsWriter(ring));
		t2.setName("NetworkStatsWriter");
		t2.start();
	}
	
	/**
	 * Called from the server listener when a packet arrives
	 * 
	 * @param m the received message
	 */
	public void receive(Message m){
		/*if(logger.isDebugEnabled()){
			logger.debug("receive network message (ring:" + ring.getRingID() + ") : " + m);
		}*/
		if(stats.isDebugEnabled()){
			messages_distribution[m.getType().getId()]++;
			messages_size[m.getType().getId()] = messages_size[m.getType().getId()] + Message.length(m);
		}
		
		// network forwarding
		if(m.getType() == MessageType.Value){
			if(ring.getRingSuccessor(ring.getNodeID()) != m.getSender()){ // D,v -> until predecessor(P0)
				send(m);
			}
		}else if(m.getType() == MessageType.Phase2){
			if(acceptor == null && ring.getNodeID() != ring.getLastAcceptor()){ // network -> until last_accept
				send(m);
			}	
		}else if(m.getType() == MessageType.Decision){
			// network -> predecessor(last_accept)
			if(ring.getNodeID() != ring.getRingPredecessor(ring.getLastAcceptor())){
				send(m);
			}
		}else if(m.getType() == MessageType.Phase1 || m.getType() == MessageType.Phase1Range){
			if(m.getReceiver() == PaxosRole.Leader){
				if(leader == null){
					send(m);
				}
			}else if(m.getReceiver() == PaxosRole.Acceptor){
				if(acceptor == null){
					send(m);
				}
			}
		}else if(m.getType() == MessageType.Safe){
			if(learner == null && ring.getNodeID() != ring.getCoordinatorID()){ // network -> until coordinator
				send(m);
			}	
		}else if(m.getType() == MessageType.Trim){
			if(acceptor == null && ring.getNodeID() != ring.getCoordinatorID()){ // network -> until coordinator
				send(m);
			}				
		}

		// local delivery
		if(m.getType() == MessageType.Value){
			if(learner != null){
				learner.deliver(ring,m);
			}
			if(leader != null){
				leader.deliver(ring,m);
			}
			if(acceptor != null){
				acceptor.deliver(ring,m);
			}
		}else if(m.getType() == MessageType.Phase2){
			if(learner != null){
				learner.deliver(ring,m);
			}			
			if(acceptor != null){
				acceptor.deliver(ring,m);
			}	
		}else if(m.getType() == MessageType.Decision){
			if(leader != null){
				leader.deliver(ring,m);
			}
			if(acceptor != null){
				acceptor.deliver(ring,m);
			}
			if(learner != null){
				learner.deliver(ring,m);
			}			
			if(proposer != null){
				proposer.deliver(ring,m);
			}
		}else if(m.getType() == MessageType.Phase1 || m.getType() == MessageType.Phase1Range){
			if(m.getReceiver() == PaxosRole.Leader){
				if(leader != null){
					leader.deliver(ring,m);
				}
			}else if(m.getReceiver() == PaxosRole.Acceptor){
				if(acceptor != null){
					acceptor.deliver(ring,m);
				}
			}
		}else if(m.getType() == MessageType.Safe){
			if(leader != null){
				leader.deliver(ring,m);
			}else if(learner != null){
				learner.deliver(ring,m);
			}
		}else if(m.getType() == MessageType.Trim){
			if(learner != null){
				learner.deliver(ring,m);
			}
			if(leader != null){
				leader.deliver(ring,m);
			}else if(acceptor != null){
				acceptor.deliver(ring,m);
			}
		}
	}
	
	/**
	 * close the server listener
	 */
	public void closeServer(){
		try {
			selector.close();
			server.close();
		} catch (IOException e) {
			logger.error("NetworkManager server close error",e);
		}
	}
		
	/**
	 * connect to the ring successor
	 * 
	 * @param addr
	 */
	public void connectClient(InetSocketAddress addr){
		try {
			Thread.sleep(1000); // give node time to start (zookeeper is fast!)
		} catch (InterruptedException e) {
		}
		try {
			client = SocketChannel.open();
			client.setOption(StandardSocketOptions.SO_SNDBUF,buf_size);
			client.setOption(StandardSocketOptions.SO_RCVBUF,buf_size);			
			client.socket().setSendBufferSize(buf_size);
			client.configureBlocking(true); // Client runs in Blocking Mode !!!
			client.connect(addr);
			client.setOption(StandardSocketOptions.TCP_NODELAY,tcp_nodelay);
			Thread t = new Thread(new TCPSender(this,client,send_queue));
			t.setName("TCPSender");
			t.start();
			logger.debug("NetworkManager create connection " + addr + " (" + client.getLocalAddress() + ")");
		} catch (IOException e) {
			logger.error("NetworkManager client connect error",e);
		}
	}

	/**
	 * disconnect client (ring successor)
	 */
	public void disconnectClient(){
		try {
			if(client != null){
				client.close();
				logger.debug("NetworkManager close connection");
			}
		} catch (IOException e) {
			logger.error("NetworkManager client close error",e);
		}
	}	
	
	/**
	 * @param m the message to send
	 */
	public void send(Message m){
		try {
			send_queue.transfer(m); // (blocking call)
		} catch (InterruptedException e) {
		}
	}
	
	/**
	 * @return the acceptor
	 */
	public Role getAcceptor() {
		return acceptor;
	}

	/**
	 * @param acceptor the acceptor to set
	 */
	public void setAcceptor(Role acceptor) {
		this.acceptor = acceptor;
	}

	/**
	 * @return the leader
	 */
	public Role getLeader() {
		return leader;
	}

	/**
	 * @param leader the leader to set
	 */
	public void setLeader(Role leader) {
		this.leader = leader;
	}

	/**
	 * @return the learner
	 */
	public Role getLearner() {
		return learner;
	}

	/**
	 * @param learner the learner to set
	 */
	public void setLearner(Role learner) {
		this.learner = learner;
	}

	/**
	 * @return the proposer
	 */
	public Role getProposer() {
		return proposer;
	}

	/**
	 * @param proposer the proposer to set
	 */
	public void setProposer(Role proposer) {
		this.proposer = proposer;
	}

	/**
	 * @param role
	 */
	public synchronized void registerCallback(Role role){
		if(role instanceof AcceptorRole){
			this.acceptor = role;			
		}else if(role instanceof CoordinatorRole){
			this.leader = role;
		}else if(role instanceof LearnerRole){
			this.learner = role;
		}else if(role instanceof ProposerRole){
			this.proposer = role;
		}
	}
	
	/**
	 * @param value
	 * @return a byte[]
	 */
	public static synchronized final byte[] intToByte(int value) {
	    return new byte[] {
	            (byte)(value >>> 24),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
	
	/**
	 * @param b
	 * @return the int
	 */
	public static synchronized final int byteToInt(byte [] b) { 
		return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF); 
	}
}
