package ch.usi.da.paxos.lab;
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
 * @author Samuel Benz <benz@geoid.ch>
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
