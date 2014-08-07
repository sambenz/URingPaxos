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
 * @author Samuel Benz benz@geoid.ch
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
		//Thread t2 = new Thread(new TCPSenderTest());
		//t2.setName("TCPSender");
	    //t2.start();	
	}
}
