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
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.ring.NetworkManager;

/**
 * Name: SCTPListener<br>
 * Description: <br>
 * 
 * Creation date: Aug 15, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class TCPSenderTest implements Runnable {

	private final static Logger logger = Logger.getLogger(TCPSenderTest.class);
	
	private final Socket socket;
		
	/**
	 * @throws IOException 
	 */
	public TCPSenderTest(String ip) throws IOException{
		socket = new Socket(ip, 2020);
		socket.setTcpNoDelay(true);
	}
	
	@Override
	public void run() {
		int send = 0;
		try {
			OutputStream out = socket.getOutputStream();
			while(send < 500000){
				Long t = System.nanoTime();
				Message m = new Message(0,0,PaxosRole.Leader,MessageType.Value,0,0,new Value(t.toString(),new byte[8912]));
				byte[] b = Message.toWire(m);
				
//				out.write(intToByte(0x00)); // insert something useless (simulates an error)
//				out.write(intToByte(0x39));
//				out.write(intToByte(0xf0));
//				out.write(intToByte(0xab));
//				out.write(intToByte(0x4c));
//				out.write(intToByte(0x61));
//				out.write(intToByte(0x88));
				
				out.write(intToByte(NetworkManager.MAGIC_NUMBER)); // "umrp" magic header
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
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException{
		Thread t = new Thread(new TCPSenderTest(args[0]));
		t.setName("TCPSender");
	    t.start();
		//Thread t2 = new Thread(new TCPSenderTest());
		//t2.setName("TCPSender");
	    //t2.start();	
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
