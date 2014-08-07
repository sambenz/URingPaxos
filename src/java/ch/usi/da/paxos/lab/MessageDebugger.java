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
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

import ch.usi.da.paxos.Configuration;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;

/**
 * Name: MessageDebugger<br>
 * Description: <br>
 * 
 * Creation date: Apr 3, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class MessageDebugger {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(Configuration.getConfiguration().isEmpty()){
			Configuration.read(args[0]);
		}
		for(PaxosRole group: PaxosRole.values()){
			System.out.println("register " + group);
			int port = Configuration.getGroup(group).getPort();
			MulticastSocket socket = new MulticastSocket(port);
			InetAddress addr = Configuration.getGroup(group).getAddress();
			socket.joinGroup(addr);
			new Thread(new Printer(socket)).start();
		}
	}
}	
	/**
	 * Name: Printer<br>
	 * Description: <br>
	 * 
	 * Creation date: Apr 3, 2012<br>
	 * $Id$
	 * 
	 * @author benz@geoid.ch
	 */
	class Printer implements Runnable{

		private final MulticastSocket socket;
		
		/**
		 * @param socket
		 */
		public Printer(MulticastSocket socket){
			this.socket = socket;
		}
		@Override
		public void run() {
			byte[] buffer = new byte[8192];
    		while(true){
    			DatagramPacket p = new DatagramPacket(buffer,0,buffer.length);
    			try {
					socket.receive(p);
					System.out.println(Message.fromWire(p.getData()));
				} catch (IOException e) {
				}
    		}
		}
		
	}


