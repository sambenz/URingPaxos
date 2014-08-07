package ch.usi.da.smr.transport;
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
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Arrays;

import ch.usi.da.smr.message.Message;

/**
 * Name: UDPListener<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class UDPListener implements Runnable {

	private final DatagramSocket socket;
	
	private Receiver receiver = null;
	
	public UDPListener(int port) throws SocketException{
		socket = new DatagramSocket(port);
	}

	@Override
	public void run() {
		while(!socket.isClosed()){
			try {
				byte[] buffer = new byte[65535];
				DatagramPacket packet = new DatagramPacket(buffer,buffer.length);
				socket.receive(packet);
				if(receiver != null){
					Message m = Message.fromByteArray(Arrays.copyOfRange(packet.getData(),0,packet.getLength()));
					receiver.receive(m);
				}
			} catch (SocketException e) {
				//e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void registerReceiver(Receiver receiver){
		this.receiver = receiver; 
	}
	
	public void close(){
		socket.close();
	}
}
