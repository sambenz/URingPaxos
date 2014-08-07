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
import java.net.InetAddress;
import java.net.SocketException;

import ch.usi.da.smr.message.Message;

/**
 * Name: UDPSender<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class UDPSender {

	private final DatagramSocket socket;
	
	public UDPSender() throws SocketException{
		socket = new DatagramSocket();
	}
	
	public void send(Message m){
		try {
			String[] sender = m.getTo().split(";");
			InetAddress address = InetAddress.getByName(sender[0]);
			int port = Integer.parseInt(sender[1]);
			byte[] buffer = Message.toByteArray(m);
			DatagramPacket packet = new DatagramPacket(buffer,0,buffer.length,address,port);
			socket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
