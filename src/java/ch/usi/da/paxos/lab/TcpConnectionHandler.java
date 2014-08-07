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
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.ring.NetworkManager;

/**
 * Name: TcpConnectionHandler<br>
 * Description: <br>
 * 
 * Creation date: Sep 12, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class TcpConnectionHandler implements Runnable {

	private final Socket socket;
	
	private long total_r = 0;
	
	private long total_b = 0;
	
	private long start_time;
	
	List<Long> t = new ArrayList<Long>();

	/**
	 * @param socket
	 */
	public TcpConnectionHandler(Socket socket) {
		this.socket = socket;
	}

	@Override
	public void run() {
		start_time = System.nanoTime();
		try{
			InputStream in = socket.getInputStream();
			byte[] buffer = new byte[65540];
			int count;
			ByteBuffer l = ByteBuffer.allocate(8); // prefix-length buffer
			ByteBuffer m = null; // message buffer
			while((count = in.read(buffer)) >= 0){
				for(int i=0;i<count;i++){
					byte b = buffer[i];
					if(l.hasRemaining()){
						l.put(b);
						if(!l.hasRemaining()){
							int magic = byteToInt(Arrays.copyOfRange(l.array(),0,4));
							if(magic  == NetworkManager.MAGIC_NUMBER){
								m = ByteBuffer.allocate(byteToInt(Arrays.copyOfRange(l.array(),4,8)));
							}else{
								l.flip(); // re-sync to next magic nr.
								l.getInt();
								l.compact();
							}
						}
					}else if(m != null){
						m.put(b);
						if(!m.hasRemaining()){
							Message message = Message.fromWire(m.array());
							long latency = System.nanoTime()-Long.parseLong(message.getValue().getID());
							t.add(latency);
							total_b = total_b+m.capacity();
							total_r++;
							if(total_r % 20000 == 0){
								float runtime = (float)(System.nanoTime()-start_time)/(1000*1000*1000);
								System.out.println("r: " + total_r + " " + (float)total_r/runtime + " msg/s (" + (float)(total_b/runtime)/1024 + " kbytes/s) (" + (float)8*(total_b/runtime)/1024/1024 + " Mbit/s)");
								print();
							}
							m = null;
							l.clear();
						}
					}
				}
			}
		}catch (IOException e){
			e.printStackTrace();
		}finally{
			try { socket.close(); } catch (IOException e) { }
		}
		
	}

	/**
	 * print stats
	 */
	public void print(){
		long sum = 0;
		for(Long l : t){
			sum = sum + l;
		}
		System.err.println("avg:" + (float)(sum/t.size()));
	}
	
	/**
	 * @param b
	 * @return the int
	 */
	public static final int byteToInt(byte [] b) { 
		return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF); 
	}

}
