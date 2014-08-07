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

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.MessageType;

/**
 * Name: NetworkStatsWriter<br>
 * Description: <br>
 * 
 * Creation date: Mar 07, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class NetworkStatsWriter implements Runnable {

	private final static Logger logger = Logger.getLogger("ch.usi.da.paxos.Stats");
	
	private final NetworkManager network;
	
	private final int ringID;
	
	private long start_time = -1;
	
	private long last_time = System.nanoTime();
	
	private long last_recv_count = 0;

	private long last_recv_bytes = 0;

	private long last_send_count = 0;

	private long last_send_bytes = 0;

		
	public NetworkStatsWriter(RingManager ring) {
		ringID = ring.getRingID();
		this.network = ring.getNetwork();
	}
	
	@Override
	public void run() {
		boolean run = logger.isInfoEnabled();
		while(run){
			if(start_time < 0 && network.send_count > 5){
				start_time = System.nanoTime();
				logger.debug("NetworkStatsWriter set start time.");
			}
			try {
				long time = System.nanoTime();
				long recv_count = network.recv_count - last_recv_count;
				long recv_bytes = network.recv_bytes - last_recv_bytes;
				long send_count = network.send_count - last_send_count;
				long send_bytes = network.send_bytes - last_send_bytes;

				float t = (float)(time-last_time)/(1000*1000*1000);
				// float revc_bwk = (float)(recv_bytes/t)/1024; // kbyte/s
				// float send_bwk = (float)(send_bytes/t)/1024;
				float recv_bwm = (float)8*(recv_bytes/t)/1024/1024; // Mbit/s
				float send_bwm = (float)8*(send_bytes/t)/1024/1024;
				
				float t2 = (float)(time-start_time)/(1000*1000*1000);
				float recv_bw = (float)8*(network.recv_bytes/t2)/1024/1024; // Mbit/s
				float send_bw = (float)8*(network.send_bytes/t2)/1024/1024;
				
				logger.info(String.format("TCP %d in/out %.1f/%.1f msg/s %.2f/%.2f Mbit/s (avg: %.2f/%.2f Mbit/s)",ringID,(float)recv_count/t,(float)send_count/t,recv_bwm,send_bwm,recv_bw,send_bw));
				
				if(logger.isDebugEnabled()){
					for(MessageType m : MessageType.values()){
						if(network.messages_distribution[m.getId()] > 0){
							logger.debug("NetworkManager message " + m + ": " + network.messages_distribution[m.getId()] + " (" + network.messages_size[m.getId()] + " bytes)");
						}
					}
				}
				
				last_recv_count += recv_count;
				last_recv_bytes += recv_bytes;
				last_send_count += send_count;
				last_send_bytes += send_bytes;
				last_time = time;
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;				
			}
		}
	}

}
