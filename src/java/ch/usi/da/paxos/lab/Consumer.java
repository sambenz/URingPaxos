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

import java.util.concurrent.TransferQueue;

/**
 * Name: Consumer<br>
 * Description: <br>
 * 
 * Creation date: Sep 5, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Consumer implements Runnable {
	
	private final TransferQueue<Long> values;

	private long rcv_count = 0;
	
	private long avg_lat = 0;
	/**
	 * @param values
	 */
	public Consumer(TransferQueue<Long> values) {
		this.values = values;
	}

	@Override
	public void run() {
		long start_time = System.nanoTime();
		while(true){
			try {
				Long sent = values.take();
				long t = System.nanoTime();
				rcv_count++;
				avg_lat = avg_lat + (t-sent);
				if(rcv_count % 100 == 0){
					long time = t-start_time;
					System.out.println(rcv_count + " " + ((float)rcv_count/time)*1000000000 + " latency:" + (avg_lat/100));
					avg_lat = 0;
				}
			} catch (InterruptedException e) {
			}
		}
	}

}
