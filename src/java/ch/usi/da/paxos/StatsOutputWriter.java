package ch.usi.da.paxos;
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

/**
 * Name: StatsOutputWriter<br>
 * Description: <br>
 * 
 * Creation date: Aug 19, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class StatsOutputWriter implements Runnable {

	private final Proposer proposer;
	
	private final int sleep_time = 2000;
	
	private final long start_time;
	
	private long p_send = 0;
	
	/**
	 * @param proposer
	 */
	public StatsOutputWriter(Proposer proposer){
		this.proposer = proposer;
		start_time = System.currentTimeMillis();
	}
	
	@Override
	public void run() {
		while(proposer.isPerfTest()){
			try {
				Thread.sleep(sleep_time);
			} catch (InterruptedException e) {
				break;
			}
			float runtime = (float)(System.currentTimeMillis()-start_time)/1000;
			long total_s = proposer.getSendCount().get();
			long total_r = proposer.getRecvCount().get();
			long total_b = proposer.getRecvBytes().get();
			if(total_s > p_send){
				System.out.println("(s: " + total_s + " r: " + total_r + ") " + (float)total_r/runtime + " msg/s (" + (float)(total_b/runtime)/1024 + " kbytes/s) (" + (float)8*(total_b/runtime)/1024/1024 + " Mbit/s)");
			}
			p_send = total_s;
		}
	}
	
}
