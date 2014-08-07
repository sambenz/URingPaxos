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
 * Name: Producer<br>
 * Description: <br>
 * 
 * Creation date: Sep 5, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Producer implements Runnable {
	
	private final TransferQueue<Long> values;
	
	/**
	 * @param values
	 */
	public Producer(TransferQueue<Long> values) {
		this.values = values;
	}

	@Override
	public void run() {
		for(long n=0;n<10000000;n++){
			try {
				values.transfer(System.nanoTime());
			} catch (InterruptedException e) {
			}
		}
	}

}
