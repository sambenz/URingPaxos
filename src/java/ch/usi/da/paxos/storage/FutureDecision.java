package ch.usi.da.paxos.storage;
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Name: FutureDecision<br>
 * Description: <br>
 * 
 * See Java Concurrency in Practice p. 187
 *  
 * Creation date: Mar 20, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class FutureDecision {

	private Decision decision = null;
	
	private final CountDownLatch done = new CountDownLatch(1);
	
	public FutureDecision(){
		
	}
	
	public boolean isDecided(){
		return (done.getCount() == 0);
	}
	
	public synchronized void setDecision(Decision decision){
		if(!isDecided()){
			this.decision = decision;
			done.countDown();
		}
	}
	
	public Decision getDecision() throws InterruptedException {
		done.await();
		synchronized (this) {
			return decision;
		}
	}
	
	public Decision getDecision(int timeout) throws InterruptedException {
		done.await(timeout,TimeUnit.MILLISECONDS);
		synchronized (this) {
			return decision;
		}
	}

}
