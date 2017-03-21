package ch.usi.da.dmap.server;
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
 * Name: FutureResponse<br>
 * Description: <br>
 * 
 * See Java Concurrency in Practice p. 187
 *  
 * Creation date: Mar 03, 2017<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class FutureResponse {
	
	private Object repsonse = null;
	
	private final CountDownLatch done = new CountDownLatch(1);
	
	public FutureResponse(){
		
	}
	
	public boolean isDecided(){
		return (done.getCount() == 0);
	}
	
	public synchronized void setResponse(Object repsonse){
		if(!isDecided()){
			this.repsonse = repsonse;
			done.countDown();
		}
	}
	
	public Object getResponse() throws InterruptedException {
		done.await();
		synchronized (this) {
			return repsonse;
		}
	}
	
	public Object getResponse(int timeout) throws InterruptedException {
		done.await(timeout,TimeUnit.MILLISECONDS);
		synchronized (this) {
			return repsonse;
		}
	}

}
