package ch.usi.da.paxos.storage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Name: FutureDecision<br>
 * Description: <br>
 * 
 * See Concurrency in Practice p. 187
 *  
 * Creation date: Mar 20, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
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
