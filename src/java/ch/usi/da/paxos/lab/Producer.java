package ch.usi.da.paxos.lab;

import java.util.concurrent.TransferQueue;

/**
 * Name: Producer<br>
 * Description: <br>
 * 
 * Creation date: Sep 5, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
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
