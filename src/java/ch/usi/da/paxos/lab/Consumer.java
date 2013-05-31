package ch.usi.da.paxos.lab;

import java.util.concurrent.TransferQueue;

/**
 * Name: Consumer<br>
 * Description: <br>
 * 
 * Creation date: Sep 5, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
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
