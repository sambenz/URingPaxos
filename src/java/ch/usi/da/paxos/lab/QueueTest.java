package ch.usi.da.paxos.lab;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

/**
 * Name: QueueTest<br>
 * Description: <br>
 * 
 * Creation date: Sep 5, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class QueueTest {

	private final ExecutorService pool = Executors.newFixedThreadPool(2);

	private final TransferQueue<Long> values = new LinkedTransferQueue<Long>();

	/**
	 * 
	 */
	public QueueTest(){	
	}
	
	/**
	 * 
	 */
	public void start(){
		pool.execute(new Producer(values));
		pool.execute(new Consumer(values));
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		QueueTest test = new QueueTest();
		test.start();
	}

}
