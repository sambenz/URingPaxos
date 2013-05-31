package ch.usi.da.paxos.ring;

import org.apache.log4j.Logger;

/**
 * Name: LearnerStatsWriter<br>
 * Description: <br>
 * 
 * Creation date: Mar 09, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class LearnerStatsWriter implements Runnable {

	private final static Logger logger = Logger.getLogger("ch.usi.da.paxos.Stats");
	
	private long start_time = -1;
	
	private long last_time = System.nanoTime();

	private final LearnerRole learner;
		
	private final int ringID;
		
	private long last_deliver_count = 0;

	private long last_deliver_bytes = 0;
			
	public LearnerStatsWriter(RingManager ring, LearnerRole learner) {
		this.learner = learner;
		ringID = ring.getRingID();
	}
	
	@Override
	public void run() {
		boolean run = logger.isInfoEnabled();
		while(run){
			if(start_time < 0 && learner.deliver_count > 0){
				start_time = System.nanoTime();
				logger.debug("LearnerStatsWriter set start time.");
			}
			try {
				long time = System.nanoTime();
				long deliver_count = learner.deliver_count - last_deliver_count;
				long deliver_bytes = learner.deliver_bytes - last_deliver_bytes;

				float t = (float)(time-last_time)/(1000*1000*1000);
				float deliver_bwm = (float)8*(deliver_bytes/t)/1024/1024; // Mbit/s
				
				float t2 = (float)(time-start_time)/(1000*1000*1000);
				float deliver_bw = (float)8*(learner.deliver_bytes/t2)/1024/1024; // Mbit/s
				
				logger.info(String.format("Learner %d delivered %.1f values/s %.2f Mbit/s (avg: %.2f Mbit/s) (wait: %d) (count: %d)",ringID,deliver_count/t,deliver_bwm,deliver_bw,learner.getValues().size(),learner.deliver_count));
				
				last_deliver_count += deliver_count;
				last_deliver_bytes += deliver_bytes;
				last_time = time;
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;				
			}
		}
	}

}
