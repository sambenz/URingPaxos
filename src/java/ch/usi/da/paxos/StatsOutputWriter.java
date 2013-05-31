package ch.usi.da.paxos;

/**
 * Name: StatsOutputWriter<br>
 * Description: <br>
 * 
 * Creation date: Aug 19, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
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
