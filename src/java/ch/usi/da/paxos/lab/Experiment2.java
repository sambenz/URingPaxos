package ch.usi.da.paxos.lab;
/* 
 * Copyright (c) 2015 Università della Svizzera italiana (USI)
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.ConfigKey;
import ch.usi.da.paxos.api.PaxosNode;
import ch.usi.da.paxos.message.Control;
import ch.usi.da.paxos.message.ControlType;
import ch.usi.da.paxos.ring.RingManager;
import ch.usi.da.paxos.storage.FutureDecision;

/**
 * Name: Experiment2<br>
 * Description: <br>
 * 
 * Adding load with additional rings.
 * 
 * Creation date: Oct 1, 2015<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Experiment2 implements Runnable {

	private final PaxosNode paxos;

	private final static Logger logger = Logger.getLogger(Experiment2.class);

	private final static Logger stats_logger = Logger.getLogger("ch.usi.da.paxos.Stats");
	
	private final List<Long> latency = Collections.synchronizedList(new ArrayList<Long>());

	private int concurrent_values = 5;
	
	private int value_size = 32768;
		
	private int value_count = 9000000;
	
	private volatile boolean send1 = true;

	private volatile boolean send2 = true;

	private volatile boolean send3 = true;

	private volatile boolean send4 = true;

	public Experiment2(PaxosNode paxos) {
		this.paxos = paxos;
		
		if(paxos.getRings().size() < 2){
			logger.error("Experiment needs proposeres in two rings!");
			throw(new RuntimeException());
		}
		
		/*RingManager ring = paxos.getRings().get(0).getRingManager();
		if(ring.getConfiguration().containsKey(ConfigKey.concurrent_values)){
			concurrent_values = Integer.parseInt(ring.getConfiguration().get(ConfigKey.concurrent_values));
			logger.info("Experiment concurrent_values: " + concurrent_values);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.value_size)){
			value_size = Integer.parseInt(ring.getConfiguration().get(ConfigKey.value_size));
			logger.info("Experiment value_size: " + value_size);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.value_count)){
			value_count = Integer.parseInt(ring.getConfiguration().get(ConfigKey.value_count));
			logger.info("Experiment value_count: " + value_count);
		}*/

	}

	@Override
	public void run() {
		final int send_per_thread = (int) value_count / concurrent_values;
		final AtomicLong stat_latency = new AtomicLong();
		final AtomicLong stat_command = new AtomicLong();		    		
		latency.clear();
		final Thread stats = new Thread("ExperimentStatsWriter"){		    			
			private long last_time = System.nanoTime();
			private long last_sent_count = 0;
			private long last_sent_time = 0;
			@Override
			public void run() {
				while(true){
					try {
						long time = System.nanoTime();
						long sent_count = stat_command.get() - last_sent_count;
						long sent_time = stat_latency.get() - last_sent_time;
						float t = (float)(time-last_time)/(1000*1000*1000);
						float count = sent_count/t;
						stats_logger.info(String.format("Client sent %.1f command/s avg. latency %.0f ns",count,sent_time/count));
						last_sent_count += sent_count;
						last_sent_time += sent_time;
						last_time = time;
						Thread.sleep(100);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;				
					}
				}
			}
		};
		stats.start();
		
		final Thread coord = new Thread("ExperimentCoordinator"){		    			
			@Override
			public void run() {
				try {

					// curl -XPOST `heat output-show mdrp scale_up_url|tr -d '"\n'`
					/*Process p = Runtime.getRuntime().exec("heat output-show mdrp scale_up_url");
				    p.waitFor();
				    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
				    String up_url = reader.readLine().replace("\"","");
				    logger.info("Heat scale up_url " + up_url);
				    
				    p = Runtime.getRuntime().exec("heat output-show mdrp scale_dn_url");
				    p.waitFor();
				    reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
				    String dn_url = reader.readLine().replace("\"","");
				    logger.info("Heat scale dn_url " + dn_url);
		    
				    // upscale
				    p = Runtime.getRuntime().exec("curl -XPOST " + up_url);
				    p.waitFor();
				    reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
				    String line = "";			
				    while ((line = reader.readLine())!= null) {
				    	logger.info(line);
				    }
				    reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
				    line = "";			
				    while ((line = reader.readLine())!= null) {
				    	logger.warn(line);
				    }
				    
					Thread.sleep(60000);*/
					
					Thread.sleep(15000);

					Control c = new Control(1,ControlType.Subscribe,1,2);
					paxos.getProposer(2).control(c);
					paxos.getProposer(1).control(c);

					for(int i=0;i<concurrent_values;i++){
						Thread t = new Thread("Command Sender 2 " + i){
							@Override
							public void run(){
								int send_count = 0;
								while(send2 && send_count < send_per_thread){
									FutureDecision f = null;
									try{
										long time = System.nanoTime();
										if((f = paxos.getProposer(2).propose(new byte[value_size])) != null){
											f.getDecision(1000); // wait response
											long lat = System.nanoTime() - time;
											stat_latency.addAndGet(lat);
											stat_command.incrementAndGet();
											latency.add(lat);
										}
									} catch (Exception e){
										logger.error("Error in send thread!",e);
									}
									send_count++;
								}
								logger.debug("Thread terminated.");
							}
						};
						t.start();
					}
					
					Thread.sleep(15000);
					

					c = new Control(2,ControlType.Subscribe,1,3);
					paxos.getProposer(3).control(c);
					paxos.getProposer(1).control(c);

					for(int i=0;i<concurrent_values;i++){
						Thread t = new Thread("Command Sender 3 " + i){
							@Override
							public void run(){
								int send_count = 0;
								while(send3 && send_count < send_per_thread){
									FutureDecision f = null;
									try{
										long time = System.nanoTime();
										if((f = paxos.getProposer(3).propose(new byte[value_size])) != null){
											f.getDecision(1000); // wait response
											long lat = System.nanoTime() - time;
											stat_latency.addAndGet(lat);
											stat_command.incrementAndGet();
											latency.add(lat);
										}
									} catch (Exception e){
										logger.error("Error in send thread!",e);
									}
									send_count++;
								}
								logger.debug("Thread terminated.");
							}
						};
						t.start();
					}
					
					Thread.sleep(15000);

					c = new Control(3,ControlType.Subscribe,1,4);
					paxos.getProposer(4).control(c);
					paxos.getProposer(1).control(c);

					for(int i=0;i<concurrent_values;i++){
						Thread t = new Thread("Command Sender 4 " + i){
							@Override
							public void run(){
								int send_count = 0;
								while(send4 && send_count < send_per_thread){
									FutureDecision f = null;
									try{
										long time = System.nanoTime();
										if((f = paxos.getProposer(4).propose(new byte[value_size])) != null){
											f.getDecision(1000); // wait response
											long lat = System.nanoTime() - time;
											stat_latency.addAndGet(lat);
											stat_command.incrementAndGet();
											latency.add(lat);
										}
									} catch (Exception e){
										logger.error("Error in send thread!",e);
									}
									send_count++;
								}
								logger.debug("Thread terminated.");
							}
						};
						t.start();
					}
					
					Thread.sleep(15000);

					send1 = false;
					send2 = false;
					send3 = false;
					send4 = false;
					
					Thread.sleep(5000);
					printHistogram();
					logger.info("Finished experiment!");
					
				} catch (Exception e) {
					e.printStackTrace();
				}				
			}
		};
		coord.start();
		
		for(int i=0;i<concurrent_values;i++){
			Thread t = new Thread("Command Sender 1 " + i){
				@Override
				public void run(){
					int send_count = 0;
					while(send1 && send_count < send_per_thread){
						FutureDecision f = null;
						try{
							long time = System.nanoTime();
							if((f = paxos.getProposer(1).propose(new byte[value_size])) != null){
								f.getDecision(1000); // wait response
								long lat = System.nanoTime() - time;
								stat_latency.addAndGet(lat);
								stat_command.incrementAndGet();
								latency.add(lat);
							}
						} catch (Exception e){
							logger.error("Error in send thread!",e);
						}
						send_count++;
					}
					logger.debug("Thread terminated.");
				}
			};
			t.start();
		}

	}
	
	private void printHistogram(){
		Map<Long,Long> histogram = new HashMap<Long,Long>();
		int a = 0,b = 0,b2 = 0,c = 0,d = 0,e = 0,f = 0;
		long sum = 0;
		for(Long l : latency){
			sum = sum + l;
			if(l < 1000000){ // <1ms
				a++;
			}else if (l < 10000000){ // <10ms
				b++;
			}else if (l < 25000000){ // <25ms
				b2++;
			}else if (l < 50000000){ // <50ms
				c++;
			}else if (l < 75000000){ // <75ms
				f++;
			}else if (l < 100000000){ // <100ms
				d++;
			}else{
				e++;
			}
			Long key = new Long(Math.round(l/1000));
			if(histogram.containsKey(key)){
				histogram.put(key,histogram.get(key)+1);
			}else{
				histogram.put(key,1L);
			}
		}
		float avg = (float)sum/latency.size()/1000/1000;
		stats_logger.info("client latency histogram: <1ms:" + a + " <10ms:" + b + " <25ms:" + b2 + " <50ms:" + c + " <75ms:" + f + " <100ms:" + d + " >100ms:" + e + " avg:" + avg);
		if(stats_logger.isDebugEnabled()){
			for(Entry<Long, Long> bin : histogram.entrySet()){ // details for CDF
				stats_logger.debug(bin.getKey() + "," + bin.getValue());
			}
		}
	}

}
