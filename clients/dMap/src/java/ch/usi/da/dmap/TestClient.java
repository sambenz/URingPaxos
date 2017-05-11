package ch.usi.da.dmap;
/* 
 * Copyright (c) 2017 Università della Svizzera italiana (USI)
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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/**
 * Name: TestClient<br>
 * Description: <br>
 * 
 * Creation date: Jan 28, 2017<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class TestClient {
	static {
		// get hostname and pid for log file name
		String host = "localhost";
		try {
			Process proc = Runtime.getRuntime().exec("hostname");
			BufferedInputStream in = new BufferedInputStream(proc.getInputStream());
			proc.waitFor();
			byte [] b = new byte[in.available()];
			in.read(b);
			in.close();
			host = new String(b).replace("\n","");
		} catch (IOException | InterruptedException e) {
		}
		int pid = 0;
		try {
			pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
		} catch (NumberFormatException | IOException e) {
		}
		System.setProperty("logfilename", "L" + host + "-" + pid + ".log");
	}
	
	private final static Logger logger = Logger.getLogger(TestClient.class);

	private final static Logger stats_logger = Logger.getLogger("ch.usi.da.paxos.Stats");
	
	private final String mapID;
	
	private final String zookeeper;
	
	private final List<Long> latency = Collections.synchronizedList(new ArrayList<Long>());
	
	private final AtomicLong stat_latency = new AtomicLong();
	
	private final AtomicLong stat_command = new AtomicLong();
	
	final Random rnd = new Random();
	
	public TestClient(String mapID, String zookeeper){
		this.mapID = mapID;
		this.zookeeper = zookeeper;
	}

	public void load(final int key_count) {
		SortedMap<Integer, String> dmap = new DistributedOrderedMap<Integer, String>(mapID,zookeeper);
		int send_count = 0;
		while(send_count < key_count){
			dmap.put(send_count++,"value of " + send_count); //size on AB: ~380 bytes
			logger.info(send_count);
		}
	}

	public void startIterator(final int concurrent_iterator) throws InterruptedException {
		latency.clear();
		final CountDownLatch await = new CountDownLatch(concurrent_iterator);
		
		final Thread stats = new Thread("ClientStatsWriter"){		    			
			private long last_time = System.nanoTime();
			private long last_sent_count = 0;
			private long last_sent_time = 0;
			@Override
			public void run() {
				while(await.getCount() > 0){
					try {
						long time = System.nanoTime();
						long sent_count = stat_command.get() - last_sent_count;
						long sent_time = stat_latency.get() - last_sent_time;
						float t = (float)(time-last_time)/(1000*1000*1000);
						float count = sent_count/t;
						logger.info(String.format("Client sent %.1f command/s avg. latency %.0f ns",count,sent_time/count));
						last_sent_count += sent_count;
						last_sent_time += sent_time;
						last_time = time;
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;				
					}
				}
			}
		};
		stats.start();
		
		for(int i=0;i<concurrent_iterator;i++){
			Thread t = new Thread("Command Sender " + i){
				@Override
				public void run(){
					SortedMap<Integer, String> dmap = new DistributedOrderedMap<Integer, String>(mapID,zookeeper);
					long time = System.nanoTime();
					for(Entry<Integer, String> e : dmap.entrySet()){
						if(logger.isDebugEnabled()){
							logger.debug(e);
						}
						long ntime = System.nanoTime();
						long lat = ntime - time;
						time = ntime;
						if(stats_logger.isDebugEnabled()){
							latency.add(lat);
						}
						stat_latency.addAndGet(lat);
						stat_command.incrementAndGet();							
					}
					await.countDown();
				}
			};
			t.start();
		}
		
		await.await(); // wait until finished
		Thread.sleep(5000);
		printHistogram();
	}

	public void start(final int concurrent_cmd, final int send_per_thread,final int key_count) throws InterruptedException {
		latency.clear();
		final CountDownLatch await = new CountDownLatch(concurrent_cmd);
		
		final Thread stats = new Thread("ClientStatsWriter"){		    			
			private long last_time = System.nanoTime();
			private long last_sent_count = 0;
			private long last_sent_time = 0;
			@Override
			public void run() {
				while(await.getCount() > 0){
					try {
						long time = System.nanoTime();
						long sent_count = stat_command.get() - last_sent_count;
						long sent_time = stat_latency.get() - last_sent_time;
						float t = (float)(time-last_time)/(1000*1000*1000);
						float count = sent_count/t;
						logger.info(String.format("Client sent %.1f command/s avg. latency %.0f ns",count,sent_time/count));
						last_sent_count += sent_count;
						last_sent_time += sent_time;
						last_time = time;
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;				
					}
				}
			}
		};
		stats.start();
		
		for(int i=0;i<concurrent_cmd;i++){
			Thread t = new Thread("Command Sender " + i){
				@Override
				public void run(){
					int send_count = 0;
					SortedMap<Integer, String> dmap = new DistributedOrderedMap<Integer, String>(mapID,zookeeper);
					while(send_count < send_per_thread){
						try{
							long time = System.nanoTime();
							int k = rnd.nextInt(key_count);
							dmap.put(k,"value of " + k); //size on AB: ~380 bytes
							long lat = System.nanoTime() - time;
							if(stats_logger.isDebugEnabled()){
								latency.add(lat);
							}
							stat_latency.addAndGet(lat);
							stat_command.incrementAndGet();							
						} catch (Exception e){
							logger.error("Error in send thread!",e);
						}
						send_count++;
					}
					await.countDown();
				}
			};
			t.start();
		}
		
		await.await(); // wait until finished
		Thread.sleep(5000);
		printHistogram();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		String zoo = "127.0.0.1:2181";
		if(args.length > 0){
			zoo = args[0];
		}
		
		TestClient t = new TestClient("83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b",zoo);
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Press a key to start ...");
		String s = in.readLine();
		int concurrent_cmd = 10; // # of threads
		final int send_per_thread = 100000;
		final int key_count = 60000; // n * 380 byte memory needed at replica
		if(!s.isEmpty()){
			if(s.contains("load")){
				t.load(key_count);
				Thread.sleep(3000);
				System.exit(0);
			}else{
				concurrent_cmd = Integer.parseInt(s);
			}
		}
		stats_logger.info("Start performance testing with " + concurrent_cmd + " threads.");
		//t.startIterator(concurrent_cmd);
		t.start(concurrent_cmd,send_per_thread,key_count);
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
		for(Entry<Long, Long> bin : histogram.entrySet()){ // details for CDF
			stats_logger.debug(bin.getKey() + "," + bin.getValue());
		}
	}
}
