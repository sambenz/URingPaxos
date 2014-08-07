package ch.usi.da.paxos.ring;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.BatchPolicy;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.api.Proposer;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.storage.FutureDecision;
import ch.usi.da.paxos.storage.Proposal;

/**
 * Name: ProposerRole<br>
 * Description: <br>
 * 
 * Creation date: Aug 12, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ProposerRole extends Role implements Proposer {
	
	private final static Logger logger = Logger.getLogger(ProposerRole.class);

	private final static Logger stats = Logger.getLogger("ch.usi.da.paxos.Stats");

	private final static Logger proposallogger = Logger.getLogger("ch.usi.da.paxos.storage.Proposal");

	private final RingManager ring;
	
	private final RandomDataGenerator random = new RandomDataGenerator();;

	private int concurrent_values = 20;
	
	private ValueType value_type = ValueType.FIX;
	
	private int value_size = 8912;
		
	private int value_count = 900000;
	
	private final Map<String,Proposal> proposals = new ConcurrentHashMap<String,Proposal>();

	private final Map<String,FutureDecision> futures = new ConcurrentHashMap<String,FutureDecision>();
	
	private BatchPolicy batcher;
	
	private final BlockingQueue<Message> send_queue = new LinkedBlockingQueue<Message>();
	
	private long send_count = 0;
	
	private boolean test = false;
	
	private final List<Long> latency = new ArrayList<Long>();

	/**
	 * @param ring 
	 */
	public ProposerRole(RingManager ring) {
		this.ring = ring;

		if(ring.getConfiguration().containsKey(ConfigKey.concurrent_values)){
			concurrent_values = Integer.parseInt(ring.getConfiguration().get(ConfigKey.concurrent_values));
			logger.info("Proposer concurrent_values: " + concurrent_values);
		}
		String batcher_class = "none";
		if(ring.getConfiguration().containsKey(ConfigKey.batch_policy)){
			batcher_class = ring.getConfiguration().get(ConfigKey.batch_policy);
		}
		try {
			if(!batcher_class.equals("none")){
				Class<?> policy = Class.forName(batcher_class);
				batcher = (BatchPolicy) policy.newInstance();
			}else{
				batcher = null;
			}
			logger.info("Proposer batch policy: " + batcher);
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			batcher = null;
			logger.error("Could not initilaize the batch policy!", e);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.value_size)){
			String v = ring.getConfiguration().get(ConfigKey.value_size);
			if(v.toLowerCase().startsWith("int")){
				value_type = ValueType.INTVALUE;
			}else if(v.toLowerCase().startsWith("uni")){
				value_type = ValueType.UNIFORM;
			}else if(v.toLowerCase().startsWith("nor")){
				value_type = ValueType.NORMAL;
			}else if(v.toLowerCase().startsWith("exp")){
				value_type = ValueType.EXPONENTIAL;
			}else if(v.toLowerCase().startsWith("zip")){
				value_type = ValueType.ZIPF;
			}else{
				value_type = ValueType.FIX;
				value_size = Integer.parseInt(v);
				logger.info("Proposer value_size: " + value_size);
			}
			logger.info("Proposer value_type: " + value_type);
		}else{
			logger.info("Proposer value_size: " + value_size);
		}
		if(ring.getConfiguration().containsKey(ConfigKey.value_count)){
			value_count = Integer.parseInt(ring.getConfiguration().get(ConfigKey.value_count));
			logger.info("Proposer value_count: " + value_count);
		}
	}

	@Override
	public void run() {
		ring.getNetwork().registerCallback(this);
		Thread t = new Thread(new ProposerResender(this));
		t.setName("ProposerResender");
		t.start();
		if(batcher != null){
			batcher.setProposer(this);
			Thread b = new Thread(batcher);
			b.setName("BatchPolicy");
			b.start();
		}
	}

	/**
	 * Use this method if you propose byte[] from outside!
	 * 
	 * @param b A byte array which will proposed in a paxos instance
	 * @return A FutureDecision object on which you can wait until the value is proposed
	 */
	public synchronized FutureDecision propose(byte[] b){
		send_count++;
		Value v = new Value(System.nanoTime() + "" + ring.getNodeID(),b);
		if(proposallogger.isDebugEnabled()){
			proposallogger.debug(v);
		}else if(proposallogger.isInfoEnabled()){
			proposallogger.info(v.asString());
		}
		FutureDecision future = new FutureDecision();
		futures.put(v.getID(),future);
		Message m = new Message(0,ring.getNodeID(),PaxosRole.Leader,MessageType.Value,0,0,v);
		if(batcher != null){
			send_queue.add(m);
		}else{
			send(m);
		}
		return future;
	}
	
	/**
	 * @param m
	 */
	public void send(Message m){
		proposals.put(m.getValue().getID(),new Proposal(m.getValue()));
		ring.getNetwork().send(m); // send to all !
		if(ring.getNetwork().getLearner() != null){
			ring.getNetwork().getLearner().deliver(ring,m);
		}
		if(ring.getNetwork().getAcceptor() != null){
			ring.getNetwork().getAcceptor().deliver(ring,m);
		}
		if(ring.getNetwork().getLeader() != null){
			ring.getNetwork().getLeader().deliver(ring,m);
		}
	}
	
	public void deliver(RingManager fromRing,Message m){
		/*if(logger.isDebugEnabled()){
			logger.debug("proposer " + ring.getNodeID() + " received " + m);
		}*/
		if(m.getType() == MessageType.Decision){
			String ID = m.getValue().getID();
			if(proposals.containsKey(ID)){
				Proposal p = proposals.get(ID);
				Value v = p.getValue();
				if(m.getValue().equals(v)){ // compared by ID
					if(v.isBatch()){
						ByteBuffer buffer = ByteBuffer.wrap(v.getValue());
						while(buffer.remaining() > 0){
							try {
								Message n = Message.fromBuffer(buffer);
								set_decision(fromRing,n,n.getValue());
							} catch (Exception e) {
								logger.error("Proposer could not de-serialize batch message!" + e);
							}
						}
					}else{
						set_decision(fromRing,m,v);
					}
				}else{
					logger.error("Proposer received Decision with different values for same instance " + m.getInstance() + "!");
				}
				proposals.remove(ID);
			}
		}
	}

	private void set_decision(RingManager fromRing,Message m,Value v){
		String ID = m.getValue().getID();
		if(futures.containsKey(ID)){
			FutureDecision f = futures.get(ID);
			f.setDecision(new Decision(fromRing.getRingID(),m.getInstance(),m.getBallot(),v));
			futures.remove(ID);
		}
		if(test){
			long time = System.nanoTime();
			long send_time = Long.valueOf(ID.substring(0,ID.length()-1)); // since ID == nano-time + ring-id
			long lat = time - send_time;
			latency.add(lat);
			if(send_count < value_count){
				propose(getTestValue());
			}else{
				printHistogram();
				test = false;
			}
		}
		if(!test && logger.isDebugEnabled()){
			long time = System.nanoTime();
			long send_time = Long.valueOf(ID.substring(0,ID.length()-1)); // since ID == nano-time + ring-id
			long lat = time - send_time;
			logger.debug("Value " + v + " proposed and learned in " + lat + " ns (@proposer)");
		}		
	}

	/**
	 * @return the open proposals
	 */
	public Map<String, Proposal> getProposals(){
		return proposals;
	}
	
	/**
	 * @return the ring manager
	 */
	public RingManager getRingManager(){
		return ring;
	}
	
	/**
	 * @return the send (batch) queue
	 */
	public BlockingQueue<Message> getSendQueue(){
		return send_queue;
	}
	
	public void setTestMode(){
		test = true;
	}
	
	public byte[] getTestValue(){
		int v = value_size;
		switch(value_type){
		case INTVALUE: // use for correctness test
			return Integer.toString(random.nextInt(0,Integer.MAX_VALUE)).getBytes();
		case NORMAL: 
			v = (int)random.nextGaussian(16000,14000); // tune this parameter to something meaningful
			break;
		case EXPONENTIAL:
			v = (int)random.nextExponential(16000); 
			break;
		case ZIPF:
			v = random.nextZipf(60000,0.5); // Extremely slow?
			break;
		default:
			v = value_size;
			break;
		}
		if(v > 0 && v <= 60000){
			return new byte[v];
		}else{
			return new byte[value_size];
		}
	}

	public int getValueCount(){
		return value_count;
	}

	public int getConcurrentValues(){
		return concurrent_values;
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
			Long key = new Long(Math.round(l/1000/1000));
			if(histogram.containsKey(key)){
				histogram.put(key,histogram.get(key)+1);
			}else{
				histogram.put(key,1L);
			}
		}
		float avg = (float)sum/latency.size()/1000/1000;
		logger.info("proposer latency histogram: <1ms:" + a + " <10ms:" + b + " <25ms:" + b2 + " <50ms:" + c + " <75ms:" + f + " <100ms:" + d + " >100ms:" + e + " avg:" + avg);
		if(stats.isDebugEnabled()){
			for(Entry<Long, Long> bin : histogram.entrySet()){
				stats.debug(bin.getKey() + "," + bin.getValue());
			}
		}
	}
}
