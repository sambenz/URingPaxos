package ch.usi.da.smr.statistics;
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

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import ch.usi.da.smr.thrift.gen.Decision;
import ch.usi.da.smr.thrift.gen.Value;
import ch.usi.da.smr.transport.ThriftABListener;
import ch.usi.da.smr.transport.ThriftABSender;

/**
 * Name: StatisticClient<br>
 * Description: This is a single threaded thrift interface tester.<br>
 * 
 * The SMR replica is not involved in this test!
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class StatisticClient implements Runnable {

	private final ThriftABSender proposer;
	
	private final ThriftABListener learner;
	
	public StatisticClient() throws TTransportException{
		proposer = new ThriftABSender("localhost",9081);
		learner = new ThriftABListener("localhost",9093);
	}

	@Override
	public void run() {
		while(learner.getLearner().getInputProtocol().getTransport().isOpen()){
			try {
				Decision d = learner.getLearner().deliver(1000);
				if(d.isSetInstance()){
					// do nothing (only an example)
				}
			} catch (TTransportException e){
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}

	public ThriftABSender getProposer(){
		return proposer;
	}

	public ThriftABListener getLearner(){
		return learner;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			StatisticClient client = new StatisticClient();
			Statistics stats = new Statistics();
			Thread t = new Thread(client);
			t.start();
			Thread.sleep(1000);
			Value v = new Value();
			v.setCmd(new byte[32654]);
			int i = 0;
			while(i<100000){
				long start = System.nanoTime();
				client.getProposer().getProposer().propose(v); // blocking call
				stats.feedEvent(new Event(v.getCmd().length,System.nanoTime()-start));
				i++;
			}
			client.getProposer().close();
			client.getLearner().close();
		} catch (InterruptedException | TException e) {
			e.printStackTrace();
		}
	}

}
