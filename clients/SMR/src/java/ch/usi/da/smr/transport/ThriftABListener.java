package ch.usi.da.smr.transport;
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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.thrift.gen.Decision;
import ch.usi.da.smr.thrift.gen.PaxosLearnerService;
import ch.usi.da.smr.thrift.gen.PaxosLearnerService.Client;

/**
 * Name: ABListener<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ThriftABListener implements ABListener, Runnable {

	private final TTransport transport;
	
	private final TProtocol protocol;
	
	private final PaxosLearnerService.Client learner;
	
	private Receiver receiver = null;
	
	public ThriftABListener(String host, int port) throws TTransportException {
    	transport = new TFramedTransport(new TSocket(host,port));
        protocol = new TBinaryProtocol(transport);
        learner = new PaxosLearnerService.Client(protocol);
        transport.open();
	}

	@Override
	public void run() {
		while(transport.isOpen()){
			try {
				Decision d = learner.deliver(1000);
				if(d != null && d.getValue() != null){
					Message m = Message.fromDecision(d);
					if(m != null && receiver != null){
						receiver.receive(m);
					}
				}
			} catch (TTransportException e){
			} catch (TException e) {
				e.printStackTrace();
			}
		}		
	}
	
	@Override
	public void safe(int ring, long instance) throws TException {
		learner.safe(ring,instance);
	}

	@Override
	public void registerReceiver(Receiver receiver){
		this.receiver = receiver; 
	}

	public Client getLearner(){
		return learner;
	}
	
	public void close(){
		transport.close();
	}
}
