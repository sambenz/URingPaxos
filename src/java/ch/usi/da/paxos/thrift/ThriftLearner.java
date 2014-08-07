package ch.usi.da.paxos.thrift;
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

import org.apache.log4j.Logger;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import ch.usi.da.paxos.api.Learner;
import ch.usi.da.paxos.thrift.gen.PaxosLearnerService;
import ch.usi.da.paxos.thrift.gen.PaxosLearnerService.Iface;
import ch.usi.da.paxos.thrift.gen.PaxosLearnerService.Processor;

/**
 * Name: ThriftLearner<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ThriftLearner implements Runnable {

	private final static Logger logger = Logger.getLogger(ThriftLearner.class);

	private int port;
	
	private final Learner learner;
	
	public ThriftLearner(Learner learner, int port) {
		this.learner = learner;
		this.port = port;
	}

	@Override
	public void run() {
       try {
           TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
           PaxosLearnerService.Processor<Iface> processor = new Processor<Iface>(new PaxosLearnerServiceImpl(this.learner));
           TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
           logger.info("Starting thrift learner server on port " + port);
           server.serve();
        } catch (TTransportException e) {
           logger.error(e);
        }
	}

}
