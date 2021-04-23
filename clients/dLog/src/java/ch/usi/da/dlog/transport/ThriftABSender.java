package ch.usi.da.dlog.transport;
/* 
 * Copyright (c) 2014 Università della Svizzera italiana (USI)
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

import ch.usi.da.dlog.message.Message;
import ch.usi.da.dlog.thrift.gen.PaxosProposerService;
import ch.usi.da.dlog.thrift.gen.Value;
import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

/**
 * Name: ThriftABSender<br>
 * Description: <br>
 * 
 * Creation date: Apr 07, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ThriftABSender implements ABSender {

	private final TTransport transport;
    
	private final TProtocol protocol;
	
    private final PaxosProposerService.Client proposer;

	public ThriftABSender(String host, int port) throws TTransportException {
    	transport = new TFramedTransport(new TSocket(host,port));
        protocol = new TBinaryProtocol(transport);
        proposer = new PaxosProposerService.Client(protocol);
        transport.open();
	}
	
	@Override
	public long abroadcast(Message m){
		Value value = new Value(ByteBuffer.wrap(Message.toByteArray(m)));
		try {
			//long start = System.nanoTime();
			long ret = 1; //proposer.propose(value);
			proposer.nb_propose(value);
			//System.err.println(System.nanoTime() - start);
			return ret;
		} catch (TException e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public PaxosProposerService.Client getProposer(){
		return proposer;
	}
	
	@Override
	public void close(){
		transport.close();
	}
}
