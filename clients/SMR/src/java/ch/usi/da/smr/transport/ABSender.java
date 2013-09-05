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

import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.thrift.gen.PaxosProposerService;
import ch.usi.da.smr.thrift.gen.Value;

/**
 * Name: ABSender<br>
 * Description: <br>
 * 
 * Creation date: Mar 12, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class ABSender {

	private final TTransport transport;
    
	private final TProtocol protocol;
	
    private final PaxosProposerService.Client proposer;

	public ABSender(String host, int port) throws TTransportException {
    	transport = new TFramedTransport(new TSocket(host,port));
        protocol = new TBinaryProtocol(transport);
        proposer = new PaxosProposerService.Client(protocol);
        transport.open();
	}
			
	public int abroadcast(Message m){
		Value value = new Value(ByteBuffer.wrap(Message.toByteArray(m)));
		try {
			return proposer.propose(value);
		} catch (TException e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public PaxosProposerService.Client getProposer(){
		return proposer;
	}
	
	public void close(){
		transport.close();
	}
}
