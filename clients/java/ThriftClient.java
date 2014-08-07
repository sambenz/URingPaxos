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

import ch.usi.da.paxos.thrift.gen.PaxosLearnerService;
import ch.usi.da.paxos.thrift.gen.PaxosProposerService;
import ch.usi.da.paxos.thrift.gen.Value;

/**
 * Name: ThriftClient<br>
 * Description: <br>
 * 
 * Creation date: Feb 7, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class ThriftClient {
  
    public static void main(String[] args) {
    	ThriftClient client = new ThriftClient();
    	Thread t = new Thread(client.new ThriftLearnerClient());
    	t.start();
    	Thread t2 = new Thread(client.new ThriftProposerClient());
    	t2.start();
    }

    public class ThriftProposerClient implements Runnable {

		@Override
		public void run() {
	        try {
	        	TTransport t = new TFramedTransport(new TSocket("localhost", 9080));
	            TProtocol p = new TBinaryProtocol(t);
	            PaxosProposerService.Client proposer = new PaxosProposerService.Client(p);
	            t.open();
	            Value value;
	            for(int i=0;i<1000;i++){
	            	String s = "Hi Thrift! (" + i + ")";
	            	value = new Value(ByteBuffer.wrap(s.getBytes()));
	            	//long time = System.nanoTime();
	            	long instance = proposer.propose(value);
	            	if(instance < 0){
	            		System.err.println("error in proposing " + value + "!");
	            	}
	            	//System.err.println("proposed in " + (System.nanoTime()-time));
	            }
	            t.close();
	        } catch (TTransportException e) {
	            e.printStackTrace();
	        } catch (TException e) {
	            e.printStackTrace();
	        }
		}
    	
    }
    
    public class ThriftLearnerClient implements Runnable {

		@Override
		public void run() {
	        try {
	        	TTransport t = new TFramedTransport(new TSocket("localhost", 9090));
	            TProtocol p = new TBinaryProtocol(t);
	            PaxosLearnerService.Client learner = new PaxosLearnerService.Client(p);
	            t.open();
	            Value value;
	            while((value = learner.deliver(4000)).cmd == null ? false : true){
	            	System.out.println(new String(value.getCmd()));
	            }
	            t.close();
	        } catch (TTransportException e) {
	            e.printStackTrace();
	        } catch (TException e) {
	            e.printStackTrace();
	        }
		}
    	
    }

}