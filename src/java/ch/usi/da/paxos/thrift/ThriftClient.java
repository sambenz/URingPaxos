package ch.usi.da.paxos.thrift;

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
	            	int instance = proposer.propose(value);
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