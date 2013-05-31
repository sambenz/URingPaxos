package ch.usi.da.paxos.thrift;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import ch.usi.da.paxos.ring.RingManager;
import ch.usi.da.paxos.thrift.gen.PaxosLearnerService;
import ch.usi.da.paxos.thrift.gen.PaxosLearnerService.Iface;
import ch.usi.da.paxos.thrift.gen.PaxosLearnerService.Processor;

public class ThriftLearner implements Runnable {

	private final static Logger logger = Logger.getLogger(ThriftLearner.class);

	private int port;
	
	private final RingManager ring;
	
	public ThriftLearner(RingManager ring) {
		this.ring = ring;
		port = 9090 + ring.getNodeID();
	}

	@Override
	public void run() {
       try {
           TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
           PaxosLearnerService.Processor<Iface> processor = new Processor<Iface>(new PaxosLearnerServiceImpl(ring));
           TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
           logger.info("Starting thrift learner server on port " + port);
           server.serve();
        } catch (TTransportException e) {
           logger.error(e);
        }
	}

}
