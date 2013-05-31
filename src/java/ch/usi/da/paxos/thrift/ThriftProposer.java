package ch.usi.da.paxos.thrift;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import ch.usi.da.paxos.ring.RingManager;
import ch.usi.da.paxos.thrift.gen.PaxosProposerService;
import ch.usi.da.paxos.thrift.gen.PaxosProposerService.Iface;
import ch.usi.da.paxos.thrift.gen.PaxosProposerService.Processor;

public class ThriftProposer implements Runnable {

	private final static Logger logger = Logger.getLogger(ThriftProposer.class);

	private final int port;
	
	private final RingManager ring;
	
	public ThriftProposer(RingManager ring) {
		this.ring = ring;
		port = 9080 + ring.getNodeID();
	}

	@Override
	public void run() {
		try {
           TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
           PaxosProposerService.Processor<Iface> processor = new Processor<Iface>(new PaxosProposerServiceImpl(ring));
           TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
           logger.info("Starting thrift proposer server on port " + port);
           server.serve();
        } catch (TTransportException e) {
           logger.error(e);
        }
	}

}
