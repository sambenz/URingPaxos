package ch.usi.da.paxos;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.ring.RingManager;

public class TestTopology {

	Logger logger = Logger.getLogger("ch.usi.da");
	
	Random rand = new Random();

	@Before 
	public void initialize() throws Exception {
		logger.setLevel(Level.ERROR);
	}

	@Test
	public void RingManager() throws Exception {
		
		ZooKeeper zoo = new ZooKeeper("127.0.0.1:2181",500,null);
		InetSocketAddress addr = new InetSocketAddress(Util.getHostAddress(),getPort());
		RingManager rm1 = new RingManager(1,2,addr,zoo,"/ringpaxos");
		rm1.init();

		Thread.sleep(1000);
		assertEquals(1,rm1.getRing().size());
		assertEquals(0,rm1.getAcceptors().size());
		assertEquals(0,rm1.getProposers().size());
		assertEquals(0,rm1.getLearners().size());
		
		rm1.registerRole(PaxosRole.Acceptor);
		rm1.registerRole(PaxosRole.Learner);
		rm1.registerRole(PaxosRole.Proposer);
		
		Thread.sleep(1000);
		assertEquals(1,rm1.getRing().size());
		assertEquals(1,rm1.getAcceptors().size());
		assertEquals(1,rm1.getProposers().size());
		assertEquals(1,rm1.getLearners().size());
		assertEquals(2,rm1.getCoordinatorID());
		assertEquals(2,rm1.getLastAcceptor());
				
		ZooKeeper zoo2 = new ZooKeeper("127.0.0.1:2181",500,null);
		InetSocketAddress addr2 = new InetSocketAddress(Util.getHostAddress(),getPort());
		RingManager rm2 = new RingManager(1,1,addr2,zoo2,"/ringpaxos");
		rm2.init();

		Thread.sleep(1000);		
		assertEquals(2,rm1.getRing().size());

		rm2.registerRole(PaxosRole.Acceptor);
		rm2.registerRole(PaxosRole.Learner);
		rm2.registerRole(PaxosRole.Proposer);

		Thread.sleep(1000);
		assertEquals(2,rm1.getAcceptors().size());
		assertEquals(2,rm1.getProposers().size());
		assertEquals(2,rm1.getLearners().size());
		assertEquals(1,rm1.getCoordinatorID());
		assertEquals(2,rm1.getLastAcceptor());
    	
		zoo.close();
		zoo2.close();
	}

	@Test
	public void TopoManager() throws Exception {
		
		ZooKeeper zoo = new ZooKeeper("127.0.0.1:2181",500,null);
		InetSocketAddress addr = new InetSocketAddress(Util.getHostAddress(),getPort());
		TopologyManager tm1 = new TopologyManager(1,1,addr,zoo,"/ringpaxos");
		tm1.init();
		
		assertEquals(1,tm1.getNodes().size());

		ZooKeeper zoo2 = new ZooKeeper("127.0.0.1:2181",500,null);
		InetSocketAddress addr2 = new InetSocketAddress(Util.getHostAddress(),getPort());
		TopologyManager tm2 = new TopologyManager(1,2,addr2,zoo2,"/ringpaxos");
		tm2.init();

		Thread.sleep(1000);
		
		assertEquals(2,tm1.getNodes().size());

		zoo.close();
		zoo2.close();
	}

	private int getPort(){
		return 2000 + rand.nextInt(1000); // assign port between 2000-3000
	}
}
