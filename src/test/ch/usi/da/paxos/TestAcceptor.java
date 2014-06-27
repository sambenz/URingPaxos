package ch.usi.da.paxos;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.examples.Util;
import ch.usi.da.paxos.lab.DummyWatcher;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.ring.AcceptorRole;
import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.ring.RingManager;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.storage.FutureDecision;

public class TestAcceptor {

	Logger logger = Logger.getLogger("ch.usi.da");
	Node n1;
	Node n2;
	Node n3;
	
	@BeforeClass
	public static void prepare() throws Exception {
		ZooKeeper zoo = new ZooKeeper("localhost:2181",1000,new DummyWatcher());
		String path = "/ringpaxos/ring1/config/stable_storage";
		String data = "ch.usi.da.paxos.storage.InMemory";
		zoo.setData(path,data.getBytes(),-1);
		zoo.close();
	}
	
	@Before 
	public void initialize() throws Exception {
		logger.setLevel(Level.ERROR);
		n1 = new Node("localhost:2181",Util.parseRingsArgument("1,1:PAL"));
		n2 = new Node("localhost:2181",Util.parseRingsArgument("1,2:PAL"));
		n3 = new Node("localhost:2181",Util.parseRingsArgument("1,3:PAL"));
		n1.start();
		n2.start();
		n3.start();
		Thread.sleep(6000); // wait until ring is fully started 
	}
	
	@After 
	public void close() throws Exception {
		n1.stop();
		n2.stop();
		n3.stop();
	}

	@Test
	public void basicPropose() throws Exception {
		String s = "Proposer 1 test!";
		FutureDecision fd = n1.getProposer(1).propose(s.getBytes());
		Decision d = fd.getDecision();
		assertEquals(s,new String(d.getValue().getValue()));

		s = "Proposer 2 test!";
		fd = n2.getProposer(1).propose(s.getBytes());
		d = fd.getDecision();
		assertEquals(s,new String(d.getValue().getValue()));

		s = "Proposer 3 test!";
		fd = n3.getProposer(1).propose(s.getBytes());
		d = fd.getDecision();
		assertEquals(s,new String(d.getValue().getValue()));
	}

	@Test
	public void basicLearn() throws Exception {
		String s = "Junit test: ";
		Node[] nodes = new Node[]{ n1, n2, n3};
		Random random = new Random();
		for(int i=0;i<100;i++){
			int n = random.nextInt(3);
			nodes[n].getProposer(1).propose((s + n+1 + " " + i).getBytes());
		}
		Thread.sleep(5000); // wait for at least one re-transmit
		
		List<Decision> d1 = new ArrayList<Decision>(); 
		n1.getLearner().getDecisions().drainTo(d1);

		List<Decision> d2 = new ArrayList<Decision>(); 
		n2.getLearner().getDecisions().drainTo(d2);

		List<Decision> d3 = new ArrayList<Decision>(); 
		n3.getLearner().getDecisions().drainTo(d3);

		assertEquals(true,d1.size() >= 100); // why >= ? Because there could be also skip instances.
		assertEquals(d1,d2);
		assertEquals(d2,d3);
	}

	@Test
	public void phase1() throws Exception {
		AcceptorRole a1 = (AcceptorRole) n1.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a2 = (AcceptorRole) n2.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a3 = (AcceptorRole) n3.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		RingManager ring1 = n1.getRings().get(0).getRingManager();
		//RingManager ring2 = n2.getRings().get(0).getRingManager();
		//RingManager ring3 = n3.getRings().get(0).getRingManager();
		
		// pahse1range (executed during start-up)
		assertEquals(5000,a1.getPromised().size());		
		assertEquals((int)11,(int)a1.getPromised().get(1L));
		
		// phase1 (normal)
		a1.getPromised().clear();
		a2.getPromised().clear();
		a3.getPromised().clear();
		Message m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 1, null);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals((int)1,(int)a1.getPromised().get(1L));
		assertEquals((int)1,(int)a2.getPromised().get(1L));
		assertEquals((int)1,(int)a3.getPromised().get(1L));
		
		// phase1 (higher ballot)
		a1.getPromised().clear();
		a2.getPromised().clear();
		a3.getPromised().clear();
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 1, null);
		a1.deliver(ring1, m);
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 2, null);
		a1.deliver(ring1, m);		
		Thread.sleep(1000);
		assertEquals((int)2,(int)a1.getPromised().get(1L));
		assertEquals((int)2,(int)a2.getPromised().get(1L));
		assertEquals((int)2,(int)a3.getPromised().get(1L));
		
		// phase1 (smaller ballot)
		a1.getPromised().clear();
		a2.getPromised().clear();
		a2.getPromised().put(1L,2);
		a3.getPromised().clear();
		a3.getPromised().put(1L,2);		
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 1, null);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals((int)1,(int)a1.getPromised().get(1L));
		assertEquals((int)2,(int)a2.getPromised().get(1L)); // will not forward message
		assertEquals((int)2,(int)a3.getPromised().get(1L));		

		// phase1 (smaller ballot)
		a1.getPromised().clear();
		a1.getPromised().put(1L,2);				
		a2.getPromised().clear();
		a2.getPromised().put(1L,2);
		a3.getPromised().clear();
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 1, null);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals((int)2,(int)a1.getPromised().get(1L)); // will not forward message
		assertEquals((int)2,(int)a2.getPromised().get(1L));
		assertEquals(false,a3.getPromised().containsKey(1L)); // since a1 will not forward?		
		
		/* CoordinatorRole c = (CoordinatorRole) n1.getRings().get(0).getRingManager().getNetwork().getLeader();
		List<Promise> pl = new ArrayList<Promise>();
		c.getPromiseQueue().drainTo(pl);
		for(Promise p : pl){
			System.err.println(p.getInstance() + " " + p.getBallot());
		}*/
	}

	@Test
	public void phase1decided() throws Exception {
		AcceptorRole a1 = (AcceptorRole) n1.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a2 = (AcceptorRole) n2.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a3 = (AcceptorRole) n3.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		RingManager ring1 = n1.getRings().get(0).getRingManager();
		//RingManager ring2 = n2.getRings().get(0).getRingManager();
		//RingManager ring3 = n3.getRings().get(0).getRingManager();

		// higher ballot but already decided
		String s = "Proposer 1 test!";
		FutureDecision fd = n1.getProposer(1).propose(s.getBytes());
		Decision d = fd.getDecision();
		assertEquals(s,new String(d.getValue().getValue())); // will decide instance 1 with ballot 11
		
		// send phase 1 for decided instance with higher ballot
		Message m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 50, null);
		a1.deliver(ring1, m);
		
		Thread.sleep(1000);
		
		assertEquals(50,(int)a1.getStableStorage().get(1L).getBallot());
		assertEquals(50,(int)a2.getStableStorage().get(1L).getBallot());
		assertEquals(50,(int)a3.getStableStorage().get(1L).getBallot());
		
	}
	
	@Test
	public void phase2() throws Exception {
		AcceptorRole a1 = (AcceptorRole) n1.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a2 = (AcceptorRole) n2.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a3 = (AcceptorRole) n3.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		RingManager ring1 = n1.getRings().get(0).getRingManager();
		RingManager ring2 = n2.getRings().get(0).getRingManager();
		RingManager ring3 = n3.getRings().get(0).getRingManager();
		
		// pahse1range (executed during start-up)
		assertEquals(5000,a1.getPromised().size());		
	}

}
