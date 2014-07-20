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
import ch.usi.da.paxos.message.Value;
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
		path = "/ringpaxos/config/multi_ring_lambda";
		data = "0";
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
		
		// pahse1range (executed during start-up)	
		assertEquals((int)11,(int)a1.getStableStorage().getBallot(1L));
		
		// phase1 (normal)
		a1.getStableStorage().putBallot(1L,0); // clear ballots				
		a2.getStableStorage().putBallot(1L,0);				
		a3.getStableStorage().putBallot(1L,0);				
		Message m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 1, null);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals((int)1,(int)a1.getStableStorage().getBallot(1L));
		assertEquals((int)1,(int)a2.getStableStorage().getBallot(1L));
		assertEquals((int)1,(int)a3.getStableStorage().getBallot(1L));
		
		// phase1 (higher ballot)
		a1.getStableStorage().putBallot(1L,0); // clear ballots				
		a2.getStableStorage().putBallot(1L,0);				
		a3.getStableStorage().putBallot(1L,0);				
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 1, null);
		a1.deliver(ring1, m);
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 2, null);
		a1.deliver(ring1, m);		
		Thread.sleep(1000);
		assertEquals((int)2,(int)a1.getStableStorage().getBallot(1L));
		assertEquals((int)2,(int)a2.getStableStorage().getBallot(1L));
		assertEquals((int)2,(int)a3.getStableStorage().getBallot(1L));
		
		// phase1 (smaller ballot)
		a1.getStableStorage().putBallot(1L,0); // clear ballots				
		a2.getStableStorage().putBallot(1L,0);				
		a3.getStableStorage().putBallot(1L,0);				
		a2.getStableStorage().putBallot(1L,2);
		a3.getStableStorage().putBallot(1L,2);		
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 1, null);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals((int)1,(int)a1.getStableStorage().getBallot(1L));
		assertEquals((int)2,(int)a2.getStableStorage().getBallot(1L));
		assertEquals((int)2,(int)a3.getStableStorage().getBallot(1L));		

		// phase1 (smaller ballot)
		a1.getStableStorage().putBallot(1L,2);				
		a2.getStableStorage().putBallot(1L,2);
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 1, null);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals((int)2,(int)a1.getStableStorage().getBallot(1L));
		assertEquals((int)2,(int)a2.getStableStorage().getBallot(1L));
		assertEquals((int)2,(int)a3.getStableStorage().getBallot(1L));
		
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

		// higher ballot but already decided
		String s = "Proposer 1 test!";
		FutureDecision fd = n1.getProposer(1).propose(s.getBytes());
		Decision d = fd.getDecision();
		assertEquals(s,new String(d.getValue().getValue())); // will decide instance 1 with ballot 11
		
		// send phase 1 for decided instance with higher ballot
		Message m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 50, null);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals(50,(int)a1.getStableStorage().getDecision(1L).getBallot());
		assertEquals(50,(int)a2.getStableStorage().getDecision(1L).getBallot());
		assertEquals(50,(int)a3.getStableStorage().getDecision(1L).getBallot());
		assertEquals(s,new String(a1.getStableStorage().getDecision(1L).getValue().getValue()));
		assertEquals(s,new String(a2.getStableStorage().getDecision(1L).getValue().getValue()));
		assertEquals(s,new String(a3.getStableStorage().getDecision(1L).getValue().getValue()));
		
		// create two decisions with different ballot and test that highest get resent
		a1.getStableStorage().putBallot(2L,70); // set ballots				
		a2.getStableStorage().putBallot(2L,80);				
		a3.getStableStorage().putBallot(2L,80);				
		s = "Decided 1";
		Value v = new Value(Value.getSkipID() + "1", s.getBytes());
		a1.getStableStorage().putDecision(2L,new Decision(1, 2L, 70, v));
		s = "Decided 2";
		v = new Value(Value.getSkipID() + "2", s.getBytes());
		a2.getStableStorage().putDecision(2L,new Decision(1, 2L, 80, v));
		a3.getStableStorage().putDecision(2L,new Decision(1, 2L, 80, v));
		m = new Message(2L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 110, null);
		a1.deliver(ring1, m);
		Thread.sleep(2000);
		assertEquals(110,(int)a1.getStableStorage().getDecision(2L).getBallot());
		assertEquals(110,(int)a2.getStableStorage().getDecision(2L).getBallot());
		assertEquals(110,(int)a3.getStableStorage().getDecision(2L).getBallot());
		assertEquals(110,(int)a1.getStableStorage().getBallot(2L));
		assertEquals(110,(int)a2.getStableStorage().getBallot(2L));
		assertEquals(110,(int)a3.getStableStorage().getBallot(2L));
		assertEquals(s,new String(a1.getStableStorage().getDecision(2L).getValue().getValue()));
		assertEquals(s,new String(a2.getStableStorage().getDecision(2L).getValue().getValue()));
		assertEquals(s,new String(a3.getStableStorage().getDecision(2L).getValue().getValue()));
		
		// create two decisions with different ballot and test that highest get resent
		a1.getStableStorage().putBallot(3L,80); // set ballots				
		a2.getStableStorage().putBallot(3L,80);				
		a3.getStableStorage().putBallot(3L,70);				
		s = "Decided 1";
		v = new Value(Value.getSkipID(), s.getBytes());
		a3.getStableStorage().putDecision(3L,new Decision(1, 3L, 70, v));
		s = "Decided 2";
		v = new Value(Value.getSkipID(), s.getBytes());
		a1.getStableStorage().putDecision(3L,new Decision(1, 3L, 80, v));
		a2.getStableStorage().putDecision(3L,new Decision(1, 3L, 80, v));
		m = new Message(3L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase1, 110, null);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals(110,(int)a1.getStableStorage().getDecision(3L).getBallot());
		assertEquals(110,(int)a2.getStableStorage().getDecision(3L).getBallot());
		assertEquals(110,(int)a3.getStableStorage().getDecision(3L).getBallot());
		assertEquals(110,(int)a1.getStableStorage().getBallot(3L));
		assertEquals(110,(int)a2.getStableStorage().getBallot(3L));
		assertEquals(110,(int)a3.getStableStorage().getBallot(3L));
		assertEquals(s,new String(a1.getStableStorage().getDecision(3L).getValue().getValue()));
		assertEquals(s,new String(a2.getStableStorage().getDecision(3L).getValue().getValue()));
		assertEquals(s,new String(a3.getStableStorage().getDecision(3L).getValue().getValue()));
		
	}
	
	@Test
	public void phase2() throws Exception {
		AcceptorRole a1 = (AcceptorRole) n1.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a2 = (AcceptorRole) n2.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a3 = (AcceptorRole) n3.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		RingManager ring1 = n1.getRings().get(0).getRingManager();

		/* 
		 * To send Phase2 messages to the acceptors without a previous Value message
		 * requires a ballot >= 100 or a Skip values.
		 * 
		 * This is due the ring management which will prevent sending Values twice the ring.
		 */
		
		// send phase 2 for undecided instance with smaller ballot
		String s = "Test Phase 2";
		Value v = new Value(Value.getSkipID(), s.getBytes());
		Message m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase2, 1, v);
		a1.deliver(ring1, m);
		Thread.sleep(1000);		
		assertEquals(null,a1.getStableStorage().getDecision(1L));
		assertEquals(null,a2.getStableStorage().getDecision(1L));
		assertEquals(null,a3.getStableStorage().getDecision(1L));

		// send phase 2 for undecided instance with exact ballot
		v = new Value(Value.getSkipID(), s.getBytes());
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase2, 11, v);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals(s,new String(a1.getStableStorage().getDecision(1L).getValue().getValue()));
		assertEquals(s,new String(a2.getStableStorage().getDecision(1L).getValue().getValue()));
		assertEquals(s,new String(a3.getStableStorage().getDecision(1L).getValue().getValue()));

		// send phase 2 for decided instance with higher ballot
		Value v2 = new Value(Value.getSkipID(), "Should no be decided!".getBytes());
		m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase2, 100, v2);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals(s,new String(a1.getStableStorage().getDecision(1L).getValue().getValue()));
		assertEquals(s,new String(a2.getStableStorage().getDecision(1L).getValue().getValue()));
		assertEquals(s,new String(a3.getStableStorage().getDecision(1L).getValue().getValue()));

		// send phase 2 for undecided instance with higher ballot
		v = new Value("Key 3", s.getBytes());
		m = new Message(2L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase2, 100, v);
		a1.deliver(ring1, m);
		Thread.sleep(1000);		
		assertEquals(s,new String(a1.getStableStorage().getDecision(2L).getValue().getValue()));
		assertEquals(s,new String(a2.getStableStorage().getDecision(2L).getValue().getValue()));
		assertEquals(s,new String(a3.getStableStorage().getDecision(2L).getValue().getValue()));
		
	}

	@Test
	public void phase2special() throws Exception {
		AcceptorRole a1 = (AcceptorRole) n1.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a2 = (AcceptorRole) n2.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		AcceptorRole a3 = (AcceptorRole) n3.getRings().get(0).getRingManager().getNetwork().getAcceptor();
		RingManager ring1 = n1.getRings().get(0).getRingManager();
		
		/*
		 * Test: must decide to the highest ballot/value quorum:
		 * 
		 * Phase 1 b:100 to all
		 * Assume 2a only received by A2
		 * New coordinator makes 1a b:101 (not received by A2)
		 * New coordinator send Phase 2 to all
		 */
		
		a1.getStableStorage().putBallot(1L,100);
		a2.getStableStorage().putBallot(1L,100);
		a3.getStableStorage().putBallot(1L,100);

		String s0 = "Decided";
		Value v0 = new Value(Value.getSkipID(), s0.getBytes());
		a2.getStableStorage().putDecision(1L,new Decision(1, 1L, 100, v0));
		
		String s = "Test Phase 2 special";
		Value v = new Value(Value.getSkipID(), s.getBytes());
		Message m = new Message(1L,ring1.getNodeID(),PaxosRole.Acceptor,MessageType.Phase2, 101, 101, v);
		a1.deliver(ring1, m);
		Thread.sleep(1000);
		assertEquals(101,(int)a1.getStableStorage().getDecision(1L).getBallot());
		assertEquals(101,(int)a2.getStableStorage().getDecision(1L).getBallot());
		assertEquals(101,(int)a3.getStableStorage().getDecision(1L).getBallot());
		assertEquals(101,(int)a1.getStableStorage().getBallot(1L));
		assertEquals(101,(int)a2.getStableStorage().getBallot(1L));
		assertEquals(101,(int)a3.getStableStorage().getBallot(1L));
		assertEquals(s,new String(a1.getStableStorage().getDecision(1L).getValue().getValue()));
		assertEquals(s,new String(a2.getStableStorage().getDecision(1L).getValue().getValue()));
		assertEquals(s,new String(a3.getStableStorage().getDecision(1L).getValue().getValue()));
		
	}

}
