package ch.usi.da.paxos;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.usi.da.paxos.lab.DummyWatcher;
import ch.usi.da.paxos.message.Control;
import ch.usi.da.paxos.message.ControlType;
import ch.usi.da.paxos.ring.Node;
import ch.usi.da.paxos.storage.Decision;

public class TestMdRP {

	Logger logger = Logger.getLogger("ch.usi.da");
	Node s1;
	Node s2;
	Node aa;
	Node g1;
	Node g2;
	
	@BeforeClass
	public static void prepare() throws Exception {
		Thread.sleep(3000);
		ZooKeeper zoo = new ZooKeeper("localhost:2181",1000,new DummyWatcher());
		String path = "/ringpaxos/topology1/config/stable_storage";
		String data = "ch.usi.da.paxos.storage.InMemory";
		zoo.setData(path,data.getBytes(),-1);
		path = "/ringpaxos/config/multi_ring_lambda";
		data = "0";
		zoo.setData(path,data.getBytes(),-1);
		zoo.close();
	}
	
	@Before 
	public void initialize() throws Exception {
		logger.setLevel(Level.INFO);
		s1 = new Node(1,"localhost:2181",Util.parseRingsArgument("1:PAL"));
		s2 = new Node(2,"localhost:2181",Util.parseRingsArgument("2:PAL"));
		aa = new Node(3,"localhost:2181",Util.parseRingsArgument("1:A;2:A"));
		g1 = new Node(4,1,"localhost:2181",Util.parseRingsArgument("1:L"));
		g2 = new Node(5,2,"localhost:2181",Util.parseRingsArgument("2:L"));
		s1.start();
		s2.start();
		aa.start();
		g1.start();
		g2.start();
		Thread.sleep(6000); // wait until ring is fully started 
	}
	
	@After 
	public void close() throws Exception {
		s1.stop();
		s2.stop();
		aa.stop();
		g1.stop();
		g2.stop();
	}

	@Test
	public void basicUnSubscribe() throws Exception {
		
		// stream 1
		String s = "m1";
		s1.getProposer(1).propose(s.getBytes());
		s = "m3";
		s1.getProposer(1).propose(s.getBytes());

		Control c1 = new Control(1,ControlType.Subscribe,1,2);
		s1.getProposer(1).control(c1);

		s = "m5";
		s1.getProposer(1).propose(s.getBytes());
		s = "m7";
		s1.getProposer(1).propose(s.getBytes());		
		s = "m9";
		s1.getProposer(1).propose(s.getBytes());
		s = "m11";
		s1.getProposer(1).propose(s.getBytes());
		s = "m13";
		s1.getProposer(1).propose(s.getBytes());
		
		Control c2 = new Control(1,ControlType.Unsubscribe,1,2);
		s1.getProposer(1).control(c2);

		s = "m15";
		s1.getProposer(1).propose(s.getBytes());
		s = "m17";
		s1.getProposer(1).propose(s.getBytes());
		s = "m19";
		s1.getProposer(1).propose(s.getBytes());
		s = "m21";
		s1.getProposer(1).propose(s.getBytes());

		// stream 2
		s = "s,1,2";
		s2.getProposer(2).control(c1);
		Thread.sleep(2000); // give time to start up; recovery starts when next value is proposed

		s = "m2";
		s2.getProposer(2).propose(s.getBytes());
		s = "m4";
		s2.getProposer(2).propose(s.getBytes());
		s = "m6";
		s2.getProposer(2).propose(s.getBytes());
		s = "m8";
		s2.getProposer(2).propose(s.getBytes());
		s = "m10";
		s2.getProposer(2).propose(s.getBytes());
		
		s = "u,1,2";
		s2.getProposer(2).control(c2);
		
		s = "m12";
		s2.getProposer(2).propose(s.getBytes());
		s = "m14";
		s2.getProposer(2).propose(s.getBytes());
		s = "m16";
		s2.getProposer(2).propose(s.getBytes());
		s = "m18";
		s2.getProposer(2).propose(s.getBytes());
		s = "m20";
		s2.getProposer(2).propose(s.getBytes());
		s = "m22";
		s2.getProposer(2).propose(s.getBytes());
				
		Thread.sleep(2000); // wait until everything is proposed
		
		System.err.println(format(g1.getLearner().getDecisions()));
		System.err.println(format(g2.getLearner().getDecisions()));
		
		assertEquals(format(g1.getLearner().getDecisions()),"[m1,m3,m5,m6,m7,m8,m9,m10,m11,m13,m15,m17,m19,m21]");
		assertEquals(format(g2.getLearner().getDecisions()),"[m2,m4,m6,m8,m10,m12,m14,m16,m18,m20,m22]");

	}
		
	@Test
	public void doubleOffsetSubscribe() throws Exception {
		
		// stream 1
		String s = "m1";
		s1.getProposer(1).propose(s.getBytes());
		s = "m3";
		s1.getProposer(1).propose(s.getBytes());
		s = "m5";
		s1.getProposer(1).propose(s.getBytes());
		s = "m7";
		s1.getProposer(1).propose(s.getBytes());

		Control c1 = new Control(1,ControlType.Subscribe,1,2);
		s1.getProposer(1).control(c1);
		Thread.sleep(2000); // give time to start up; recovery starts when next value is proposed
		
		s = "m9";
		s1.getProposer(1).propose(s.getBytes());
		s = "m11";
		s1.getProposer(1).propose(s.getBytes());
		s = "m13";
		s1.getProposer(1).propose(s.getBytes());
		s = "m15";
		s1.getProposer(1).propose(s.getBytes());

		Control c2 = new Control(1,ControlType.Subscribe,2,1);
		s1.getProposer(1).control(c2);

		s = "m17";
		s1.getProposer(1).propose(s.getBytes());
		s = "m19";
		s1.getProposer(1).propose(s.getBytes());


		// stream 2
		s = "m2";
		s2.getProposer(2).propose(s.getBytes());
		s = "m4";
		s2.getProposer(2).propose(s.getBytes());

		s2.getProposer(2).control(c1);

		s = "m6";
		s2.getProposer(2).propose(s.getBytes());
		s = "m8";
		s2.getProposer(2).propose(s.getBytes());
		s = "m10";
		s2.getProposer(2).propose(s.getBytes());
		s = "m12";
		s2.getProposer(2).propose(s.getBytes());

		s2.getProposer(2).control(c2);
		Thread.sleep(2000); // give time to start up; recovery starts when next value is proposed
		s = "m21";
		s1.getProposer(1).propose(s.getBytes());
		s = "m23";
		s1.getProposer(1).propose(s.getBytes());

		
		s = "m14";
		s2.getProposer(2).propose(s.getBytes());
		s = "m16";
		s2.getProposer(2).propose(s.getBytes());
		s = "m18";
		s2.getProposer(2).propose(s.getBytes());
		s = "m20";
		s2.getProposer(2).propose(s.getBytes());
		s = "m22";
		s2.getProposer(2).propose(s.getBytes());

		Thread.sleep(5000); // wait until everything is proposed

		System.err.println(format(g1.getLearner().getDecisions()));
		System.err.println(format(g2.getLearner().getDecisions()));

		assertEquals(format(g1.getLearner().getDecisions()),"[m1,m3,m5,m7,m9,m10,m11,m12,m13,m15,m14,m16,m17,m18,m19,m20,m21,m22,m23]");
		assertEquals(format(g2.getLearner().getDecisions()),"[m2,m4,m6,m8,m10,m12,m14,m16,m17,m18,m19,m20,m21,m22,m23]");

	}

	@Test
	public void doubleInterleaveSubscribe() throws Exception {
		
		// stream 1
		String s = "m1";
		s1.getProposer(1).propose(s.getBytes());

		Control c1 = new Control(1,ControlType.Subscribe,1,2);
		s1.getProposer(1).control(c1);
		Thread.sleep(2000); // give time to start up; recovery starts when next value is proposed

		s = "m3";
		s1.getProposer(1).propose(s.getBytes());
		s = "m5";
		s1.getProposer(1).propose(s.getBytes());
		s = "m7";
		s1.getProposer(1).propose(s.getBytes());		
		s = "m9";
		s1.getProposer(1).propose(s.getBytes());
		
		Control c2 = new Control(1,ControlType.Subscribe,2,1);
		s1.getProposer(1).control(c2);
		
		s = "m11";
		s1.getProposer(1).propose(s.getBytes());
		s = "m13";
		s1.getProposer(1).propose(s.getBytes());
		s = "m15";
		s1.getProposer(1).propose(s.getBytes());
		s = "m17";
		s1.getProposer(1).propose(s.getBytes());
		s = "m19";
		s1.getProposer(1).propose(s.getBytes());


		// stream 2
		s = "m2";
		s2.getProposer(2).propose(s.getBytes());
		s = "m4";
		s2.getProposer(2).propose(s.getBytes());
		s = "m6";
		s2.getProposer(2).propose(s.getBytes());

		s2.getProposer(2).control(c2);
		Thread.sleep(2000); // give time to start up; recovery starts when next value is proposed
		s = "m21";
		s1.getProposer(1).propose(s.getBytes());
		s = "m23";
		s1.getProposer(1).propose(s.getBytes());

		s = "m8";
		s2.getProposer(2).propose(s.getBytes());
		s = "m10";
		s2.getProposer(2).propose(s.getBytes());
		s = "m12";
		s2.getProposer(2).propose(s.getBytes());		
		s = "m14";
		s2.getProposer(2).propose(s.getBytes());

		s2.getProposer(2).control(c1);

		s = "m16";
		s2.getProposer(2).propose(s.getBytes());
		s = "m18";
		s2.getProposer(2).propose(s.getBytes());
		s = "m20";
		s2.getProposer(2).propose(s.getBytes());
		s = "m22";
		s2.getProposer(2).propose(s.getBytes());
		s = "m24";
		s2.getProposer(2).propose(s.getBytes());

		Thread.sleep(7000); // wait until everything is proposed

		System.err.println(format(g1.getLearner().getDecisions()));
		System.err.println(format(g2.getLearner().getDecisions()));

		assertEquals(format(g1.getLearner().getDecisions()),"[m1,m3,m5,m7,m9,m11,m13,m15,m16,m17,m18,m19,m20,m21,m22,m23,m24]");
		assertEquals(format(g2.getLearner().getDecisions()),"[m2,m4,m6,m8,m10,m12,m11,m14,m13,m15,m16,m17,m18,m19,m20,m21,m22,m23,m24]");
		
	}

	public String format(BlockingQueue<Decision> list){
		StringBuffer b = new StringBuffer();
		b.append("[");
		for(Decision d : list){
			b.append(d.getValue().asString() + ",");
		}
		b.deleteCharAt(b.length()-1);
		b.append("]");
		return b.toString();
	}
}