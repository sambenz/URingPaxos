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

public class TestMdRPExtended {

	Logger logger = Logger.getLogger("ch.usi.da");
	Node s1;
	Node s2;
	Node s3;
	Node s4;
	Node aa;
	Node g1;
	
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
		s2 = new Node(1,"localhost:2181",Util.parseRingsArgument("2:PAL"));
		s3 = new Node(1,"localhost:2181",Util.parseRingsArgument("3:PAL"));
		s4 = new Node(1,"localhost:2181",Util.parseRingsArgument("4:PAL"));
		aa = new Node(2,"localhost:2181",Util.parseRingsArgument("1:A;2:A;3:A;4:A"));
		g1 = new Node(3,1,"localhost:2181",Util.parseRingsArgument("1:L"));
		s1.start();
		s2.start();
		s3.start();
		s4.start();
		aa.start();
		g1.start();
		Thread.sleep(6000); // wait until ring is fully started 
	}
	
	@After 
	public void close() throws Exception {
		s1.stop();
		s2.stop();
		s3.stop();
		s4.stop();
		aa.stop();
		g1.stop();
	}

	@Test
	public void multipleSubscribe() throws Exception {
		
		String s = "m1";
		s1.getProposer(1).propose(s.getBytes());
		s = "m2";
		s1.getProposer(1).propose(s.getBytes());

		Control c = new Control(1,ControlType.Subscribe,1,2);
		s1.getProposer(1).control(c);
		Thread.sleep(2000);		
		s2.getProposer(2).control(c);
		s = "m0"; // skipping manually
		s2.getProposer(2).propose(s.getBytes());
		s2.getProposer(2).propose(s.getBytes());

		s = "m3";
		s1.getProposer(1).propose(s.getBytes());
		s = "m4";
		s2.getProposer(2).propose(s.getBytes());

		c = new Control(2,ControlType.Subscribe,1,3);
		s1.getProposer(1).control(c);
		s2.getProposer(2).control(c);
		Thread.sleep(2000);		
		s3.getProposer(3).control(c);
		s = "m0"; // skipping manually
		s3.getProposer(3).propose(s.getBytes());
		s3.getProposer(3).propose(s.getBytes());
		s3.getProposer(3).propose(s.getBytes());
		s3.getProposer(3).propose(s.getBytes());
		
		s = "m5";
		s1.getProposer(1).propose(s.getBytes());
		s = "m6";
		s2.getProposer(2).propose(s.getBytes());
		s = "m7";
		s3.getProposer(3).propose(s.getBytes());

		Thread.sleep(2000);
		
		c = new Control(3,ControlType.Subscribe,1,4);
		s1.getProposer(1).control(c);
		s2.getProposer(2).control(c);
		s3.getProposer(3).control(c);
		Thread.sleep(2000);
		s4.getProposer(4).control(c);
		s = "m0"; // skipping manually
		s4.getProposer(4).propose(s.getBytes());
		s4.getProposer(4).propose(s.getBytes());
		s4.getProposer(4).propose(s.getBytes());
		s4.getProposer(4).propose(s.getBytes());
		s4.getProposer(4).propose(s.getBytes());
		s4.getProposer(4).propose(s.getBytes());		

		s = "m8";
		s1.getProposer(1).propose(s.getBytes());
		s = "m9";
		s2.getProposer(2).propose(s.getBytes());
		s = "m10";
		s3.getProposer(3).propose(s.getBytes());
		s = "m11";
		s4.getProposer(4).propose(s.getBytes());

		s = "m12";
		s1.getProposer(1).propose(s.getBytes());
		s = "m13";
		s2.getProposer(2).propose(s.getBytes());
		s = "m14";
		s3.getProposer(3).propose(s.getBytes());
		s = "m15";
		s4.getProposer(4).propose(s.getBytes());

		s = "m16";
		s1.getProposer(1).propose(s.getBytes());
		s = "m17";
		s2.getProposer(2).propose(s.getBytes());
		s = "m18";
		s3.getProposer(3).propose(s.getBytes());
		s = "m19";
		s4.getProposer(4).propose(s.getBytes());
				
		Thread.sleep(2000); // wait until everything is proposed
		
		System.err.println(format(g1.getLearner().getDecisions()));
		
		assertEquals(format(g1.getLearner().getDecisions()),"[m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13,m14,m15,m16,m17,m18,m19]");

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