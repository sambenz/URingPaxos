package ch.usi.da.paxos;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import ch.usi.da.paxos.api.StableStorage;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.storage.BerkeleyStorage;
import ch.usi.da.paxos.storage.BufferArray;
import ch.usi.da.paxos.storage.Decision;
import ch.usi.da.paxos.storage.InMemory;

public class TestStorage {

	Logger logger = Logger.getLogger("ch.usi.da");
	
	@Before 
	public void initialize() throws Exception {
		logger.setLevel(Level.ERROR);
	}

	@Test
	public void BufferArray() throws Exception {
		StableStorage db = new BufferArray();
		
		Decision d = new Decision(0,1L,42,new Value("id","value".getBytes()));		
		assertEquals(false,db.containsDecision(1L));
		
		db.putDecision(1L,d);		
		assertEquals(true,db.containsDecision(1L));
		assertEquals(d,db.getDecision(1L));

		assertEquals(false,db.containsBallot(1L));		
		db.putBallot(1L,100);
		assertEquals(true,db.containsBallot(1L));
		assertEquals(100,db.getBallot(1L));
		
		Decision d2 = new Decision(0,15001L,43,new Value("id","value".getBytes()));
		db.putDecision(15001L,d2);		
		assertEquals(false,db.containsDecision(1L));
		assertEquals(true,db.containsDecision(15001L));
		assertEquals(d2,db.getDecision(15001L));
		
		db.close();
	}

	@Test
	public void InMemory() throws Exception {
		StableStorage db = new InMemory();
		
		Decision d = new Decision(0,1L,42,new Value("id","value".getBytes()));
		assertEquals(false,db.containsDecision(1L));
		
		db.putDecision(1L,d);		
		assertEquals(true,db.containsDecision(1L));
		assertEquals(d,db.getDecision(1L));

		assertEquals(false,db.containsBallot(1L));		
		db.putBallot(1L,100);
		assertEquals(true,db.containsBallot(1L));
		assertEquals(100,db.getBallot(1L));
		
		db.close();
	}

	@Test
	public void BerkeleyStorage() throws Exception {
		File file = new File("/tmp/ringpaxos-db/0");
		file.mkdirs();
		
		BerkeleyStorage db = new BerkeleyStorage(file,false,false);

		Decision d = new Decision(0,1L,42,new Value("id","value".getBytes()));
		Decision d2 = new Decision(0,1L,43,new Value("id","value".getBytes()));
		//assertEquals(false,db.containsDecision(1L));
		db.putDecision(1L,d);
		db.putDecision(1L,d2);
		assertEquals(true,db.containsDecision(1L));
		assertEquals(d2,db.getDecision(1L));
		
		db.putDecision(2L,d);
		db.putDecision(3L,d);
		db.putDecision(4L,d);
		db.putDecision(5L,d);
		db.putDecision(6L,d);
		db.putDecision(7L,d);
		db.putDecision(8L,d);
		db.putDecision(9L,d);
		db.putDecision(10L,d);
		assertEquals(true,db.trim(7L));
		assertEquals(null,db.getDecision(6L));
		assertEquals(d,db.getDecision(7L));		
		
		assertEquals(false,db.containsBallot(1L));		
		db.putBallot(1L,100);
		assertEquals(true,db.containsBallot(1L));
		assertEquals(100,db.getBallot(1L));

		db.close();
		
		// re-open
		db = new BerkeleyStorage(file,false,false);
		assertEquals(d,db.getDecision(7L));		
		assertEquals(100,db.getBallot(1L));		
		db.close();

	}
}
