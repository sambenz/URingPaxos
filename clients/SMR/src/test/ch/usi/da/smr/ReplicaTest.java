package ch.usi.da.smr;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.message.Message;
import ch.usi.da.smr.transport.Receiver;
import ch.usi.da.smr.transport.UDPListener;

public class ReplicaTest implements Receiver {

	Logger logger = Logger.getLogger("ch.usi.da");
	List<Message> received;
	Replica replica;
	UDPListener udp;
	
	@Before 
	public void initialize() throws Exception {
		logger.setLevel(Level.FATAL);
		
		new File(Replica.state_file).delete();
		new File(Replica.snapshot_file).delete();
		
		replica = new Replica("0",1,1,0,"localhost:2181");
		replica.start();
		
		received = new ArrayList<Message>();
		udp = new UDPListener(1234);
		udp.registerReceiver(this);
		Thread t = new Thread(udp);
		t.start();
	}
	
	@After 
	public void close() {
        replica.close();
        udp.close();
	}

	@Test
	public void applyCmd() throws Exception {
		// put
		List<Command> cmd = new ArrayList<Command>();
		cmd.add(new Command(1, CommandType.PUT, "test", "Value".getBytes()));
		Message m = new Message(1,"127.0.0.1;1234","", cmd);
		m.setInstance(1);
		m.setRing(1);
		replica.receive(m);

		// get
		cmd = new ArrayList<Command>();
		cmd.add(new Command(2, CommandType.GET, "test",new byte[0]));
		m = new Message(2,"127.0.0.1;1234","", cmd);
		m.setInstance(2);
		m.setRing(1);
		replica.receive(m);

		// wait response
		Command response = null;
		for(int i=0;i<5;i++){
			Thread.sleep(1000);
			for(Message r : received){
				for(Command c : r.getCommands()){
					if(c.getID() == 2){
						response = c;
						break;
					}
				}
			}
		}
		assertEquals("Value",new String(response.getValue()));
	}

	@Test
	public void isReady() throws Exception {
		assertEquals(false,replica.is_ready(1,2L));
		assertEquals(true,replica.is_ready(1,1L));
	}

	@Test
	public void recovery() throws Exception {
		Replica replica2 = new Replica("0",1,2,0,"localhost:2181");
		replica2.start();
		
		// set state and checkpoint
		for(int i=1;i<11;i++){
			List<Command> cmd = new ArrayList<Command>();
			cmd.add(new Command(i, CommandType.PUT, "test" + i, "Value".getBytes()));
			Message m = new Message(1,"127.0.0.1;1234","", cmd);
			m.setRing(1);
			m.setInstance(i);
			replica.receive(m);
		}
		replica.checkpoint();
		
		// recover
		assertEquals(false,replica2.is_ready(1,2L)); // triggers recovery
		Thread.sleep(2000);
		assertEquals(true,replica2.is_ready(1,2L));
		assertEquals(true,replica2.is_ready(1,11L));
		assertEquals(false,replica2.is_ready(1,12L));		
		replica2.close();
	}

	@Override
	public void receive(Message m) {
		received.add(m);
	}

	@Override
	public boolean is_ready(Integer ring, Long instance) {
		return true;
	}
	
}
