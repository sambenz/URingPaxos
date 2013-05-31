package ch.usi.da.paxos.storage;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.PaxosRole;
import ch.usi.da.paxos.message.Value;

/**
 * Name: CyclicArray<br>
 * Description: <br>
 * 
 * Creation date: Apr 23, 2013<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class CyclicArray implements StableStorage {

	private final static Logger logger = Logger.getLogger(CyclicArray.class);
	
	private native int init();
	
	private native void nput(int i,byte[] b);
	
	private native byte[] nget(int i);
	
	public CyclicArray(){
		try{
			System.loadLibrary("paxos");
			this.init(); // native calloc
			logger.info("CyclicArray JNI native array enabled!");
		}catch(UnsatisfiedLinkError e){
			logger.error("CyclicArray init error:",e);
		}
	}

	@Override
	public void put(Integer instance, Decision decision) {
		//TODO: not optimal with this kind of serialization; but still fast ...
		Message m = new Message(instance, 0, PaxosRole.Proposer, MessageType.Value, decision.getBallot(), decision.getValue());
		nput(instance.intValue(),Message.toWire(m));
	}

	@Override
	public Decision get(Integer instance) {
		byte[] b = nget(instance.intValue());
		if(b.length > 0){
			Message m = Message.fromWire(b);
			if(m.getInstance() == instance){
				Decision d = new Decision(m.getInstance(),m.getBallot(),m.getValue());
				return d;
			}else{
				return null;
			}
		}
		return null;
	}

	@Override
	public native boolean contains(Integer instance);

	@Override
	public void close(){
		
	}
	
	/**
	 * Debug method
	 */
	public static void main(String[] args){
		CyclicArray db = new CyclicArray();
		Decision d = new Decision(1,42,new Value("id","value".getBytes()));
		Decision d2 = new Decision(15001,43,new Value("id","value".getBytes()));

		db.put(d.getInstance(),d);
		db.put(d2.getInstance(),d);
		d = null;
		d2 = null;		
		System.gc();

		System.out.println(db.get(1));
		System.out.println(db.get(15001));
		
		System.out.println(db.contains(1));	
		System.out.println(db.contains(15001));
	}
}
