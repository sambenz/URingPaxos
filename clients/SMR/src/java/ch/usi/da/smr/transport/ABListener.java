package ch.usi.da.smr.transport;


public interface ABListener {

	void registerReceiver(Receiver receiver);

	void safe(int ring, long instance) throws Exception;
	
	void close();

}
