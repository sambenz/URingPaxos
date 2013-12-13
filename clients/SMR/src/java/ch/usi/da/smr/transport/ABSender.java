package ch.usi.da.smr.transport;

import ch.usi.da.smr.message.Message;

public interface ABSender {

	public long abroadcast(Message m);

	public void close();
	
}
