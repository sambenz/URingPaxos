package ch.usi.da.dlog.transport;

import ch.usi.da.dlog.message.Message;

public interface ABSender {

	public long abroadcast(Message m);

	public void close();
	
}
