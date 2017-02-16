package ch.usi.da.dmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class Utils {
	
	public static ByteBuffer getBuffer(Object o) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		out = new ObjectOutputStream(bos);   
		out.writeObject(o);
		out.flush();
		ByteBuffer ret = ByteBuffer.wrap(bos.toByteArray());
		bos.close();
		return ret;
	}

	public static Object getObject(byte[] obj) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(obj);
		ObjectInput in = null;
		in = new ObjectInputStream(bis);
		Object ret = in.readObject();
		in.close();
		bis.close();
		return ret;
	}
}
