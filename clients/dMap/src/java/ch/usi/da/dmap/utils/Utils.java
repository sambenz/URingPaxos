package ch.usi.da.dmap.utils;
/* 
 * Copyright (c) 2017 Università della Svizzera italiana (USI)
 * 
 * This file is part of URingPaxos.
 *
 * URingPaxos is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * URingPaxos is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with URingPaxos.  If not, see <http://www.gnu.org/licenses/>.
 */

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
