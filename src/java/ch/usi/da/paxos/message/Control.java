package ch.usi.da.paxos.message;
/* 
 * Copyright (c) 2015 Università della Svizzera italiana (USI)
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

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Name: Control<br>
 * Description: <br>
 * 
 * Creation date: Oct 15, 2015<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Control implements Serializable {

	private static final long serialVersionUID = -7653083064242882623L;
	
	private final int id;
	
	private final ControlType type;
	
	private final int groupID;
	
	private final int ringID;
	
	public Control(int id, ControlType type, int groupID, int ringID){
		this.id = id;
		this.type = type;
		this.groupID = groupID;
		this.ringID = ringID;
	}
	
	public int getID(){
		return id;
	}

	public ControlType getType(){
		return type;
	}
	
	public boolean isPrepare(){
		if(type == ControlType.Prepare){
			return true;
		}
		return false;
	}
	
	public boolean isSubscribe(){
		if(type == ControlType.Subscribe){
			return true;
		}
		return false;
	}

	public boolean isUnsubscribe(){
		if(type == ControlType.Unsubscribe){
			return true;
		}
		return false;
	}

	public int getGroupID(){
		return groupID;
	}
	
	public int getRingID(){
		return ringID;
	}
	
	public String toString(){
		return "control id: " + id + " type: " + type + " in group " + groupID + " ring " + ringID;
	}

	public boolean equals(Object obj) {
		if(obj instanceof Control){
            if(this.id == ((Control) obj).getID() 
            		&& this.groupID == ((Control) obj).getGroupID() 
            		&& this.ringID == ((Control) obj).getRingID()
            		&& this.type.equals(((Control) obj).getType())){
                    return true;
            }
		}
		return false;
	}
	
	public int hashCode() {
		return new Long(this.id).hashCode();
	}
	
	/**
	 * An convenient way for wire representation 
	 * 
	 * @param c
	 * @return control byte array
	 */
	public static byte[] toWire(Control c){
		ByteBuffer buffer = ByteBuffer.allocate(8+2+4+8);
		// int  id
		// short type
		// int   group
		// long  position
		buffer.putInt(c.getID());
		buffer.putShort((short)c.getType().getId());
		buffer.putInt(c.getGroupID());
		buffer.putInt(c.getRingID());		
		return buffer.array();
	}

	/**
	 * This method is not really efficient; but convenient for std. IO
	 * 
	 * @param b
	 * @return Control object
	 */
	public static Control fromWire(byte[] b) {
		ByteBuffer buffer = ByteBuffer.wrap(b);
		try {
			int id = buffer.getInt();
			ControlType type = ControlType.fromId(buffer.getShort());
			int group = buffer.getInt();
			int ring = buffer.getInt();
			Control c = new Control(id,type,group,ring);
			return c;
		} catch (Exception e) {
			return null;
		}
	}
	
}
