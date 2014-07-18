package ch.usi.da.paxos.message;
/* 
 * Copyright (c) 2013 Università della Svizzera italiana (USI)
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

/**
 * Name: Value<br>
 * Description: <br>
 * 
 * Creation date: Apr 11, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class Value implements Serializable {

	private static final long serialVersionUID = -8140500988041555392L;

	private final byte[] value;
	
	private final byte[] id;
	
	private final String ID;
	
	private final static String skipID  = "SKIP:";

	private final boolean batch;
	
	/**
	 * Public constructor
	 * 
	 * @param ID the value id
	 * @param value the bytes
	 * 
	 */
	public Value(String ID, byte[] value){
		this.ID = ID;
		this.id = ID.getBytes();
		this.value = value;
		this.batch = false;
	}

	/**
	 * Public constructor
	 * 
	 * @param ID the value id
	 * @param value the bytes
	 * @param batch is batch Value
	 * 
	 */
	public Value(String ID, byte[] value, boolean batch){
		this.ID = ID;
		this.id = ID.getBytes();
		this.value = value;
		this.batch = batch;
	}

	/**
	 * Get the ID
	 * 
	 * @return the ID
	 */
	public String getID(){
		return ID;
	}
	
	/**
	 * @return the ID in byte[]
	 */
	public byte[] getByteID(){
		return id;
	}
	
	/**
	 * @return the value
	 */
	public byte[] getValue(){
		return value;
	}
	
	public String toString(){
		if(isBatch()){
			return("<batch>");
		}else if(value.length == 0){
			return("<none> (" + ID + ")");
		}else if(new String(value).length()>40){
			return(new String(value).subSequence(0,39) + "... (" + ID + ")");
		}else{
			return(new String(value) + " (" + ID + ")");
		}
	}

	public String asString(){
		if(isBatch()){
			return("<batch>");
		}else if(value.length == 0){
			return("<none>");
		}else if(new String(value).length()>40){
			return(new String(value).subSequence(0,39) + "...");
		}else{
			return(new String(value));
		}
	}

	public boolean equals(Object obj) {
		if(obj instanceof Value){
            if(this.ID.equals(((Value) obj).getID())){
                    return true;
            }
		}
		return false;
	}
	
	public int hashCode() {
		return this.ID.hashCode();
	}

	public boolean isSkip() {
		return this.getID().startsWith(Value.skipID);
	}

	public boolean isBatch() {
		return this.batch;
	}

	public static String getSkipID(){
		return skipID + "" + System.currentTimeMillis();
	}
}
