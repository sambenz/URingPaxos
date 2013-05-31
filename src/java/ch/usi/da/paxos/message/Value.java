package ch.usi.da.paxos.message;

import java.io.Serializable;

import ch.usi.da.paxos.ring.NetworkManager;

/**
 * Name: Value<br>
 * Description: <br>
 * 
 * Creation date: Apr 11, 2012<br>
 * $Id$
 * 
 * @author benz@geoid.ch
 */
public class Value implements Serializable {

	private static final long serialVersionUID = -8140500988041555392L;

	private final byte[] value;
	
	private final byte[] id;
	
	private final String ID;
	
	public static final String skipID = "SKIP!";

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
		if(value.length == 0){
			return("<none> (" + ID + ")");
		}else if(new String(value).length()>40){
			return(new String(value).subSequence(0,39) + "... (" + ID + ")");
		}else if(ID.equals(skipID)){
			return(NetworkManager.byteToInt(value) + " (" + ID + ")");
		}else{
			return(new String(value) + " (" + ID + ")");
		}
	}

	public String asString(){
		if(value.length == 0){
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

}
