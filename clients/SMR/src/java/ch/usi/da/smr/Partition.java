package ch.usi.da.smr;

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

/**
 * Name: Partition<br>
 * Description: <br>
 * 
 * Creation date: Aug 28, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Partition {

	private final String ID;

	private final int ring;
	
	private final int low;

	private final int high;

	public Partition(String ID, int ring, int low, int high){
		this.ID = ID.toUpperCase();
		this.ring = ring;
		this.low = low;
		this.high = high;
	}
	
	public String getID(){
		return ID;
	}

	public int getRing(){
		return ring;
	}
	
	public int getLow(){
		return low;
	}
	
	public int getHigh(){
		return high;
	}
	
	public String toString(){
		return ID + ": " + low + "->" + high + " (ring " + ring + ")";
	}
	
	public boolean equals(Object obj) {
		if(obj instanceof Partition && this.ID.equals(((Partition)obj).getID())){
			return true;
		}
		return false;
	}
}
