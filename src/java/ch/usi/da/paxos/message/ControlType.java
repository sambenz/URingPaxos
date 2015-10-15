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

/**
 * Name: ControlType<br>
 * Description: <br>
 * 
 * Creation date: Oct 15, 2015<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public enum ControlType {
	/**
	 * prepare a subscribe
	 */
	Prepare(0),
	/**
	 * subscribe
	 */
	Subscribe(1),
	/**
	 * unsubscribe
	 */
	Unsubscribe(2);
	
	private final int id;
	
	private ControlType(int id) {
		this.id = id;
	}
	
	public int getId() {
		return id;
	}
	
	public static ControlType fromId(int id) {
		for (ControlType t: values()){
			if (t.id == id) { return t; }
		}
		throw new RuntimeException("ControlType " + id + " does not exist!");
	}
}
