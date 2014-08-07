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

/**
 * Name: MessageType<br>
 * Description: <br>
 * 
 * Creation date: Mar 31, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public enum MessageType {
	/**
	 * 1a
	 */
	Prepare(0),
	/**
	 * 1a nack
	 */
	Nack(1),
	/**
	 * 1b
	 */
	Promise(2),
	/**
	 * 2a
	 */
	Accept(3),
	/**
	 * 2b
	 */
	Accepted(4),
	/**
	 * send a value to a leader
	 */
	Value(5),
	/**
	 * new type for RingPaxos (combines 1a/1b)
	 */
	Phase1(6),
	/**
	 * new type for RingPaxos (multi ballot reservation)
	 */
	Phase1Range(7),
	/**
	 * new type for RingPaxos (combines 2a/2b)
	 */
	Phase2(8),
	/**
	 * new type for RingPaxos (this msg type will sent to ALL roles)
	 */
	Decision(9),
	/**
	 * new type for RingPaxos (learners to coord.)
	 */	
	Safe(10),
	/**
	 * new type for RingPaxos (coord. to accptors)
	 */
	Trim(11),
	/**
	 * new type for RingPaxos (learners to coord.)
	 */
	Relearn(12),
	/**
	 * new type for RingPaxos (learners to coord.)
	 */
	Latency(13);

	
	private final int id;
	
	private MessageType(int id) {
		this.id = id;
	}
	
	public int getId() {
		return id;
	}
	
	public static MessageType fromId(int id) {
		for (MessageType t: values()){
			if (t.id == id) { return t; }
		}
		throw new RuntimeException("MessageType " + id + " does not exist!");
	}
}
