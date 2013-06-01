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
 * @author Samuel Benz <benz@geoid.ch>
 */
public enum MessageType {
	/**
	 * 1a
	 */
	Prepare,
	/**
	 * 1a nack
	 */
	Nack,
	/**
	 * 1b
	 */
	Promise,
	/**
	 * 2a
	 */
	Accept,
	/**
	 * 2b
	 */
	Accepted,
	/**
	 * send a value to a leader
	 */
	Value,
	/**
	 * new type for RingPaxos (combines 1a/1b)
	 */
	Phase1,
	/**
	 * new type for RingPaxos (multi ballot reservation)
	 */
	Phase1Range,
	/**
	 * new type for RingPaxos (combines 2a/2b)
	 */
	Phase2,
	/**
	 * new type for RingPaxos (this msg type will sent to ALL roles)
	 */
	Decision
}
