package ch.usi.da.paxos.storage;

import ch.usi.da.paxos.api.StableStorage;
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
 * Name: SyncBerkeleyStorage<br>
 * Description: <br>
 * 
 * This class only exists that you can the accepters
 * can always call a default constructor!
 *  
 * Creation date: Apr 20, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz <benz@geoid.ch>
 */
public class SyncBerkeleyStorage implements StableStorage {

	private final BerkeleyStorage storage;
	
	public SyncBerkeleyStorage(){
		storage = new BerkeleyStorage(null,false,false);
	}

	@Override
	public void putBallot(Long instance, int ballot) {
		storage.putBallot(instance, ballot);		
	}

	@Override
	public int getBallot(Long instance) {
		return storage.getBallot(instance);
	}

	@Override
	public synchronized boolean containsBallot(Long instance) {
		return storage.containsBallot(instance);
	}
	
	@Override
	public void putDecision(Long instance, Decision decision) {
		storage.putDecision(instance, decision);
		
	}

	@Override
	public Decision getDecision(Long instance) {
		return storage.getDecision(instance);
	}

	@Override
	public boolean containsDecision(Long instance) {
		return storage.containsDecision(instance);
	}

	@Override
	public boolean trim(Long instance) {
		return storage.trim(instance);
	}

	@Override
	public Long getLastTrimInstance() {
		return storage.getLastTrimInstance();
	}
	
	@Override
	public void close() {
		storage.close();
	}

}
