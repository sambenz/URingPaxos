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
	public void put(Integer instance, Decision decision) {
		storage.put(instance, decision);
		
	}

	@Override
	public Decision get(Integer instance) {
		return storage.get(instance);
	}

	@Override
	public boolean contains(Integer instance) {
		return storage.contains(instance);
	}

	@Override
	public boolean trim(Integer instance) {
		return storage.trim(instance);
	}

	@Override
	public void close() {
		storage.close();
	}
}
