package ch.usi.da.smr.recovery;
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

import java.util.Map;
import java.util.SortedMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import ch.usi.da.smr.Replica;
import ch.usi.da.smr.transport.ABListener;

/**
 * Name: SnapshotWriter<br>
 * Description: <br>
 * 
 * Creation date: Nov 06, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class SnapshotWriter implements Runnable {

	private final Logger logger = Logger.getLogger(SnapshotWriter.class);

	private final Map<Integer, Long> exec_instance;
	
	private final SortedMap<String, byte[]> db;
	
	private final RecoveryInterface stable_storage;
	
	private final ABListener ab;
	
	private final Replica replica;
	
	public SnapshotWriter(Replica replica, Map<Integer, Long> exec_instance,SortedMap<String, byte[]> db, RecoveryInterface stable_storage,ABListener ab) {
		this.ab = ab;
		this.db = db;
		this.stable_storage = stable_storage;
		this.exec_instance = exec_instance;
		this.replica = replica;
	}

	@Override
	public void run() {
		if(stable_storage.storeState(exec_instance,db)){
			try {
				for(Entry<Integer, Long> e : exec_instance.entrySet()){
					ab.safe(e.getKey(),e.getValue());
				}
				logger.info("Replica checkpointed up to instance " + exec_instance);
			} catch (Exception e) {
				logger.error(e);
			}
			replica.setActiveSnapshot(false);
		}
	}

}
