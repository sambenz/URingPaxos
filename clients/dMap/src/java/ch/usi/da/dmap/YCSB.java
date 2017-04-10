package ch.usi.da.dmap;
/* 
 * Copyright (c) 2017 Università della Svizzera italiana (USI)
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;

/**
 * Name: YCSB<br>
 * Description: <br>
 *
 * See https://github.com/brianfrankcooper/YCSB/wiki/Adding-a-Database
 * 
 * Creation date: April 10, 2017<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class YCSB extends DB {
	static {
		// get hostname and pid for log file name
		String host = "localhost";
		try {
			Process proc = Runtime.getRuntime().exec("hostname");
			BufferedInputStream in = new BufferedInputStream(proc.getInputStream());
			proc.waitFor();
			byte [] b = new byte[in.available()];
			in.read(b);
			in.close();
			host = new String(b).replace("\n","");
		} catch (IOException | InterruptedException e) {
		}
		int pid = 0;
		try {
			pid = Integer.parseInt((new File("/proc/self")).getCanonicalFile().getName());
		} catch (NumberFormatException | IOException e) {
		}
		System.setProperty("logfilename", "L" + host + "-" + pid + ".log");
	}


    private static final String HOST = "dmap.host";
    private static final String HOST_DEFAULT = "127.0.0.1:2181";
    private static final String MAP = "dmap.map";
    private static final String MAP_DEFAULT = "83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b";

    private boolean writeallfields = false;

    private SortedMap<String,HashMap<String, String>> dmap;
    
	@Override
	public void init() throws DBException {
		final String zookeeper = getProperties().getProperty(HOST, HOST_DEFAULT);
		final String mapID = getProperties().getProperty(MAP, MAP_DEFAULT);
		dmap = new DistributedOrderedMap<String,HashMap<String, String>>(mapID,zookeeper);
	    writeallfields = Boolean.parseBoolean(getProperties().getProperty(CoreWorkload.WRITE_ALL_FIELDS_PROPERTY,CoreWorkload.WRITE_ALL_FIELDS_PROPERTY_DEFAULT));
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		HashMap<String, String> r = dmap.get(key);
		if(r == null){
			return Status.NOT_FOUND;
		}else{
			if(fields != null){
				for(Entry<String, ByteIterator> e : StringByteIterator.getByteIteratorMap(r).entrySet()){
					if(fields.contains(e.getKey())){
						result.put(e.getKey(),e.getValue());
					}
				}
			}else{
				result.putAll(StringByteIterator.getByteIteratorMap(r));
			}
			return Status.OK;
		}
	}

	@Override
	public Status scan(String table, String startkey, int recordcount,	Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		for(Entry<String, HashMap<String, String>> e : dmap.tailMap(startkey).entrySet()){
			result.addElement(StringByteIterator.getByteIteratorMap(e.getValue()));
			if(result.size() == recordcount){
				break;
			}
		}
		return Status.OK;
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        if(!writeallfields) {
            HashMap<String, ByteIterator> oldval = new HashMap<String, ByteIterator>();
            read(table, key, null, oldval);
            for(String k : values.keySet()) {
                oldval.put(k, values.get(k));
            }
            values = oldval;
        }
		return insert(table, key, values);
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		dmap.put(key, StringByteIterator.getStringMap(values));
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		dmap.remove(key);
		return Status.OK;
	}
    
}
