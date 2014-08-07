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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import ch.usi.da.smr.message.Command;
import ch.usi.da.smr.message.CommandType;
import ch.usi.da.smr.transport.Response;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;

/**
 * Name: YCSB<br>
 * Description: <br>
 *
 * See https://github.com/brianfrankcooper/YCSB/wiki/Adding-a-Database
 * 
 * Creation date: Nov 15, 2013<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class YCSB extends DB {

    private static final String HOST = "zoo.host";
    private static final String HOST_DEFAULT = "127.0.0.1:2181";
    private static final String MAP = "smr.map";
    private static final String MAP_DEFAULT = "1,1;16,1";

	private PartitionManager partitions;
	private Client client;
	
	private AtomicInteger cmd_id = new AtomicInteger(0);
	
    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;
    
    private boolean writeallfields;

	@Override
	public void init() throws DBException {
		final Map<Integer,Integer> connectMap = Client.parseConnectMap(getProperties().getProperty(MAP, MAP_DEFAULT));
		try {
			partitions = new PartitionManager(getProperties().getProperty(HOST, HOST_DEFAULT));
			partitions.init();
			client = new Client(partitions,connectMap);
			client.init();
	        writeallfields = Boolean.parseBoolean(getProperties().getProperty(CoreWorkload.WRITE_ALL_FIELDS_PROPERTY, 
                    CoreWorkload.WRITE_ALL_FIELDS_PROPERTY_DEFAULT));
		} catch (Exception e) {
			throw new DBException(e);
		}
	}

	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    	Command cmd = new Command(cmd_id.incrementAndGet(),CommandType.GET,key,new byte[0]);
		try {
			Response r = client.send(cmd);
			if(r != null){
				List<Command> lc = r.getResponse(10000);
				if(lc.size() == 0){
					return NOT_FOUND;
				}else if(lc.get(0).getType() == CommandType.RESPONSE){
					if(lc.get(0).getValue() != null){
						decode(fields, new String(lc.get(0).getValue()), result); 
						return OK;
					}else{
						return NOT_FOUND;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ERROR;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,	Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    	Command cmd = new Command(cmd_id.incrementAndGet(),CommandType.GETRANGE,startkey,("").getBytes(),recordcount);
		try {
			Response r = client.send(cmd);
			if(r != null){
				List<Command> lc = r.getResponse(10000);
				if(lc.isEmpty()){ return ERROR; }
    			for(Command c : lc){
	    			if(c.getType() == CommandType.RESPONSE){
	    				HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
	    				tuple.put("key", new StringByteIterator(c.getKey()));
	    				if(c.getValue() != null && c.getValue().length > 0){
	    					decode(fields, new String(c.getValue()), tuple);
	    					result.add(tuple);
	    				}
	    			}			    				
    			}
				return OK;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ERROR;
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
        if(!writeallfields) {
            HashMap<String, ByteIterator> oldval = new HashMap<String, ByteIterator>();
            read(table, key, null, oldval);
            for(String k: values.keySet()) {
                oldval.put(k, values.get(k));
            }
            values = oldval;
        }
		return insert(table, key, values);
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    	Command cmd = new Command(cmd_id.incrementAndGet(),CommandType.PUT,key,encode(values).array());
		try {
			Response r = client.send(cmd);
			if(r != null){
				List<Command> lc = r.getResponse(10000);
				if(lc.isEmpty()){ return ERROR; }
				if(lc.get(0).getType() == CommandType.RESPONSE){
					return OK;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ERROR;
	}

	@Override
	public int delete(String table, String key) {
    	Command cmd = new Command(cmd_id.incrementAndGet(),CommandType.DELETE,key,new byte[0]);
		try {
			Response r = client.send(cmd);
			if(r != null){
				List<Command> lc = r.getResponse(10000);
				if(lc.isEmpty()){ return ERROR; }
				if(lc.get(0).getType() == CommandType.RESPONSE){
					return OK;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ERROR;
	}

	public Client getClient(){
		return client;
	}
	
	/* see MapKeeperClient.java */
    ByteBuffer encode(HashMap<String, ByteIterator> values) {
        int len = 0;
        for(String k : values.keySet()) {
            len += (k.length() + 1 + values.get(k).bytesLeft() + 1);
        }
        byte[] array = new byte[len];
        int i = 0;
        for(String k : values.keySet()) {
            for(int j = 0; j < k.length(); j++) {
                array[i] = (byte)k.charAt(j);
                i++;
            }
            array[i] = '\t';
            i++;
            ByteIterator v = values.get(k);
            i = v.nextBuf(array, i);
            array[i] = '\t';
            i++;
        }
        array[array.length-1] = 0;
        ByteBuffer buf = ByteBuffer.wrap(array);
        buf.rewind();
        return buf;
    }
    void decode(Set<String> fields, String tups, HashMap<String, ByteIterator> tup) {
        String[] tok = tups.split("\\t");
        if(tok.length == 0) { throw new IllegalStateException("split returned empty array!"); }
        for(int i = 0; i < tok.length; i+=2) {
            if(fields == null || fields.contains(tok[i])) {
                if(tok.length < i+2) { throw new IllegalStateException("Couldn't parse tuple <" + tups + "> at index " + i); }
                if(tok[i] == null || tok[i+1] == null) throw new NullPointerException("Key is " + tok[i] + " val is + " + tok[i+1]);
                tup.put(tok[i], new StringByteIterator(tok[i+1]));
            }
        }
        if(tok.length == 0) {
            System.err.println("Empty tuple: " + tups);
        }
    }
    
}
