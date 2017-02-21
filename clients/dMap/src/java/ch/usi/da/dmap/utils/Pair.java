package ch.usi.da.dmap.utils;
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

import java.util.Map.Entry;


/**
 * Name: Pair<br>
 * Description: <br>
 * 
 * Creation date: Jan 28, 2017<br>
 * $Id$
 * 
 * Notes:
 * Used to serialize a list of pairs over Thrift.
 *
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class Pair<K,V> implements java.io.Serializable, Entry<K,V>{

	private static final long serialVersionUID = 3821699802992791854L;

	private final K key;
	private V value;
	
	public Pair(K key, V value){
		this.key = key;
		this.value = value;
	}

	@Override
	public K getKey(){
		return key;
	}
	
	@Override
	public V getValue(){
		return value;
	}

	@Override
	public V setValue(V value) {
		V old = value;
		this.value = value;
		return old;
	}
	
	@Override
    public boolean equals(Object o) {
        if (!(o instanceof Pair))
            return false;
        Pair<?,?> e = (Pair<?,?>)o;

        return valEquals(key,e.getKey()) && valEquals(value,e.getValue());
    }
	
    boolean valEquals(Object o1, Object o2) {
        return (o1==null ? o2==null : o1.equals(o2));
    }
	
	@Override
    public int hashCode() {
        int keyHash = (key==null ? 0 : key.hashCode());
        int valueHash = (value==null ? 0 : value.hashCode());
        return keyHash ^ valueHash;
    }

	@Override
	public String toString(){
		return key + "=" + value;
	}

}
