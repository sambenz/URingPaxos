package ch.usi.da.dmap;

/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * This file is available under and governed by the GNU General Public
 * License version 2 only, as published by the Free Software Foundation.
 * However, the following notice accompanied the original version of this
 * file:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/0/1.0/
 */
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

import org.junit.Test;

@SuppressWarnings({"rawtypes","unchecked"})
public class DmapTest {

	/**
	 * Returns a new map from Integers 1-5 to Strings "A"-"E".
	 */
	private static DistributedOrderedMap map5() {
		DistributedOrderedMap map = new DistributedOrderedMap("83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b","127.0.0.1:2181");
		map.clear();
		assertTrue(map.isEmpty());
		map.put(1, "A");
		map.put(5, "E");
		map.put(3, "C");
		map.put(2, "B");
		map.put(4, "D");
		assertFalse(map.isEmpty());
		assertEquals(5, map.size());
		return map;
	}

	/**
	 * clear removes all pairs
	 */
	@Test
	public void testClear() {
		DistributedOrderedMap map = map5();
		map.clear();
		assertEquals(0, map.size());
	}

	/**
	 * copy constructor creates map equal to source map
	 */
//  @Test
//	public void testConstructFromSorted() {
//		DistributedOrderedMap map = map5();
//		DistributedOrderedMap map2 = new DistributedOrderedMap(map);
//		assertEquals(map, map2);
//	}

	/**
	 * Maps with same contents are equal
	 */
	@Test
	public void testEquals() {
		DistributedOrderedMap map1 = map5();
		DistributedOrderedMap map2 = map5();
		assertEquals(map1, map2);
		assertEquals(map2, map1);
//		map1.clear();
//		assertFalse(map1.equals(map2));
//		assertFalse(map2.equals(map1));
	}

	/**
	 * containsKey returns true for contained key
	 */
	@Test
	public void testContainsKey() {
		DistributedOrderedMap map = map5();
		assertTrue(map.containsKey(1));
		assertFalse(map.containsKey(0));
	}

	/**
	 * containsValue returns true for held values
	 */
	@Test
	public void testContainsValue() {
		DistributedOrderedMap map = map5();
		assertTrue(map.containsValue("A"));
		assertFalse(map.containsValue("Z"));
	}

	/**
	 * get returns the correct element at the given key,
	 * or null if not present
	 */
	@Test
	public void testGet() {
		DistributedOrderedMap map = map5();
		assertEquals("A", (String)map.get(1));
		DistributedOrderedMap empty = new DistributedOrderedMap("83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b","127.0.0.1:2181");
		empty.clear();
		assertNull(empty.get(1));
	}

	/**
	 * isEmpty is true of empty map and false for non-empty
	 */
	@Test
	public void testIsEmpty() {
		DistributedOrderedMap empty = new DistributedOrderedMap("83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b","127.0.0.1:2181");
		empty.clear();
		assertTrue(empty.isEmpty());		
		DistributedOrderedMap map = map5();
		assertFalse(map.isEmpty());
	}

	/**
	 * firstKey returns first key
	 */
	@Test
	public void testFirstKey() {
		DistributedOrderedMap map = map5();
		assertEquals(1, map.firstKey());
	}

	/**
	 * lastKey returns last key
	 */
	@Test
	public void testLastKey() {
		DistributedOrderedMap map = map5();
		assertEquals(5, map.lastKey());
	}

	/**
	 * keySet.toArray returns contains all keys
	 */
	@Test
	public void testKeySetToArray() {
		DistributedOrderedMap map = map5();
		Set s = map.keySet();
		Object[] ar = s.toArray();
		assertTrue(s.containsAll(Arrays.asList(ar)));
		assertEquals(5, ar.length);
		ar[0] = 10;
		assertFalse(s.containsAll(Arrays.asList(ar)));
	}

	/**
	 * descendingkeySet.toArray returns contains all keys
	 */
//	@Test
//	public void testDescendingKeySetToArray() {
//		DistributedOrderedMap map = map5();
//		Set s = map.descendingKeySet();
//		Object[] ar = s.toArray();
//		assertEquals(5, ar.length);
//		assertTrue(s.containsAll(Arrays.asList(ar)));
//		ar[0] = 10;
//		assertFalse(s.containsAll(Arrays.asList(ar)));
//	}

	/**
	 * keySet returns a Set containing all the keys
	 */
	@Test
	public void testKeySet() {
		DistributedOrderedMap map = map5();
		Set s = map.keySet();
		assertEquals(5, s.size());
		assertTrue(s.contains(1));
		assertTrue(s.contains(2));
		assertTrue(s.contains(3));
		assertTrue(s.contains(4));
		assertTrue(s.contains(5));
	}

	/**
	 * keySet is ordered
	 */
	@Test
	public void testKeySetOrder() {
		DistributedOrderedMap map = map5();
		Set s = map.keySet();
		Iterator i = s.iterator();
		Integer last = (Integer)i.next();
		assertEquals(last,new Integer(1));
		int count = 1;
		while (i.hasNext()) {
			Integer k = (Integer)i.next();
			assertTrue(last.compareTo(k) < 0);
			last = k;
			++count;
		}
		assertEquals(5, count);
	}

	/**
	 * descending iterator of key set is inverse ordered
	 */
//	@Test
//	public void testKeySetDescendingIteratorOrder() {
//		DistributedOrderedMap map = map5();
//		NavigableSet s = map.navigableKeySet();
//		Iterator i = s.descendingIterator();
//		Integer last = (Integer)i.next();
//		assertEquals(last,new Integer(5));
//		int count = 1;
//		while (i.hasNext()) {
//			Integer k = (Integer)i.next();
//			assertTrue(last.compareTo(k) > 0);
//			last = k;
//			++count;
//		}
//		assertEquals(5, count);
//	}

	/**
	 * descendingKeySet is ordered
	 */
//	@Test
//	public void testDescendingKeySetOrder() {
//		DistributedOrderedMap map = map5();
//		Set s = map.descendingKeySet();
//		Iterator i = s.iterator();
//		Integer last = (Integer)i.next();
//		assertEquals(last,new Integer(5));
//		int count = 1;
//		while (i.hasNext()) {
//			Integer k = (Integer)i.next();
//			assertTrue(last.compareTo(k) > 0);
//			last = k;
//			++count;
//		}
//		assertEquals(5, count);
//	}

	/**
	 * descending iterator of descendingKeySet is ordered
	 */
//	@Test
//	public void testDescendingKeySetDescendingIteratorOrder() {
//		DistributedOrderedMap map = map5();
//		NavigableSet s = map.descendingKeySet();
//		Iterator i = s.descendingIterator();
//		Integer last = (Integer)i.next();
//		assertEquals(last,new Integer(1));
//		int count = 1;
//		while (i.hasNext()) {
//			Integer k = (Integer)i.next();
//			assertTrue(last.compareTo(k) < 0);
//			last = k;
//			++count;
//		}
//		assertEquals(5, count);
//	}

	/**
	 * values collection contains all values
	 */
	@Test
	public void testValues() {
		DistributedOrderedMap map = map5();
		Collection s = map.values();
		assertEquals(5, s.size());
		assertTrue(s.contains("A"));
		assertTrue(s.contains("B"));
		assertTrue(s.contains("C"));
		assertTrue(s.contains("D"));
		assertTrue(s.contains("E"));
	}

	/**
	 * entrySet contains all pairs
	 */
	@Test
	public void testEntrySet() {
		DistributedOrderedMap map = map5();
		Set s = map.entrySet();
		assertEquals(5, s.size());
		Iterator it = s.iterator();
		while (it.hasNext()) {
			Map.Entry e = (Map.Entry) it.next();
			assertTrue(
					(e.getKey().equals(1) && e.getValue().equals("A")) ||
					(e.getKey().equals(2) && e.getValue().equals("B")) ||
					(e.getKey().equals(3) && e.getValue().equals("C")) ||
					(e.getKey().equals(4) && e.getValue().equals("D")) ||
					(e.getKey().equals(5) && e.getValue().equals("E")));
		}
	}

	/**
	 * descendingEntrySet contains all pairs
	 */
//	@Test
//	public void testDescendingEntrySet() {
//		DistributedOrderedMap map = map5();
//		Set s = map.descendingMap().entrySet();
//		assertEquals(5, s.size());
//		Iterator it = s.iterator();
//		while (it.hasNext()) {
//			Map.Entry e = (Map.Entry) it.next();
//			assertTrue(
//					(e.getKey().equals(1) && e.getValue().equals("A")) ||
//					(e.getKey().equals(2) && e.getValue().equals("B")) ||
//					(e.getKey().equals(3) && e.getValue().equals("C")) ||
//					(e.getKey().equals(4) && e.getValue().equals("D")) ||
//					(e.getKey().equals(5) && e.getValue().equals("E")));
//		}
//	}

	/**
	 * entrySet.toArray contains all entries
	 */
	@Test
	public void testEntrySetToArray() {
		DistributedOrderedMap map = map5();
		Set s = map.entrySet();
		Object[] ar = s.toArray();
		assertEquals(5, ar.length);
		for (int i = 0; i < 5; ++i) {
			assertTrue(map.containsKey(((Map.Entry)(ar[i])).getKey()));
			assertTrue(map.containsValue(((Map.Entry)(ar[i])).getValue()));
		}
	}

	/**
	 * descendingEntrySet.toArray contains all entries
	 */
//	@Test
//	public void testDescendingEntrySetToArray() {
//		DistributedOrderedMap map = map5();
//		Set s = map.descendingMap().entrySet();
//		Object[] ar = s.toArray();
//		assertEquals(5, ar.length);
//		for (int i = 0; i < 5; ++i) {
//			assertTrue(map.containsKey(((Map.Entry)(ar[i])).getKey()));
//			assertTrue(map.containsValue(((Map.Entry)(ar[i])).getValue()));
//		}
//	}

	/**
	 * putAll adds all key-value pairs from the given map
	 */
	@Test
	public void testPutAll() {
		DistributedOrderedMap empty = new DistributedOrderedMap("83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b","127.0.0.1:2181");
		DistributedOrderedMap map = map5();
		empty.putAll(map);
		assertEquals(5, empty.size());
		assertTrue(empty.containsKey(1));
		assertTrue(empty.containsKey(2));
		assertTrue(empty.containsKey(3));
		assertTrue(empty.containsKey(4));
		assertTrue(empty.containsKey(5));
	}

	/**
	 * remove removes the correct key-value pair from the map
	 */
	@Test
	public void testRemove() {
		DistributedOrderedMap map = map5();
		map.remove(5);
		assertEquals(4, map.size());
		assertFalse(map.containsKey(5));
	}


	/**
	 * size returns the correct values
	 */
	@Test
	public void testSize() {
		DistributedOrderedMap map = map5();
		assertEquals(5, map.size());
	}

	@Test
	public void testToString() {
		DistributedOrderedMap map = map5();
		String s = map.toString();
		assertTrue(!s.isEmpty());
	}

	// Exception tests

	/**
	 * get(null) of n1mpty map throws NPE
	 */
	@Test
	public void testGet_NullPointerException() {
		DistributedOrderedMap c = map5();
		try {
			c.get(null);
			shouldThrow();
		} catch (NullPointerException success) {}
	}

	/**
	 * containsKey(null) of n1mpty map throws NPE
	 */
	@Test
	public void testContainsKey_NullPointerException() {
		DistributedOrderedMap c = map5();
		try {
			c.containsKey(null);
			shouldThrow();
		} catch (NullPointerException success) {}
	}

	/**
	 * remove(null) throws NPE for n1mpty map
	 */
	@Test
	public void testRemove1_NullPointerException() {
		DistributedOrderedMap c = new DistributedOrderedMap("83a8c1c0-dcb2-4afa-a447-07f79a0fcd6b","127.0.0.1:2181");
		c.put("sadsdf", "asdads");
		try {
			c.remove(null);
			shouldThrow();
		} catch (NullPointerException success) {}
	}

	/**
	 * subMap returns map with keys in requested range
	 */
	@Test
	public void testSubMapContents() {
		DistributedOrderedMap map = map5();
		SortedMap sm = map.subMap(2,4);
		assertEquals(2, sm.firstKey());
		assertEquals(3, sm.lastKey());
		assertEquals(2, sm.size());
		assertFalse(sm.containsKey(1));
		assertTrue(sm.containsKey(2));
		assertTrue(sm.containsKey(3));
		assertFalse(sm.containsKey(4));
		assertFalse(sm.containsKey(5));
		Iterator i = sm.keySet().iterator();
		Object k;
		k = (Integer)(i.next());
		assertEquals(2, k);
		k = (Integer)(i.next());
		assertEquals(3, k);
		assertFalse(i.hasNext());

//		TODO: disabled submap operations
//		Iterator j = sm.keySet().iterator();
//		j.next();
//		j.remove();
//		assertFalse(map.containsKey(2));
//		assertEquals(4, map.size());
//		assertEquals(1, sm.size());
//		assertEquals(3, sm.firstKey());
//		assertEquals(3, sm.lastKey());
//		assertEquals("C", sm.remove(3));
//		assertTrue(sm.isEmpty());
//		assertEquals(3, map.size());
	}

	@Test
	public void testSubMapContents2() {
		DistributedOrderedMap map = map5();
		SortedMap sm = map.subMap(2,3);
		assertEquals(1, sm.size());
		assertEquals(2, sm.firstKey());
		assertEquals(2, sm.lastKey());
		assertFalse(sm.containsKey(1));
		assertTrue(sm.containsKey(2));
		assertFalse(sm.containsKey(3));
		assertFalse(sm.containsKey(4));
		assertFalse(sm.containsKey(5));
		Iterator i = sm.keySet().iterator();
		Object k;
		k = (Integer)(i.next());
		assertEquals(2, k);
		assertFalse(i.hasNext());

//		TODO: disabled submap operations
//		Iterator j = sm.keySet().iterator();
//		j.next();
//		j.remove();
//		assertFalse(map.containsKey(2));
//		assertEquals(4, map.size());
//		assertEquals(0, sm.size());
//		assertTrue(sm.isEmpty());
//		assertSame(sm.remove(3), null);
//		assertEquals(4, map.size());
	}

	/**
	 * headMap returns map with keys in requested range
	 */
	@Test
	public void testHeadMapContents() {
		DistributedOrderedMap map = map5();
		SortedMap sm = map.headMap(4);
		assertTrue(sm.containsKey(1));
		assertTrue(sm.containsKey(2));
		assertTrue(sm.containsKey(3));
		assertFalse(sm.containsKey(4));
		assertFalse(sm.containsKey(5));
		Iterator i = sm.keySet().iterator();
		Object k;
		k = (Integer)(i.next());
		assertEquals(1, k);
		k = (Integer)(i.next());
		assertEquals(2, k);
		k = (Integer)(i.next());
		assertEquals(3, k);
		assertFalse(i.hasNext());
//		TODO: disabled submap operations
//		sm.clear();
//		assertTrue(sm.isEmpty());
//		assertEquals(2, map.size());
//		assertEquals(4, map.firstKey());
	}

	/**
	 * headMap returns map with keys in requested range
	 */
	@Test
	public void testTailMapContents() {
		DistributedOrderedMap map = map5();
		SortedMap sm = map.tailMap(2);
		assertFalse(sm.containsKey(1));
		assertTrue(sm.containsKey(2));
		assertTrue(sm.containsKey(3));
		assertTrue(sm.containsKey(4));
		assertTrue(sm.containsKey(5));
		Iterator i = sm.keySet().iterator();
		Object k;
		k = (Integer)(i.next());
		assertEquals(2, k);
		k = (Integer)(i.next());
		assertEquals(3, k);
		k = (Integer)(i.next());
		assertEquals(4, k);
		k = (Integer)(i.next());
		assertEquals(5, k);
		assertFalse(i.hasNext());

		Iterator ei = sm.entrySet().iterator();
		Map.Entry e;
		e = (Map.Entry)(ei.next());
		assertEquals(2, e.getKey());
		assertEquals("B", e.getValue());
		e = (Map.Entry)(ei.next());
		assertEquals(3, e.getKey());
		assertEquals("C", e.getValue());
		e = (Map.Entry)(ei.next());
		assertEquals(4, e.getKey());
		assertEquals("D", e.getValue());
		e = (Map.Entry)(ei.next());
		assertEquals(5, e.getKey());
		assertEquals("E", e.getValue());
		assertFalse(i.hasNext());

		SortedMap ssm = sm.tailMap(4);
		assertEquals(4, ssm.firstKey());
		assertEquals(5, ssm.lastKey());
//		TODO: disabled submap operations
//		assertEquals("D", ssm.remove(4));
//		assertEquals(1, ssm.size());
//		assertEquals(3, sm.size());
//		assertEquals(4, map.size());
	}

	Random rnd = new Random(666);
	BitSet bs;

	/**
	 * Submaps of submaps subdivide correctly
	 */
//	@Test
//	public void testRecursiveSubMaps() throws Exception {
//		int mapSize = 100;
//		Class cl = DistributedOrderedMap.class;
//		NavigableMap<Integer, Integer> map = newMap(cl);
//		bs = new BitSet(mapSize);
//
//		populate(map, mapSize);
//		check(map,                 0, mapSize - 1, true);
//		check(map.descendingMap(), 0, mapSize - 1, false);
//
//		mutateMap(map, 0, mapSize - 1);
//		check(map,                 0, mapSize - 1, true);
//		check(map.descendingMap(), 0, mapSize - 1, false);
//
//		bashSubMap(map.subMap(0, true, mapSize, false),
//				0, mapSize - 1, true);
//	}

//	static NavigableMap<Integer, Integer> newMap(Class cl) throws Exception {
//		NavigableMap<Integer, Integer> result
//		= (NavigableMap<Integer, Integer>) cl.newInstance();
//		assertEquals(0, result.size());
//		assertFalse(result.keySet().iterator().hasNext());
//		return result;
//	}

//	void populate(NavigableMap<Integer, Integer> map, int limit) {
//		for (int i = 0, n = 2 * limit / 3; i < n; i++) {
//			int key = rnd.nextInt(limit);
//			put(map, key);
//		}
//	}

//	void mutateMap(NavigableMap<Integer, Integer> map, int min, int max) {
//		int size = map.size();
//		int rangeSize = max - min + 1;
//
//		// Remove a bunch of entries directly
//		for (int i = 0, n = rangeSize / 2; i < n; i++) {
//			remove(map, min - 5 + rnd.nextInt(rangeSize + 10));
//		}
//
//		// Remove a bunch of entries with iterator
//		for (Iterator<Integer> it = map.keySet().iterator(); it.hasNext(); ) {
//			if (rnd.nextBoolean()) {
//				bs.clear(it.next());
//				it.remove();
//			}
//		}
//
//		// Add entries till we're back to original size
//		while (map.size() < size) {
//			int key = min + rnd.nextInt(rangeSize);
//			assertTrue(key >= min && key <= max);
//			put(map, key);
//		}
//	}

//	void mutateSubMap(NavigableMap<Integer, Integer> map, int min, int max) {
//		int size = map.size();
//		int rangeSize = max - min + 1;
//
//		// Remove a bunch of entries directly
//		for (int i = 0, n = rangeSize / 2; i < n; i++) {
//			remove(map, min - 5 + rnd.nextInt(rangeSize + 10));
//		}
//
//		// Remove a bunch of entries with iterator
//		for (Iterator<Integer> it = map.keySet().iterator(); it.hasNext(); ) {
//			if (rnd.nextBoolean()) {
//				bs.clear(it.next());
//				it.remove();
//			}
//		}
//
//		// Add entries till we're back to original size
//		while (map.size() < size) {
//			int key = min - 5 + rnd.nextInt(rangeSize + 10);
//			if (key >= min && key <= max) {
//				put(map, key);
//			} else {
//				try {
//					map.put(key, 2 * key);
//					shouldThrow();
//				} catch (IllegalArgumentException success) {}
//			}
//		}
//	}

//	void put(NavigableMap<Integer, Integer> map, int key) {
//		if (map.put(key, 2 * key) == null)
//			bs.set(key);
//	}

//	void remove(NavigableMap<Integer, Integer> map, int key) {
//		if (map.remove(key) != null)
//			bs.clear(key);
//	}

//	void bashSubMap(NavigableMap<Integer, Integer> map,
//			int min, int max, boolean ascending) {
//		check(map, min, max, ascending);
//		check(map.descendingMap(), min, max, !ascending);
//
//		mutateSubMap(map, min, max);
//		check(map, min, max, ascending);
//		check(map.descendingMap(), min, max, !ascending);
//
//		// Recurse
//		if (max - min < 2)
//			return;
//		int midPoint = (min + max) / 2;
//
//		// headMap - pick direction and endpoint inclusion randomly
//		boolean incl = rnd.nextBoolean();
//		NavigableMap<Integer,Integer> hm = map.headMap(midPoint, incl);
//		if (ascending) {
//			if (rnd.nextBoolean())
//				bashSubMap(hm, min, midPoint - (incl ? 0 : 1), true);
//			else
//				bashSubMap(hm.descendingMap(), min, midPoint - (incl ? 0 : 1),
//						false);
//		} else {
//			if (rnd.nextBoolean())
//				bashSubMap(hm, midPoint + (incl ? 0 : 1), max, false);
//			else
//				bashSubMap(hm.descendingMap(), midPoint + (incl ? 0 : 1), max,
//						true);
//		}
//
//		// tailMap - pick direction and endpoint inclusion randomly
//		incl = rnd.nextBoolean();
//		NavigableMap<Integer,Integer> tm = map.tailMap(midPoint,incl);
//		if (ascending) {
//			if (rnd.nextBoolean())
//				bashSubMap(tm, midPoint + (incl ? 0 : 1), max, true);
//			else
//				bashSubMap(tm.descendingMap(), midPoint + (incl ? 0 : 1), max,
//						false);
//		} else {
//			if (rnd.nextBoolean()) {
//				bashSubMap(tm, min, midPoint - (incl ? 0 : 1), false);
//			} else {
//				bashSubMap(tm.descendingMap(), min, midPoint - (incl ? 0 : 1),
//						true);
//			}
//		}
//
//		// subMap - pick direction and endpoint inclusion randomly
//		int rangeSize = max - min + 1;
//		int[] endpoints = new int[2];
//		endpoints[0] = min + rnd.nextInt(rangeSize);
//		endpoints[1] = min + rnd.nextInt(rangeSize);
//		Arrays.sort(endpoints);
//		boolean lowIncl = rnd.nextBoolean();
//		boolean highIncl = rnd.nextBoolean();
//		if (ascending) {
//			NavigableMap<Integer,Integer> sm = map.subMap(
//					endpoints[0], lowIncl, endpoints[1], highIncl);
//			if (rnd.nextBoolean())
//				bashSubMap(sm, endpoints[0] + (lowIncl ? 0 : 1),
//						endpoints[1] - (highIncl ? 0 : 1), true);
//			else
//				bashSubMap(sm.descendingMap(), endpoints[0] + (lowIncl ? 0 : 1),
//						endpoints[1] - (highIncl ? 0 : 1), false);
//		} else {
//			NavigableMap<Integer,Integer> sm = map.subMap(
//					endpoints[1], highIncl, endpoints[0], lowIncl);
//			if (rnd.nextBoolean())
//				bashSubMap(sm, endpoints[0] + (lowIncl ? 0 : 1),
//						endpoints[1] - (highIncl ? 0 : 1), false);
//			else
//				bashSubMap(sm.descendingMap(), endpoints[0] + (lowIncl ? 0 : 1),
//						endpoints[1] - (highIncl ? 0 : 1), true);
//		}
//	}

	/**
	 * min and max are both inclusive.  If max < min, interval is empty.
	 */
//	void check(NavigableMap<Integer, Integer> map,
//			final int min, final int max, final boolean ascending) {
//		class ReferenceSet {
//			int lower(int key) {
//				return ascending ? lowerAscending(key) : higherAscending(key);
//			}
//			int floor(int key) {
//				return ascending ? floorAscending(key) : ceilingAscending(key);
//			}
//			int ceiling(int key) {
//				return ascending ? ceilingAscending(key) : floorAscending(key);
//			}
//			int higher(int key) {
//				return ascending ? higherAscending(key) : lowerAscending(key);
//			}
//			int first() {
//				return ascending ? firstAscending() : lastAscending();
//			}
//			int last() {
//				return ascending ? lastAscending() : firstAscending();
//			}
//			int lowerAscending(int key) {
//				return floorAscending(key - 1);
//			}
//			int floorAscending(int key) {
//				if (key < min)
//					return -1;
//				else if (key > max)
//					key = max;
//
//				// BitSet should support this! Test would run much faster
//				while (key >= min) {
//					if (bs.get(key))
//						return key;
//					key--;
//				}
//				return -1;
//			}
//			int ceilingAscending(int key) {
//				if (key < min)
//					key = min;
//				else if (key > max)
//					return -1;
//				int result = bs.nextSetBit(key);
//				return result > max ? -1 : result;
//			}
//			int higherAscending(int key) {
//				return ceilingAscending(key + 1);
//			}
//			private int firstAscending() {
//				int result = ceilingAscending(min);
//				return result > max ? -1 : result;
//			}
//			private int lastAscending() {
//				int result = floorAscending(max);
//				return result < min ? -1 : result;
//			}
//		}
//		ReferenceSet rs = new ReferenceSet();
//
//		// Test contents using containsKey
//		int size = 0;
//		for (int i = min; i <= max; i++) {
//			boolean bsContainsI = bs.get(i);
//			assertEquals(bsContainsI, map.containsKey(i));
//			if (bsContainsI)
//				size++;
//		}
//		assertEquals(size, map.size());
//
//		// Test contents using contains keySet iterator
//		int size2 = 0;
//		int previousKey = -1;
//		for (int key : map.keySet()) {
//			assertTrue(bs.get(key));
//			size2++;
//			assertTrue(previousKey < 0 ||
//					(ascending ? key - previousKey > 0 : key - previousKey < 0));
//			previousKey = key;
//		}
//		assertEquals(size2, size);
//
//		// Test navigation ops
//		for (int key = min - 1; key <= max + 1; key++) {
//			assertEq(map.lowerKey(key), rs.lower(key));
//			assertEq(map.floorKey(key), rs.floor(key));
//			assertEq(map.higherKey(key), rs.higher(key));
//			assertEq(map.ceilingKey(key), rs.ceiling(key));
//		}
//
//		// Test extrema
//		if (map.size() != 0) {
//			assertEq(map.firstKey(), rs.first());
//			assertEq(map.lastKey(), rs.last());
//		} else {
//			assertEq(rs.first(), -1);
//			assertEq(rs.last(),  -1);
//			try {
//				map.firstKey();
//				shouldThrow();
//			} catch (NoSuchElementException success) {}
//			try {
//				map.lastKey();
//				shouldThrow();
//			} catch (NoSuchElementException success) {}
//		}
//	}

	static void shouldThrow(){
		assertTrue(false);
	}
	
	static void assertEq(Integer i, int j) {
		if (i == null)
			assertEquals(j, -1);
		else
			assertEquals((int) i, j);
	}

	static boolean eq(Integer i, int j) {
		return i == null ? j == -1 : i == j;
	}

}
