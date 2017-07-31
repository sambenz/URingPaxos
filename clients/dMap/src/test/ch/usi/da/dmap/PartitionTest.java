package ch.usi.da.dmap;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import ch.usi.da.dmap.thrift.gen.Replica;
import ch.usi.da.dmap.utils.Pair;

public class PartitionTest {

	public final SortedMap<Integer, Set<Replica>> partitions = new TreeMap<Integer,Set<Replica>>();
	
	public int getPartition(int hash){
		int partition;
		SortedMap<Integer,Set<Replica>> tailMap = partitions.tailMap(hash);
		partition = tailMap.isEmpty() ? partitions.firstKey() : tailMap.firstKey();
		System.out.println(hash + "->" + partition);
		return partition;
	}
	
	public Pair<Integer,Integer> getRange(int token){
		Pair<Integer,Integer> p = new Pair<Integer,Integer>(token,null);
		
		
		System.out.println(p);
		return p;
	}
	
	public static void main(String[] args) {
		PartitionTest test =  new PartitionTest();
		
		Replica r = new Replica();
		Set<Replica> s = new HashSet<Replica>();
		s.add(r);
		
		test.partitions.put(0, s);
		test.partitions.put(20000, s);
		test.partitions.put(40000, s);
		
		//test.getPartition(0);
		test.getPartition(2);
		test.getPartition(20002);
		test.getPartition(40002);
		//test.getPartition(-2);
		
		test.getRange(0);

	}

}
