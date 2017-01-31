package ch.usi.da.dmap.local;

public class Range<K extends Comparable<K>> implements Comparable<Range<K>> {

	private final K minKey;
	private final K maxKey;
	
	public Range(K minKey,K maxKey){
		this.minKey = minKey;
		this.maxKey = maxKey;
	}
	
	public K getMinKey(){
		return minKey;
	}
	
	public K getMaxKey(){
		return maxKey;
	}
	
	@Override
	public int compareTo(Range<K> o) {
		return this.getMinKey().compareTo(o.getMinKey());
	}
	
	@Override
	public String toString(){
		return minKey + " " + maxKey;
	}

}
