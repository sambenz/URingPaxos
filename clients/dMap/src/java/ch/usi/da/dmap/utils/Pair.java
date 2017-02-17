package ch.usi.da.dmap.utils;

public class Pair<K,V> implements java.io.Serializable {

	private static final long serialVersionUID = 3821699802992791854L;

	private final K key;
	private final V value;
	
	public Pair(K key, V value){
		this.key = key;
		this.value = value;
	}
	
	public K getKey(){
		return key;
	}
	
	public V getValue(){
		return value;
	}
	
    public boolean equals(Object o) {
        if (!(o instanceof Pair))
            return false;
        Pair<?,?> e = (Pair<?,?>)o;

        return valEquals(key,e.getKey()) && valEquals(value,e.getValue());
    }
    
    boolean valEquals(Object o1, Object o2) {
        return (o1==null ? o2==null : o1.equals(o2));
    }
    
    public int hashCode() {
        int keyHash = (key==null ? 0 : key.hashCode());
        int valueHash = (value==null ? 0 : value.hashCode());
        return keyHash ^ valueHash;
    }

	public String toString(){
		return key + "=" + value;
	}
}
