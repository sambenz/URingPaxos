package ch.usi.da.dmap.local;

public class Result<V> {

	private final V value;
	private final boolean valid;
	
	public Result(boolean valid, V value){
		this.value = value;
		this.valid = valid;
	}
	
	public V getValue(){
		return value;
	}
	
	public boolean isValid(){
		return valid;
	}
	
	@Override
	public String toString(){
		return valid ? value.toString() : "false";
	}
	
	//@SuppressWarnings("unchecked")
	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object o) {
		if(o instanceof Result){
			return this.value.equals(((Result<V>) o).getValue());
		}
		return false;
	}

}
