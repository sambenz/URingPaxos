package ch.usi.da.dmap.local;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/*
 * Server process; should be addressable with port (multi instances per server)
 */
public class TreeNode<K extends Comparable<K>,V> implements Comparable<TreeNode<K,V>> {
	
	private TreeNode<K,V> parent = null;
	private final int order;
	private final int maxKeys;
	private K minKey = null; // or splitKey
	private TreeNode<K,V> nextNode = null;
	private final int maxChildren;
	private SortedSet<TreeNode<K,V>> children;
	private SortedMap<K,V> data;
	
	public TreeNode(TreeNode<K,V> parent,int order){
		this.parent = parent;
		this.order = order;
		this.maxKeys = order-1;
		this.maxChildren = order;
		this.children = new TreeSet<TreeNode<K,V>>();
		this.data = new TreeMap<K,V>();
	}
	
	public void setParent(TreeNode<K,V> node){
		parent = node;
	}

	public TreeNode<K,V> getParent(){
		return parent;
	}
	
	public Result<V> get(K k){
		if(children.isEmpty() && (nextNode == null || k.compareTo(nextNode.minKey) < 0)){ 
			return new Result<V>(true,data.get(k));
		}else{
			return new Result<V>(false,null);
		}
	}
		
	public K getMinKey(){
		return minKey;
	}
		
	public void setMinKey(K k){
		minKey = k;
	}
	
	public TreeNode<K,V> getNextNode(){
		return nextNode;
	}
	
	public void setNextNode(TreeNode<K,V> n){
		nextNode = n;
	}
	
	public SortedMap<K,V> getData(){
		return data;
	}
	
	public SortedSet<TreeNode<K, V>> getChildren(){
		return children;
	}
	
	public boolean put(K k,V v){
		boolean valid = false;
		if(minKey == null){
			minKey = k;
		}else if(k.compareTo(minKey) <= 0){
			minKey = k;
		}
		// test is leaf and max boundary (allow node split detection)
		if(children.isEmpty() && (nextNode == null || k.compareTo(nextNode.minKey) < 0)){ 
			data.put(k,v);
			valid = true;
		}
		if(data.size() > maxKeys){ // split data
			if(parent == null){ // root
				TreeNode<K,V> left = new TreeNode<K,V>(this,order);
				TreeNode<K,V> right = new TreeNode<K,V>(this,order);
				int middle = data.size()/2;
				@SuppressWarnings("unchecked")
				K splitKey = (K) data.keySet().toArray()[middle];
				SortedMap<K,V> lmap = new TreeMap<K,V>(data.headMap(splitKey));
				left.getData().putAll(lmap);
				left.setMinKey(lmap.firstKey());
				SortedMap<K,V> rmap = new TreeMap<K,V>(data.tailMap(splitKey));
				right.getData().putAll(rmap);
				right.setMinKey(rmap.firstKey());
				left.setNextNode(right);
				children.add(left);
				children.add(right);
				data.clear();
			}else{
				TreeNode<K,V> right = new TreeNode<K,V>(parent,order);
				int middle = data.size()/2;
				@SuppressWarnings("unchecked")
				K splitKey = (K) data.keySet().toArray()[middle];
				SortedMap<K,V> rmap = new TreeMap<K,V>(data.tailMap(splitKey));
				right.getData().putAll(rmap);
				right.setMinKey(rmap.firstKey());
				data = new TreeMap<K,V>(data.headMap(splitKey));
				right.setNextNode(this.getNextNode());
				this.setNextNode(right);
				minKey = data.firstKey();
				parent.addChild(right);
			}
		}
		return valid;
	}

	public void addChild(TreeNode<K,V> node){
		children.add(node);
		if(children.size() > maxChildren){ // split inner node
			if(parent == null){ // root
				TreeNode<K,V> left = new TreeNode<K,V>(this,order);
				TreeNode<K,V> right = new TreeNode<K,V>(this,order);
				int middle = children.size()/2;
				@SuppressWarnings("unchecked")
				TreeNode<K,V> splitKey = (TreeNode<K, V>) children.toArray()[middle];
				SortedSet<TreeNode<K,V>> lset = new TreeSet<TreeNode<K,V>>(children.headSet(splitKey));
				left.getChildren().addAll(lset);
				left.setMinKey(lset.first().getMinKey());
				for(TreeNode<K,V> n : left.getChildren()){
					n.setParent(left);
				}
				SortedSet<TreeNode<K,V>> rset = new TreeSet<TreeNode<K,V>>(children.tailSet(splitKey));
				right.getChildren().addAll(rset);
				right.setMinKey(rset.first().getMinKey());
				for(TreeNode<K,V> n : right.getChildren()){
					n.setParent(right);
				}
				children.clear();
				children.add(left);
				children.add(right);
			}else{
				TreeNode<K,V> right = new TreeNode<K,V>(parent,order);
				int middle = children.size()/2;
				@SuppressWarnings("unchecked")
				TreeNode<K,V> splitKey = (TreeNode<K, V>) children.toArray()[middle];
				SortedSet<TreeNode<K,V>> rset = new TreeSet<TreeNode<K,V>>(children.tailSet(splitKey));
				right.getChildren().addAll(rset);
				right.setMinKey(rset.first().getMinKey());
				for(TreeNode<K,V> n : right.getChildren()){
					n.setParent(right);
				}
				children = new TreeSet<TreeNode<K,V>>(children.headSet(splitKey));
				minKey = children.first().getMinKey();
				parent.addChild(right);
			}
		}
	}

	@Override
	public String toString(){
		StringBuffer buffer = new StringBuffer();
		if(data.size() > 0){
			buffer.append("D: " + data);
		}else{
			buffer.append("I: " + children);
		}
		//buffer.append(" (" + minKey + "/" + nextNode.getMinKey() + ")");
		return buffer.toString();
	}

	@Override
	public int compareTo(TreeNode<K,V> o) {
		return this.getMinKey().compareTo(o.getMinKey());
	}
	
}
