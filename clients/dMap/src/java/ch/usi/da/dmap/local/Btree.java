package ch.usi.da.dmap.local;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/*
 * Client
 */
public class Btree<K extends Comparable<K>,V> {
	
	private TreeNode<K,V> root = null;
	private SortedMap<Range<K>,TreeNode<K,V>> cache = new TreeMap<Range<K>,TreeNode<K,V>>();
	private final boolean usecache = true;
	public int cachehit = 0;
	public int treewalk = 0;
	
	public Btree(String ID){
		root = findRoot(ID);
	}

	private TreeNode<K,V> findRoot(String ID){
		/* TODO:
		 * how to find root? (ask any node?; how to find nodes? multicast?)
		 * maybe make a tree visible in zookeeper: /btree/<ID> (order)/nodes (RPC address)
		 */
		if(root == null){
			root = new TreeNode<K,V>(null,3);
		}
		return root;
	}
	
	public TreeNode<K,V> lookupNode(K k){
		TreeNode<K,V> node = null;
		
		// cache
		//if(usecache && !cache.isEmpty() && k.compareTo(cache.firstKey().getMinKey()) <= 0){
		//	  node = cache.get(cache.firstKey()); //TODO: most left cache only possible with double linked leafs
		//}else{
		if(usecache){
			for(Entry<Range<K>,TreeNode<K,V>> e : cache.entrySet()){
				if(k.compareTo(e.getKey().getMinKey()) >= 0 && (e.getKey().getMaxKey() == null || k.compareTo(e.getKey().getMaxKey()) < 0)){
					node = e.getValue();
					break;
				}
			}
			if(node != null){
				cachehit++;
				return node;
			}
		}
		
		// tree walk
		treewalk++;
		node = root;
		while(true){
			if(node.getChildren().isEmpty()){
				// fill cache
				if(usecache && node != root){
					Range<K> range;
					if(node.getNextNode() != null){
						range = new Range<K>(node.getMinKey(),node.getNextNode().getMinKey());
					}else{
						range = new Range<K>(node.getMinKey(),null);
					}
					cache.put(range,node);
				}
				break; // is leaf
			}else{
				Iterator<TreeNode<K,V>> i = node.getChildren().iterator();
				TreeNode<K,V> n0 = i.next();
				if(k.compareTo(n0.getMinKey()) <= 0){
					node = n0; // go left
					continue;
				}
				while(i.hasNext()){
					TreeNode<K,V> n1 = i.next(); //FIXME: can throw concurrent mod exception (even with fully synchronized TreeNode)
					if(k.compareTo(n0.getMinKey()) >= 0 && k.compareTo(n1.getMinKey()) < 0){
						node = n0; // inner node
						break;
					}
					n0 = n1;
				}
				node = n0; // go right
			}
		}
		return node;
	}

	public void put(K k,V v){
		boolean done = false;
		while(!done){
			TreeNode<K,V> node = lookupNode(k);
			done = node.put(k,v);
			// clear cache
			if(!done){
				cache.clear(); //TODO: do more efficient
			}
		}
	}
	
	public V get(K k){
		while(true){
			Result<V> r = lookupNode(k).get(k);
			if(r.isValid()){
				return r.getValue();
			}else{
				cache.clear(); //TODO: do more efficient
			}
		}
	}
	
	@Override
    public String toString() {
        return TreePrinter.getString(this);
	}
	
    private static class TreePrinter {

        public static <K extends Comparable<K>,V> String getString(Btree<K,V> tree) {
            return print(tree.root,"",true);
        }

        private static <K extends Comparable<K>,V> String print(TreeNode<K,V> node, String prefix, boolean isTail) {
            StringBuilder builder = new StringBuilder();
            builder.append(prefix).append((isTail ? "└──" : "├──"));
            if(node.getData().size() > 0){
            	builder.append(node.getData());
            }else{
            	builder.append("─┐ ");
            	/*builder.append("( ");
            	for(TreeNode<K,V> n : node.getChildren()){
            		builder.append(n.getMinKey() + " ");	
            	}
            	builder.append(")");*/
            }
            builder.append("\n");
            if (node.getChildren().size() > 0) {
            	for(TreeNode<K,V> n : node.getChildren()){
            		if(n.equals(node.getChildren().last())){
            			builder.append(print(n, prefix + (isTail ? "    " : "│   "), true));
            			break;
            		}
            		builder.append(print(n, prefix + (isTail ? "    " : "│   "), false));
            	}
            }
            return builder.toString();
        }
    }

	public static void main(String[] args) {

		String ID = "d4e3eb95-5bd5-474f-a756-3f5d37dec1c8";
		Btree<Integer,String> tree = new Btree<Integer, String>(ID);
		
		/*Thread t1 = new Thread(new TreeRunner<Integer,String>(tree));
		Thread t2 = new Thread(new TreeRunner<Integer,String>(tree));
		Thread t3 = new Thread(new TreeRunner<Integer,String>(tree));
		Thread t4 = new Thread(new TreeRunner<Integer,String>(tree));
		t1.start();
		t2.start();
		t3.start();
		t4.start();*/
		
		/*tree.put(10,"10");
		tree.put(2,"2");
		tree.put(9,"9");
		tree.put(4,"4");
		tree.put(8,"8");
		tree.put(6,"6");
		tree.put(7,"7");
		tree.put(3,"3");
		tree.put(1,"1");
		tree.put(5,"5");
		System.out.println(tree);*/
		
		
		// insert random values
		Random rnd = new Random(System.currentTimeMillis());
		Set<Integer> in = new TreeSet<Integer>();
		for(int i=1;i<100;i++){
			//System.err.println(i);
			//System.err.println(tree);
			int k = rnd.nextInt(1000);
			//System.out.println(k);
			in.add(k);
			tree.put(k,Integer.toString(k));
		}
		System.out.println(tree);
		
		// test if all values can be found
		for(int i : in){
			String s = tree.get(i);
			if(s == null){
				System.err.println("not found " + i);
			}
		}
		
		// test navigation links
		TreeNode<Integer,String> start = tree.lookupNode(0);
		int n = -1;
		for(int i : start.getData().keySet()){
			in.remove(i);
			if(i <= n){
				System.err.println("violate order " + i);
			}
			n = i;
		}
		while(start.getNextNode() != null){
			start = start.getNextNode();
			for(int i : start.getData().keySet()){
				in.remove(i);
				if(i < n){
					System.err.println("violate order " + i);
				}
				n = i;
			}
		}
		if(in.size() > 0){
			System.err.println(in);
		}
		
		// test small bigger non existence key
		if(tree.get(-1) != null){ System.err.println("non exists get error");}
		if(tree.get(100000000) != null){ System.err.println("non exists get error");}
		
		System.out.println("Cache hit: " + tree.cachehit + " / tree walk: " + tree.treewalk + " (" + (int)((float)tree.cachehit/((float)(tree.treewalk+tree.cachehit)/100)) + "%)");
		
	}

}
