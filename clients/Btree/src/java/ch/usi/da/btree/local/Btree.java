package ch.usi.da.btree.local;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/*
 * Client
 */
public class Btree<K extends Comparable<K>,V> {
	
	private TreeNode<K,V> root = null;
	
	public Btree(String ID){
		root = findRoot(ID);
	}

	private TreeNode<K,V> findRoot(String ID){
		//FIXME: how to find root? (ask any node?; how to find nodes? multicast?)
		/*
		 * maybe make a tree visible in zookeeper: /btree/<ID> (order)/nodes (RPC address)
		 */
		if(root == null){ //FIXME: testing only
			root = new TreeNode<K,V>(null,3);
		}
		return root;
	}
	
	private TreeNode<K,V> lookupNode(K k){
		// caching?
		// start at root and navigate down
		return root;
	}

	public void put(K k,V v){
		TreeNode<K,V> node = lookupNode(k);
		boolean done = false;
		while(!done){
			//TODO: should reload because of checked boundaries
			if(node.getChildren().isEmpty()){
				done = node.put(k,v);
			}else{
				Iterator<TreeNode<K,V>> i = node.getChildren().iterator();
				TreeNode<K,V> n0 = i.next();
				if(k.compareTo(n0.getMinKey()) <= 0){
					node = n0; // most left
					continue;
				}
				while(i.hasNext()){
					TreeNode<K,V> n1 = i.next();
					if(k.compareTo(n0.getMinKey()) >= 0 && k.compareTo(n1.getMinKey()) < 0){
						node = n0;
						break;
					}
					n0 = n1;
				}
				node = n0; // most right
			}
		}
	}
	
	public V get(K k){
		TreeNode<K,V> node = lookupNode(k);
		boolean done = false;
		while(!done){ 
			//TODO: should reload because of checked boundaries
			if(!node.getData().isEmpty()){
				done = true;
				break;
			}else{
				Iterator<TreeNode<K,V>> i = node.getChildren().iterator();
				TreeNode<K,V> n0 = i.next();
				if(k.compareTo(n0.getMinKey()) <= 0){
					node = n0;
					continue;
				}
				while(i.hasNext()){
					TreeNode<K,V> n1 = i.next();
					if(k.compareTo(n0.getMinKey()) >= 0 && k.compareTo(n1.getMinKey()) < 0){
						node = n0;
						break;
					}
					n0 = n1;
				}
				node = n0;
			}
		}
		return node.get(k);
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
		
		/*tree.put(10,"10");
		tree.put(9,"9");
		tree.put(8,"8");
		tree.put(7,"7");
		tree.put(6,"6");
		tree.put(5,"5");
		tree.put(4,"4");
		tree.put(3,"3");
		tree.put(2,"2");
		tree.put(1,"1");
		System.out.println(tree);
		*/
		
		/*int[] in = {8930,693,1501,3259,6705,7357,3798,401,1947,5095,16,587,99};
		for(int i : in){
			tree.put(i,Integer.toString(i));
			System.out.println(tree);
		}
		System.out.println(tree);
		System.err.println(tree.get(16));*/
		
		Random rnd = new Random(System.currentTimeMillis());
		Set<Integer> in = new TreeSet<Integer>();
		for(int i=1;i<1000;i++){
			//System.err.println(i);
			//System.err.println(tree);
			int k = rnd.nextInt(10000);
			//System.out.println(k);
			in.add(k);
			tree.put(k,Integer.toString(k));
		}
		System.out.println(tree);
		for(int i : in){
			String s = tree.get(i);
			if(s == null){
				System.err.println(i);
			}
		}
	}

}
