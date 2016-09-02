package ch.usi.da.btree.local;

import java.util.Iterator;

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
		//return root.toString();
	}
	
    private static class TreePrinter {

        public static <K extends Comparable<K>,V> String getString(Btree<K,V> tree) {
            if (tree.root == null) return "Tree has no nodes.";
            return getString(tree.root, "", true);
        }

        private static <K extends Comparable<K>,V> String getString(TreeNode<K,V> node, String prefix, boolean isTail) {
            StringBuilder builder = new StringBuilder();

            builder.append(prefix).append((isTail ? "└── " : "├── "));
            if(node.getData().size() > 0){
            	builder.append(node.getData());
            }else{
            	for(TreeNode<K,V> n : node.getChildren()){
            		builder.append(n.getMinKey() + " ");	
            	}
            }
            builder.append("\n");
            if (node.getChildren().size() > 0) {
            	for(TreeNode<K,V> n : node.getChildren()){
            		if(n.equals(node.getChildren().last())){
            			builder.append(getString(n, prefix + (isTail ? "    " : "│   "), true));
            			break;
            		}
            		builder.append(getString(n, prefix + (isTail ? "    " : "│   "), false));
            	}
                /*for (int i = 0; i < node.numberOfChildren() - 1; i++) {
                    Node<T> obj = node.getChild(i);
                    builder.append(getString(obj, prefix + (isTail ? "    " : "│   "), false));
                }
                if (node.numberOfChildren() >= 1) {
                    Node<T> obj = node.getChild(node.numberOfChildren() - 1);
                    builder.append(getString(obj, prefix + (isTail ? "    " : "│   "), true));
                }*/
            }
            return builder.toString();
        }
    }

	public static void main(String[] args) {

		String ID = "d4e3eb95-5bd5-474f-a756-3f5d37dec1c8";
		Btree<Integer,String> tree = new Btree<Integer, String>(ID);
		
		tree.put(10,"10");
		System.out.println(tree);
		tree.put(20,"20");
		System.out.println(tree);
		tree.put(30,"30");
		System.out.println(tree);
		tree.put(40,"40");
		System.out.println(tree);
		tree.put(50,"50");
		System.out.println(tree);
		tree.put(60,"60");
		System.out.println(tree);
		tree.put(5,"5");
		System.out.println(tree);
		tree.put(70,"70");
		System.out.println(tree);
		tree.put(55,"55");
		System.out.println(tree);
		tree.put(56,"56");
		System.out.println(tree);
		
		System.out.println(tree.get(10));
		System.out.println(tree.get(20));
		System.out.println(tree.get(30));
		System.out.println(tree.get(40));
		System.out.println(tree.get(50));
		System.out.println(tree.get(60));
		System.out.println(tree.get(70));
		System.out.println(tree.get(55));
		System.out.println(tree.get(56));
		System.out.println(tree.get(5));

	}

}
