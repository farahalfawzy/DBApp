import java.util.*;

class Node {

	public Vector<Hashtable<String, Object>> data;
	public int minX;
	public int maxX;
	public int minY;
	public int maxY;
	public int minZ;
	public int maxZ;
	public Node left0, left1, left2, left3, right3, right2, right1, right0;

	public Node(Vector<Hashtable<String, Object>> data) {
		this(data, null, null, null, null, null, null, null, null);
	}

	public Node(Vector<Hashtable<String, Object>> data, Node left0, Node left1, Node left2, Node left3, Node right3,
			Node right2, Node right1, Node right0) {
		super();
		this.data = data;
		this.left0 = left0;
		this.left1 = left1;
		this.left2 = left2;
		this.left3 = left3;
		this.right3 = right3;
		this.right2 = right2;
		this.right1 = right1;
		this.right0 = right0;
	}

}

class Octree {
	private Node root;
	private int minX;
	private int maxX;
	private int minY;
	private int maxY;
	private int minZ;
	private int maxZ;

	public Octree(int minX, int maxX, int minY, int maxY, int minZ, int maxZ) {
		super();
		this.root = null;
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
		this.minZ = minZ;
		this.maxZ = maxZ;
	}

	public static String get3BitString(boolean higherX, boolean higherY, boolean higherZ) {
		int result = 0;
		if (higherX) {
			result |= 0b100;
		}
		if (higherY) {
			result |= 0b010;
		}
		if (higherZ) {
			result |= 0b001;
		}
		String binaryString = Integer.toBinaryString(result);
		while (binaryString.length() < 6) {
			binaryString = "0" + binaryString;
		}
		return binaryString.substring(3);
	}

	public void add(Hashtable<String, Object> key) {
		Node current = root, parent = null;
		boolean higherX = false, higherY = false, higherZ = false;
		while (current != null) {
			int midX = (current.minX + current.maxX) / 2;
			int midY = (current.minY + current.maxY) / 2;
			int midZ = (current.minZ + current.maxZ) / 2;
			if (Integer.parseInt(key.get("x").toString()) > midX) {
				higherX = true;
			} else {
				higherX = false;
			}

			if (Integer.parseInt(key.get("y").toString()) > midY) {
				higherY = true;
			} else {
				higherY = false;
			}

			if (Integer.parseInt(key.get("z").toString()) > midZ) {
				higherZ = true;
			} else {
				higherZ = false;
			}

			String r = get3BitString(higherX,higherY, higherZ);
			switch (r) {
			case "000":
				parent = current;
				current = current.left0;
				break;
			case "001":
				parent = current;
				current = current.left1;
				break;
			case "010":
				parent = current;
				current = current.left2;
				break;
			case "011":
				parent = current;
				current = current.left3;
				break;
			case "100":
				parent = current;
				current = current.right3;
				break;
			case "101":
				parent = current;
				current = current.right2;
				break;
			case "110":
				parent = current;
				current = current.right1;
				break;
			case "111":
				parent = current;
				current = current.right0;
				break;
			}
		}
//		this.displayTree();
		if (parent == null) {
			Vector<Hashtable<String, Object>> data = new Vector<Hashtable<String, Object>>();
			data.add(key);
			root = new Node(data);
		}

		else {
			
			parent.data.add(key);
			if (true) {
				//elmfrood 2loop 3ala kol wa7da w e3mlha insert tany fel node
				int midX = (current.minX + current.maxX) / 2;
				int midY = (current.minY + current.maxY) / 2;
				int midZ = (current.minZ + current.maxZ) / 2;
				Vector<Hashtable<String,Object>> v=(Vector<Hashtable<String, Object>>) parent.data.clone();
//				System.out.println(v);
				parent.data.clear();
				for(int i=0;i<v.size();i++) {
					System.out.println("here:"+v.get(i));
					if (Integer.parseInt(v.get(i).get("x").toString()) > midX) {
						higherX = true;
					} else {
						higherX = false;
					}

					if (Integer.parseInt(v.get(i).get("y").toString()) > midY) {
						higherY = true;
					} else {
						higherY = false;
					}

					if (Integer.parseInt(v.get(i).get("z").toString()) > midZ) {
						higherZ = true;
					} else {
						higherZ = false;
					}
					if(v.get(i).get("x").toString().equals("2") && v.get(i).get("y").toString().equals("0")
							&& v.get(i).get("z").toString().equals("0"))
						System.out.println("000"+higherX+""+ higherY+""+ higherZ);
					String r = get3BitString(higherX, higherY, higherZ);
					System.out.println(r);
					switch(r) {
					case "000":
						if(parent.left0 == null) { 
							Vector<Hashtable<String,Object>> tmp = new Vector<>();
							tmp.add(v.get(i));
							parent.left0= new Node(tmp);
							parent.left0.data.add(v.get(i));
							System.out.println("create new left0");
						}
						else
							parent.left0.data.add(v.get(i));
						break;
					case "001":
						if(parent.left1 == null) { 
							Vector<Hashtable<String,Object>> tmp = new Vector<>();
							tmp.add(v.get(i));
							parent.left1= new Node(tmp);
							parent.left1.data.add(v.get(i));
							System.out.println("create new left1");
						}
						else
							parent.left1.data.add(v.get(i));
						break;
					case "010":
						if(parent.left2 == null) { 
							Vector<Hashtable<String,Object>> tmp = new Vector<>();
							tmp.add(v.get(i));
							parent.left2= new Node(tmp);
							parent.left2.data.add(v.get(i));
							System.out.println("create new left2");
						}
						else
							parent.left2.data.add(v.get(i));
						break;
					case "011":
						if(parent.left3 == null) { 
							Vector<Hashtable<String,Object>> tmp = new Vector<>();
							tmp.add(v.get(i));
							parent.left3= new Node(tmp);
							parent.left3.data.add(v.get(i));
							System.out.println("create new left3");
						}
						else
							parent.left3.data.add(v.get(i));
						break;
					case "100":
						if(parent.right3 == null) { 
							Vector<Hashtable<String,Object>> tmp = new Vector<>();
							tmp.add(v.get(i));
							parent.right3= new Node(tmp);
							parent.right3.data.add(v.get(i));
							System.out.println("create new right3");
						}
						else
							parent.right3.data.add(v.get(i));
						break;
					case "101":
						if(parent.right2 == null) { 
							Vector<Hashtable<String,Object>> tmp = new Vector<>();
							tmp.add(v.get(i));
							parent.right2= new Node(tmp);
							parent.right2.data.add(v.get(i));
							System.out.println("create new right2");
						}
						else
							parent.right2.data.add(v.get(i));
						break;
					case "110":
						if(parent.right1 == null) { 
							Vector<Hashtable<String,Object>> tmp = new Vector<>();
							tmp.add(v.get(i));
							parent.right1= new Node(tmp);
							parent.right1.data.add(v.get(i));
							System.out.println("create new right1");
						}
						else
							parent.right1.data.add(v.get(i));
						break;
					case "111":
						if(parent.right0 == null) { 
							Vector<Hashtable<String,Object>> tmp = new Vector<>();
							tmp.add(v.get(i));
							parent.right0= new Node(tmp);
							parent.right0.data.add(v.get(i));
							System.out.println("create new right0");
						}
						else
							parent.right0.data.add(v.get(i));
						break;
					}
				}
				parent.data.clear();
				Hashtable<String,Object> newHtbl = new Hashtable<>();
				newHtbl.put("x", midX);
				newHtbl.put("y", midY);
				newHtbl.put("z", midZ);
				parent.data.add(newHtbl);
			}
		}
		this.displayTree();
		System.out.println("##########################################");
	}

//	public boolean delete(Comparable key) {
//        if (root == null)
//               return false;
//        Node current = root;
//        Node parent = root;
//        boolean right = true;
//        // searching for the node to be deleted
//        while (key.compareTo(current.data) != 0) {  
//               if (key.compareTo(current.data) < 0) {         right = false;
//                     parent = current;
//                     current = current.left;
//               } else {
//                     right = true;
//                     parent = current;
//                     current = current.right;
//               }
//               if (current == null)
//                     return false;
//        }
//
//        Node substitute = null;
//        //  case 1: Node to be deleted has no children
//        if (current.left == null && current.right == null)
//               substitute = null;
//
//        //  case 2: Node to be deleted has one child
//        else if (current.left == null)
//               substitute = current.right;
//        else if (current.right == null)
//               substitute = current.left;
//        else // case 3: Node to be deleted has two children
//        {
//               Node successor = current.right;
//               Node successorParent = current;
//               //  searching for the inorder successor of the node to be deleted
//               while (successor.left != null) {
//                     successorParent = successor;
//                     successor = successor.left;
//               }
//               substitute = successor;
//               if (successorParent == current) {
//                     if (successor.right == null)
//                            successorParent.right = null;
//                     else
//                            successorParent.right = successor.right;
//               } else {
//                     if (successor.right == null)
//                            successorParent.left = null;
//                     else
//                            successorParent.left = successor.right;
//               }
//               successor.right = current.right;
//               successor.left = current.left;
//               substitute = successor;
//        } // case 3 done
//        if (current == root) // Replacing the deleted node
//               root = substitute;
//        else if (right)
//               parent.right = substitute;
//        else
//               parent.left = substitute;
//        return true;
//
// }

	public void displayTree() {
		java.util.Stack<Node> globalStack = new java.util.Stack<Node>();
		globalStack.push(root);
		int nBlanks = 32;
		boolean isRowEmpty = false;
		System.out.println("......................................................");
		while (isRowEmpty == false) {
			java.util.Stack<Node> localStack = new java.util.Stack<Node>();
			isRowEmpty = true;

			for (int j = 0; j < nBlanks; j++)
				System.out.print(' ');

			while (globalStack.isEmpty() == false) {
				Node temp = globalStack.pop();
				if (temp != null) {
					System.out.print(temp.data);
					localStack.push(temp.right0);
					localStack.push(temp.right1);
					localStack.push(temp.right2);
					localStack.push(temp.right3);
					localStack.push(temp.left3);
					localStack.push(temp.left2);
					localStack.push(temp.left1);
					localStack.push(temp.left0);
					

					if (temp.right0 != null || temp.right1 != null || temp.right2 != null || temp.right3 != null
							|| temp.left0 != null || temp.left1 != null || temp.left2 != null || temp.left3 != null)
						isRowEmpty = false;
				} else {
					System.out.print("--");
					localStack.push(null);
					localStack.push(null);
				}
				for (int j = 0; j < nBlanks * 2 - 2; j++)
					System.out.print(' ');
			} // end while globalStack not empty
			System.out.println();
			nBlanks /= 2;
			while (localStack.isEmpty() == false)
				globalStack.push(localStack.pop());
		} // end while isRowEmpty is false
		System.out.println("......................................................");
	}

	public static void main(String[] args) {
		// Create a new Octree
		Octree octree = new Octree(0,100,0,100,0,100);

		// Add 32 nodes to the Octree
		for (int i = 0; i < 4; i++) {
			for (int j = 0; j < 4; j++) {
				for (int k = 0; k < 2; k++) {
					Hashtable<String, Object> node = new Hashtable<>();
					node.put("x", i);
					node.put("y", j);
					node.put("z", k);
					octree.add(node);
				}
			}
		}
		octree.displayTree();
		System.out.println(get3BitString(true, false,true));
	}

}