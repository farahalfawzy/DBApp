import java.util.Hashtable;

public class Octree {
	private Node root;
	private String X;
	private String Y;
	private String Z;
	private int MaxRowsinNode;

	public Octree(String X, String Y, String Z, int MinX, int MaxX, int MinY, int MaxY, int MinZ, int MaxZ,
			int MaxRowsinNode) {
		this.root = new Leaf(MinX, MaxX, MinY, MaxY, MinZ, MaxZ);
		this.X = X;
		this.Y = Y;
		this.Z = Z;
		this.MaxRowsinNode = MaxRowsinNode;
	}

	public Node getRoot() {
		return root;
	}

	public void setRoot(Node root) {
		this.root = root;
	}

	public void insertTupleInIndex(Hashtable<String, Object> key) {
		Node current = this.root;
		NonLeaf Parent = null;
		String positionOfLeaf = "";
		while (current != null) {
			this.displayTree2();

			if (current instanceof Leaf) {

				Leaf curLeaf = (Leaf) current;
				if (curLeaf.getBucket().size() == MaxRowsinNode) {// need to create new node
					NonLeaf newNode = new NonLeaf(current.getMinX(), current.getMaxX(), current.getMinY(),
							current.getMaxY(), current.getMinZ(), current.getMaxZ());
					if (Parent == null) {
						this.root = newNode;
						for (int i = 0; i < curLeaf.getBucket().size(); i++) {
							insertIncorrectLeaf(curLeaf.getBucket().get(i), newNode);
						}
						insertIncorrectLeaf(key, newNode);
						return;
					} else {
						switch (positionOfLeaf) {
						case "000":
							Parent.left0 = newNode;
							break;
						case "001":
							Parent.left1 = newNode;
							break;
						case "010":
							Parent.left2 = newNode;
							break;
						case "011":
							Parent.left3 = newNode;
							break;
						case "100":
							Parent.right3 = newNode;
							break;
						case "101":
							Parent.right2 = newNode;
							break;
						case "110":
							Parent.right1 = newNode;
							break;
						case "111":
							Parent.right0 = newNode;
							break;
						}
						for (int i = 0; i < curLeaf.getBucket().size(); i++) {
							insertIncorrectLeaf(curLeaf.getBucket().get(i), newNode);
						}
						insertIncorrectLeaf(key, newNode);
						return;

					}

				} else {
					curLeaf.getBucket().add(key);
					return;
				}

			} else {

				NonLeaf curNonleaf = (NonLeaf) current;
				boolean higherX = false, higherY = false, higherZ = false;

				int midX = (current.getMinX() + current.getMaxX()) / 2;
				int midY = (current.getMinY() + current.getMaxY()) / 2;
				int midZ = (current.getMinZ() + current.getMaxZ()) / 2;

				if (Integer.parseInt(key.get("X").toString()) > midX) {
					higherX = true;
				} else {
					higherX = false;
				}

				if (Integer.parseInt(key.get("Y").toString()) > midY) {
					higherY = true;
				} else {
					higherY = false;
				}

				if (Integer.parseInt(key.get("Z").toString()) > midZ) {
					higherZ = true;
				} else {
					higherZ = false;
				}
				String r = get3BitString(higherX, higherY, higherZ);
				positionOfLeaf = r;
				switch (r) {
				case "000":
					Parent = curNonleaf;
					current = curNonleaf.left0;
					break;
				case "001":
					Parent = curNonleaf;
					current = curNonleaf.left1;
					break;
				case "010":
					Parent = curNonleaf;
					current = curNonleaf.left2;
					break;
				case "011":
					Parent = curNonleaf;
					current = curNonleaf.left3;
					break;
				case "100":
					Parent = curNonleaf;
					current = curNonleaf.right3;
					break;
				case "101":
					Parent = curNonleaf;
					current = curNonleaf.right2;
					break;
				case "110":
					Parent = curNonleaf;
					current = curNonleaf.right1;
					break;
				case "111":
					Parent = curNonleaf;
					current = curNonleaf.right0;
					break;
				}

			}
		}

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


	public void displayTree2() {
		Node Current = this.root;
		int n = 0;
		java.util.Stack<Node> localStack = new java.util.Stack<Node>();
		java.util.Stack<Integer> localStack2 = new java.util.Stack<Integer>();

		int level = 0;
		localStack.push(Current);
		localStack2.push(n);
		localStack2.push(level);
		while (localStack.isEmpty() == false) {
			Current = localStack.pop();
			if (Current instanceof NonLeaf) {
				NonLeaf temp = (NonLeaf) Current;
				level++;

				n = -1;
				localStack.push(temp.left0);
				localStack2.push(++n);
				localStack2.push(level);
				localStack.push(temp.left1);
				localStack2.push(++n);
				localStack2.push(level);
				localStack.push(temp.left2);
				localStack2.push(++n);
				localStack2.push(level);
				localStack.push(temp.left3);
				localStack2.push(++n);
				localStack2.push(level);
				localStack.push(temp.right3);
				localStack2.push(++n);
				localStack2.push(level);
				localStack.push(temp.right2);
				localStack2.push(++n);
				localStack2.push(level);
				localStack.push(temp.right1);
				localStack2.push(++n);
				localStack2.push(level);
				localStack.push(temp.right0);
				localStack2.push(++n);
				localStack2.push(level);
			} else {
				Leaf temp = (Leaf) Current;
				System.out.println("Level " + localStack2.pop() + " NodeNo. " + localStack2.pop());
				for (int i = 0; i < temp.getBucket().size(); i++) {
					Hashtable h = temp.getBucket().get(i);
					System.out.println(i + " " + h.toString());

				}
				System.out.println("----------------------------------------");

			}
		}
	}

	public static void insertIncorrectLeaf(Hashtable<String, Object> key, NonLeaf node) {
		boolean higherX = false, higherY = false, higherZ = false;
		Node current = node;
		int midX = (current.getMinX() + current.getMaxX()) / 2;
		int midY = (current.getMinY() + current.getMaxY()) / 2;
		int midZ = (current.getMinZ() + current.getMaxZ()) / 2;

		if (Integer.parseInt(key.get("X").toString()) > midX) {
			higherX = true;
		} else {
			higherX = false;
		}

		if (Integer.parseInt(key.get("Y").toString()) > midY) {
			higherY = true;
		} else {
			higherY = false;
		}

		if (Integer.parseInt(key.get("Z").toString()) > midZ) {
			higherZ = true;
		} else {
			higherZ = false;
		}
		String r = get3BitString(higherX, higherY, higherZ);
		switch (r) {
		case "000":
			((Leaf) node.left0).getBucket().add(key);
			break;
		case "001":
			((Leaf) node.left1).getBucket().add(key);

			break;
		case "010":
			((Leaf) node.left2).getBucket().add(key);

			break;
		case "011":
			((Leaf) node.left3).getBucket().add(key);

			break;
		case "100":
			((Leaf) node.right3).getBucket().add(key);

			break;
		case "101":
			((Leaf) node.right2).getBucket().add(key);

			break;
		case "110":
			((Leaf) node.right1).getBucket().add(key);

			break;
		case "111":
			((Leaf) node.right0).getBucket().add(key);

			break;
		}
	}

	public static void main(String[] args) {
		Octree tree = new Octree("X", "Y", "Z", 0, 10, 0, 20,0, 40, 3);
		Hashtable<String, Object> key = new Hashtable<>();
		key.put("X", 2);
		key.put("Y", 3);
		key.put("Z", 6);
		tree.insertTupleInIndex(key);// 000
		key = new Hashtable<>();
		key.put("X", 6);
		key.put("Y", 3);
		key.put("Z", 5);
		tree.insertTupleInIndex(key);// 100
		// tree.displayTree2();
		key = new Hashtable<>();
		key.put("X", 2);
		key.put("Y", 3);
		key.put("Z", 25);
		tree.insertTupleInIndex(key);// 001
		tree.displayTree2();
		key = new Hashtable<>();

		key.put("X", 2);
		key.put("Y", 13);
		key.put("Z", 6);
		tree.insertTupleInIndex(key);// 010
		tree.displayTree2();
		key = new Hashtable<>();

		key.put("X", 7);
		key.put("Y", 3);
		key.put("Z", 27);
		tree.insertTupleInIndex(key);// 101
		key = new Hashtable<>();

		key.put("X", 2);
		key.put("Y", 3);
		key.put("Z", 9);
		tree.insertTupleInIndex(key);// 000
		key = new Hashtable<>();

		key.put("X", 1);
		key.put("Y", 8);
		key.put("Z", 2);
		tree.insertTupleInIndex(key);// 000
		key = new Hashtable<>();

		key.put("X", 7);
		key.put("Y", 11);
		key.put("Z", 26);
		tree.insertTupleInIndex(key);// 111
		tree.displayTree2();
		key = new Hashtable<>();

		key.put("X", 4);
		key.put("Y", 9);
		key.put("Z", 18);
		tree.insertTupleInIndex(key);// 000 //4th element in 000
		System.out.println("\n\n");
		tree.displayTree2();
	}
}