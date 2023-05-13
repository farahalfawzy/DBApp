import java.io.FileInputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.Vector;
import java.util.ArrayList;

public class Octree implements Serializable {
	private Node root;
	private String X;
	private String Y;
	private String Z;
	private int MaxRowsinNode;

	private static int MaxRowsinNode() {
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
			prop.load(fis);
			return Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
			// return 4;
		} catch (Exception ex) {
			System.out.println("was here");
			return -1;
		}
	}

	public Octree(String X, String Y, String Z, Object MinX, Object MaxX, Object MinY, Object MaxY, Object MinZ,
			Object MaxZ) {
		this.root = new Leaf(MinX, MaxX, MinY, MaxY, MinZ, MaxZ);
		this.X = X.toLowerCase();
		this.Y = Y.toLowerCase();
		this.Z = Z.toLowerCase();
		this.MaxRowsinNode = MaxRowsinNode();
	}

	public String getX() {
		return X;
	}

	public String getY() {
		return Y;
	}

	public String getZ() {
		return Z;
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

			if (current instanceof Leaf) {

				Leaf curLeaf = (Leaf) current;
				if (curLeaf.getSize() == MaxRowsinNode) {// need to create new node

					NonLeaf newNode = new NonLeaf(current.getMinX(), current.getMaxX(), current.getMinY(),
							current.getMaxY(), current.getMinZ(), current.getMaxZ(), curLeaf.getBeforeLeaf(),
							curLeaf.getAfterLeaf());
					if (Parent == null) {// if current is root
						this.root = newNode;
						for (int i = 0; i < curLeaf.getBucket().size(); i++) {
							insertIncorrectLeaf(curLeaf.getBucket().get(i), newNode);
						}
						for (int i = 0; i < curLeaf.getOverflow().size(); i++) {
							insertIncorrectLeaf(curLeaf.getOverflow().get(i), newNode);
						}
						insertIncorrectLeaf(key, newNode);
						return;
					} else {// current is leaf but not root
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
						for (int i = 0; i < curLeaf.getOverflow().size(); i++) {
							insertIncorrectLeaf(curLeaf.getOverflow().get(i), newNode);
						}
						insertIncorrectLeaf(key, newNode);
						return;

					}

				} else {
					if (curLeaf.IsDup(key)) {
						curLeaf.getOverflow().add(key);
					} else {
						curLeaf.getBucket().add(key);
						curLeaf.setSize(curLeaf.getSize() + 1);
					}

					return;
				}

			} else { // current is NonLeaf

				NonLeaf curNonleaf = (NonLeaf) current;
				boolean higherX = false, higherY = false, higherZ = false;
				higherX = getHigherX(current.getMinX(), current.getMaxX(), key);
				higherY = getHigherY(current.getMinY(), current.getMaxY(), key);
				higherZ = getHigherZ(current.getMinZ(), current.getMaxZ(), key);

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

	public void getAllLeaves(Vector<Leaf> res,Node current,Hashtable<String, Object> htblColNameValue){
		if(current == null)
			return;
//		System.out.println(current.toString());
		if(current instanceof Leaf) {
			Leaf myLeaf = (Leaf) current;
			for(int i=0;i<myLeaf.getBucket().size();i++) {
				for(String key:htblColNameValue.keySet()) {
					if(myLeaf.getBucket().get(i).contains(htblColNameValue.get(key))) {
						res.add(myLeaf);
					}
				}
			}
			return;
		}
		boolean higherX = false, higherY = false, higherZ = false;
		boolean inX = false, inY = false, inZ = false;
		for(String myKey:htblColNameValue.keySet()) {
			if(myKey.equals(this.X)) {
				higherX = getHigherX(current.getMinX(), current.getMaxX(), htblColNameValue);
				inX = true;
			}
			if(myKey.equals(Y)) {
				higherY = getHigherY(current.getMinY(), current.getMaxY(), htblColNameValue);
				inY=true;
			}
			if(myKey.equals(Z)) {
				higherZ = getHigherZ(current.getMinZ(), current.getMaxZ(), htblColNameValue);
				inZ=true;
			}
		}
		if(inX) {
			if(higherX) {
				getAllLeaves(res, ((NonLeaf)(current)).right3, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right2, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right1, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right0, htblColNameValue);
			}
			else {
				getAllLeaves(res, ((NonLeaf)(current)).left0, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left1, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left2, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left3, htblColNameValue);
			}
		}
		if(inY) {
			if(higherY) {
				getAllLeaves(res, ((NonLeaf)(current)).left2, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left3, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right1, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right0, htblColNameValue);
			}
			else {
				getAllLeaves(res, ((NonLeaf)(current)).left0, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left1, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right3, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right2, htblColNameValue);
			}
		}
		if(inZ) {
			if(!higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).left0, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left2, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right3, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right1, htblColNameValue);
			}
			else {
				getAllLeaves(res, ((NonLeaf)(current)).left1, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left3, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right2, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right0, htblColNameValue);
			}
		}
		if(inX && inY) {
			if(higherX && higherY) {
				getAllLeaves(res, ((NonLeaf)(current)).right1, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right0, htblColNameValue);
			}
			if(higherX && !higherY) {
				getAllLeaves(res, ((NonLeaf)(current)).right3, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right2, htblColNameValue);
			}
			if(!higherX && higherY) {
				getAllLeaves(res, ((NonLeaf)(current)).left2, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left3, htblColNameValue);
			}
			if(!higherX && !higherY) {
				getAllLeaves(res, ((NonLeaf)(current)).left0, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left1, htblColNameValue);
			}
		}
		if(inX && inZ) {
			if(higherX && higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).right2, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right0, htblColNameValue);
			}
			if(higherX && !higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).right3, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right1, htblColNameValue);
			}
			if(!higherX && higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).left1, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left3, htblColNameValue);
			}
			if(!higherX && !higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).left0, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).left2, htblColNameValue);
			}
		}
		if(inY && inZ) {
			if(higherY && higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).left3, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right0, htblColNameValue);
			}
			if(higherY && !higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).left2, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right1, htblColNameValue);
			}
			if(!higherY && higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).left1, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right2, htblColNameValue);
			}
			if(!higherY && !higherZ) {
				getAllLeaves(res, ((NonLeaf)(current)).left0, htblColNameValue);
				getAllLeaves(res, ((NonLeaf)(current)).right3, htblColNameValue);
			}
		}
		if(inX && inY && inZ) {
			String r = get3BitString(higherX, higherY, higherZ);
			switch (r) {
			case "000":
				getAllLeaves(res, ((NonLeaf)(current)).left0, htblColNameValue);
				break;
			case "001":
				getAllLeaves(res, ((NonLeaf)(current)).left1, htblColNameValue);
				break;
			case "010":
				getAllLeaves(res, ((NonLeaf)(current)).left2, htblColNameValue);
				break;
			case "011":
				getAllLeaves(res, ((NonLeaf)(current)).left3, htblColNameValue);
				break;
			case "100":
				getAllLeaves(res, ((NonLeaf)(current)).right3, htblColNameValue);
				break;
			case "101":
				getAllLeaves(res, ((NonLeaf)(current)).right2, htblColNameValue);
				break;
			case "110":
				getAllLeaves(res, ((NonLeaf)(current)).right1, htblColNameValue);
				break;
			case "111":
				getAllLeaves(res, ((NonLeaf)(current)).right0, htblColNameValue);
				break;
			}
		}
	}
	
	public void deleteTuple(Hashtable<String, Object> key) {
		Node current = root;
		NonLeaf parent = null;
		Vector<Leaf> res = new Vector<>();
		getAllLeaves(res,current,key);
		for(int i=0;i<res.size();i++) {
			res.get(i).removeFromBucket(key);
			if(res.get(i).getSize()<MaxRowsinNode) {
				Vector<Hashtable<String, Object>> overflow = res.get(i).getOverflow();
				for(int j=0;j<overflow.size();j++){
					res.get(i).getBucket().add(overflow.get(j));
					overflow.remove(j);
					j--;
				}
			}
		}
	}

	private boolean getHigherZ(Object minZ, Object maxZ, Hashtable<String, Object> key) {
		if (minZ instanceof Integer && maxZ instanceof Integer) {
			int midZ = ((Integer.parseInt(minZ.toString())) + ((Integer.parseInt(maxZ.toString())))) / 2;

			if (((Integer) key.get(getZ())) > midZ) {
				return true;
			} else {
				return false;
			}
		}
		if (minZ instanceof Double && maxZ instanceof Double) {
			Double midZ = ((Double.parseDouble(minZ.toString())) + ((Double.parseDouble(maxZ.toString())))) / 2;
			if (((Double) key.get(getZ())) > midZ) {
				return true;
			} else {
				return false;
			}
		}
		if (minZ instanceof String && maxZ instanceof String) {
			String midZ = printMiddleString(((String) minZ).toLowerCase(), ((String) maxZ).toLowerCase(),
					((String) minZ).length());
			if (((String) key.get(getZ())).toLowerCase().compareTo(midZ) > 0)
				return true;
			else
				return false;
		}
		if (minZ instanceof java.util.Date && maxZ instanceof java.util.Date) {
			Date minDate = (Date) minZ;
			Date maxDate = (Date) maxZ;
			Date currDate = (Date) key.get(getZ());
			Date midZ = findMidPoint(minDate, maxDate);
			if (currDate.after(midZ))
				return true;
			else
				return false;

		}
		return false;
	}

	private boolean getHigherY(Object minY, Object maxY, Hashtable<String, Object> key) {

		if (minY instanceof Integer && maxY instanceof Integer) {
			int midY = ((Integer.parseInt(minY.toString())) + ((Integer.parseInt(maxY.toString())))) / 2;
			System.out.println(minY + " " + maxY + " " + midY);

			if (((Integer) key.get(getY())) > midY) {
				return true;
			} else {
				return false;
			}
		}
		if (minY instanceof Double && maxY instanceof Double) {
			Double midY = ((Double.parseDouble(minY.toString())) + ((Double.parseDouble(maxY.toString())))) / 2;
			if (((Double) key.get(getY())) > midY) {
				return true;
			} else {
				return false;
			}
		}
		if (minY instanceof String && maxY instanceof String) {
			String midY = printMiddleString(((String) minY).toLowerCase(), ((String) maxY).toLowerCase(),
					((String) minY).length());
			if (((String) key.get(getY())).toLowerCase().compareTo(midY) > 0)
				return true;
			else
				return false;
		}
		if (minY instanceof java.util.Date && maxY instanceof java.util.Date) {
			Date minDate = (Date) minY;
			Date maxDate = (Date) maxY;
			Date currDate = (Date) key.get(getY());
			Date midX = findMidPoint(minDate, maxDate);
			if (currDate.after(midX))
				return true;
			else
				return false;

		}
		return false;
	}

	private boolean getHigherX(Object minX, Object maxX, Hashtable<String, Object> key) {
		if (minX instanceof Integer && maxX instanceof Integer) {
			int midX = ((Integer.parseInt(minX.toString())) + ((Integer.parseInt(maxX.toString())))) / 2;

			if (((Integer) key.get(getX())) > midX) {
				return true;
			} else {
				return false;
			}
		}
		if (minX instanceof Double && maxX instanceof Double) {
			Double midX = ((Double.parseDouble(minX.toString())) + ((Double.parseDouble(maxX.toString())))) / 2;
			if (((Double) key.get(getX())) > midX) {
				return true;
			} else {
				return false;
			}
		}
		if (minX instanceof String && maxX instanceof String) {
			String midX = printMiddleString(((String) minX).toLowerCase(), ((String) maxX).toLowerCase(),
					((String) minX).length());
			if (((String) key.get(getX())).toLowerCase().compareTo(midX) > 0)
				return true;
			else
				return false;
		}
		if (minX instanceof java.util.Date && maxX instanceof java.util.Date) {
			Date minDate = (Date) minX;
			Date maxDate = (Date) maxX;
			Date currDate = (Date) key.get(getX());
			Date midX = findMidPoint(minDate, maxDate);
			if (currDate.after(midX))
				return true;
			else
				return false;

		}
		return false;
	}

	private static Date findMidPoint(Date date1, Date date2) {
		LocalDate localDate1 = date1.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		LocalDate localDate2 = date2.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

		Period period = Period.between(localDate1, localDate2);
		LocalDate middleLocalDate = localDate1.plus(dividePeriodByTwo(period));

		Instant instant = middleLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
		return Date.from(instant);
	}

	public static Period dividePeriodByTwo(Period period) {
		int years = period.getYears() / 2;
		int months = period.getMonths() / 2;
		int days = period.getDays() / 2;
		return Period.of(years, months, days);
	}

	static String printMiddleString(String S, String T, int N) {
		// Stores the base 26 digits after addition
		int[] a1 = new int[N + 1];

		for (int i = 0; i < N; i++) {
			a1[i + 1] = (int) S.charAt(i) - 97 + (int) T.charAt(i) - 97;
		}

		// Iterate from right to left
		// and add carry to next position
		for (int i = N; i >= 1; i--) {
			a1[i - 1] += (int) a1[i] / 26;
			a1[i] %= 26;
		}

		// Reduce the number to find the middle
		// string by dividing each position by 2
		for (int i = 0; i <= N; i++) {

			// If current value is odd,
			// carry 26 to the next index value
			if ((a1[i] & 1) != 0) {

				if (i + 1 <= N) {
					a1[i + 1] += 26;
				}
			}

			a1[i] = (int) a1[i] / 2;
		}

		String r = "";
		for (int i = 1; i <= N; i++) {
			r += (char) (a1[i] + 97);
		}
		return r;
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
		Queue<Node> queueNode = new LinkedList<Node>();
		Queue<Integer> queueInfo = new LinkedList<>();

		int level = 0;
		queueNode.add(Current);
		queueInfo.add(level);
		queueInfo.add(n);
		while (queueNode.isEmpty() == false) {
			Current = queueNode.poll();
			if (Current instanceof NonLeaf) {
				NonLeaf temp = (NonLeaf) Current;
				level = queueInfo.poll();
				int node = queueInfo.poll();

				System.out.println("Level " + level + " Node no." + node);
				if (level - 1 >= 0) {
					int parentnode = queueInfo.poll();
					System.out.println("--NonLeaf-- Parent at level:" + (level - 1) + " Node " + parentnode);
				} else {
					System.out.println("--NonLeaf-- Root");
				}

				System.out.println("----------------------------------------");

				level++;

				n = -1;
				queueNode.add(temp.left0);
				queueInfo.add(level);
				queueInfo.add(++n);
				queueInfo.add(node);
				queueNode.add(temp.left1);
				queueInfo.add(level);
				queueInfo.add(++n);
				queueInfo.add(node);
				queueNode.add(temp.left2);
				queueInfo.add(level);
				queueInfo.add(++n);
				queueInfo.add(node);
				queueNode.add(temp.left3);
				queueInfo.add(level);
				queueInfo.add(++n);
				queueInfo.add(node);
				queueNode.add(temp.right3);
				queueInfo.add(level);
				queueInfo.add(++n);
				queueInfo.add(node);
				queueNode.add(temp.right2);
				queueInfo.add(level);
				queueInfo.add(++n);
				queueInfo.add(node);
				queueNode.add(temp.right1);
				queueInfo.add(level);
				queueInfo.add(++n);
				queueInfo.add(node);
				queueNode.add(temp.right0);
				queueInfo.add(level);
				queueInfo.add(++n);
				queueInfo.add(node);
			} else {
				Leaf temp = (Leaf) Current;
				level = queueInfo.poll();
				int node = queueInfo.poll();

				System.out.println("Level " + level + " Node no." + node);
				if (level - 1 >= 0) {
					int parentnode = queueInfo.poll();
					System.out.println("--Leaf-- Parent at level:" + (level - 1) + " Node " + parentnode);
				} else {
					System.out.println("--Leaf-- Root");
				}
				for (int i = 0; i < temp.getBucket().size(); i++) {
					Hashtable h = temp.getBucket().get(i);
					System.out.println(i + " " + h.toString());

				}
				System.out.println("----------------------------------------");

			}
		}
	}

	private void insertIncorrectLeaf(Hashtable<String, Object> keyrec, NonLeaf node) {
		boolean higherX = false, higherY = false, higherZ = false;
		Node current = node;
		higherX = getHigherX(current.getMinX(), current.getMaxX(), keyrec);
		higherY = getHigherY(current.getMinY(), current.getMaxY(), keyrec);
		higherZ = getHigherZ(current.getMinZ(), current.getMaxZ(), keyrec);

		String r = get3BitString(higherX, higherY, higherZ);
		Leaf leaf = null;
		switch (r) {
		case "000":
			leaf = ((Leaf) node.left0);
			break;
		case "001":
			leaf = ((Leaf) node.left1);

			break;
		case "010":
			leaf = ((Leaf) node.left2);

			break;
		case "011":
			leaf = ((Leaf) node.left3);

			break;
		case "100":
			leaf = ((Leaf) node.right3);

			break;
		case "101":
			leaf = ((Leaf) node.right2);

			break;
		case "110":
			leaf = ((Leaf) node.right1);

			break;
		case "111":
			leaf = ((Leaf) node.right0);

			break;
		}
		if (leaf.getSize() == this.MaxRowsinNode) {
			this.insertTupleInIndex(keyrec);
			return;
		}
		if (leaf.IsDup(keyrec)) {
			leaf.getOverflow().add(keyrec);

		} else {
			leaf.getBucket().add(keyrec);
			leaf.setSize(leaf.getSize() + 1);
		}
	}

	public void updateTupleReferenceInIndex(Hashtable<String, Object> keyrec, String pageName, String oldPageName) {
		Node current = this.root;
		NonLeaf Parent = null;
		String positionOfLeaf = "";
		Object val1 = keyrec.get(this.getX());
		Object val2 = keyrec.get(this.getY());
		Object val3 = keyrec.get(this.getZ());
		Object val4 = oldPageName;
		while (current != null) {

			if (current instanceof Leaf) {

				Leaf curLeaf = (Leaf) current;
				for (int i = 0; i < curLeaf.getBucket().size(); i++) {

					if (curLeaf.getBucket().get(i).get(this.getX()).equals(val1)
							&& curLeaf.getBucket().get(i).get(this.getY()).equals(val2)
							&& curLeaf.getBucket().get(i).get(this.getZ()).equals(val3)
							&& curLeaf.getBucket().get(i).get("Page Name").equals(val4)) {

						curLeaf.getBucket().get(i).put("Page Name", pageName);
						return;
					}

				}
				for (int i = 0; i < curLeaf.getOverflow().size(); i++) {
					if (curLeaf.getOverflow().get(i).get(this.getX()).equals(val1)
							&& curLeaf.getOverflow().get(i).get(this.getY()).equals(val2)
							&& curLeaf.getOverflow().get(i).get(this.getZ()).equals(val3)
							&& curLeaf.getOverflow().get(i).get("Page Name").equals(val4)) {
						curLeaf.getOverflow().get(i).put("Page Name", pageName);
						return;

					}
				}

				current = null;
				return;

			} else { // current is NonLeaf

				NonLeaf curNonleaf = (NonLeaf) current;
				boolean higherX = false, higherY = false, higherZ = false;
				higherX = getHigherX(current.getMinX(), current.getMaxX(), keyrec);
				higherY = getHigherY(current.getMinY(), current.getMaxY(), keyrec);
				higherZ = getHigherZ(current.getMinZ(), current.getMaxZ(), keyrec);

				String r = get3BitString(higherX, higherY, higherZ);
				positionOfLeaf = r;
				switch (r) {
				case "000":
					current = curNonleaf.left0;
					break;
				case "001":
					current = curNonleaf.left1;
					break;
				case "010":
					current = curNonleaf.left2;
					break;
				case "011":
					current = curNonleaf.left3;
					break;
				case "100":
					current = curNonleaf.right3;
					break;
				case "101":
					current = curNonleaf.right2;
					break;
				case "110":
					current = curNonleaf.right1;
					break;
				case "111":
					current = curNonleaf.right0;
					break;
				}

			}
		}

	}

	public Vector<String> getPageName(Hashtable<String, Object> key) {
		Node current = root;
		NonLeaf parent = null;
		Vector<String> res = new Vector<>();
		while (current != null) {
			if (current instanceof Leaf) {
				Leaf myLeaf = (Leaf) current;
				for (int i = 0; i < myLeaf.getBucket().size(); i++) {
					if (myLeaf.getBucket().get(i).equals(key)) {
						res.add(myLeaf.getBucket().get(i).get("Page Name").toString());
					}
				}
				for (int i = 0; i < myLeaf.getOverflow().size(); i++) {
					if (myLeaf.getOverflow().get(i).equals(key)) {
						res.add(myLeaf.getOverflow().get(i).get("Page Name").toString());
					}
				}
				break;
			} else {
				NonLeaf curNonleaf = (NonLeaf) current;
				boolean higherX = false, higherY = false, higherZ = false;
				higherX = getHigherX(current.getMinX(), current.getMaxX(), key);
				higherY = getHigherY(current.getMinY(), current.getMaxY(), key);
				higherZ = getHigherZ(current.getMinZ(), current.getMaxZ(), key);

				String r = get3BitString(higherX, higherY, higherZ);
				switch (r) {
				case "000":
					parent = curNonleaf;
					current = curNonleaf.left0;
					break;
				case "001":
					parent = curNonleaf;
					current = curNonleaf.left1;
					break;
				case "010":
					parent = curNonleaf;
					current = curNonleaf.left2;
					break;
				case "011":
					parent = curNonleaf;
					current = curNonleaf.left3;
					break;
				case "100":
					parent = curNonleaf;
					current = curNonleaf.right3;
					break;
				case "101":
					parent = curNonleaf;
					current = curNonleaf.right2;
					break;
				case "110":
					parent = curNonleaf;
					current = curNonleaf.right1;
					break;
				case "111":
					parent = curNonleaf;
					current = curNonleaf.right0;
					break;
				}
			}
		}
		return res;
	}

	public Hashtable<String, Object> searchForPageNameUsingIndex(Hashtable<String, Object> key, String clustKey,
			Node current,  Object closest, String pageName) {
		if (current == null)
			return null;

		if (current instanceof Leaf) {
			Leaf myLeaf = (Leaf) current;
			String page = pageName;

			if (closest instanceof java.lang.Integer) {
				Integer  closestInt = (Integer) closest;
				
				for (int i = 0; i < myLeaf.getBucket().size(); i++) {
					Integer curTuple = ((Integer) myLeaf.getBucket().get(i).get(clustKey));
					if(!closestInt.equals((int)1e6)) {
						if(closestInt.compareTo(((Integer) key.get(clustKey)))<0) {
							if (curTuple.compareTo(closestInt) > 0) {
								closestInt = curTuple;
								page = (String) myLeaf.getBucket().get(i).get("Page Name");
							}
						}
						else {
							if ((((Integer) key.get(clustKey))).compareTo(curTuple) > 0)
								continue;
							if (curTuple.compareTo(closestInt) < 0) {
								closestInt = curTuple;
								page = (String) myLeaf.getBucket().get(i).get("Page Name");
							}
						}
					}
					else {
						closestInt = curTuple;
						page = (String) myLeaf.getBucket().get(i).get("Page Name");
					}

				}
				Hashtable<String, Object> hash = new Hashtable<>();
				hash.put("closest", closestInt);
				hash.put("Page Name", page);
				return hash;
			}
			if (closest instanceof java.lang.Double) {
				Double closestDouble = (Double) closest;
				
				for (int i = 0; i < myLeaf.getBucket().size(); i++) {
					Double curTuple = ((Double) myLeaf.getBucket().get(i).get(clustKey));
					if(!closestDouble.equals(1e6)) {
						if(closestDouble.compareTo(((Double) key.get(clustKey)))<0) {
							if (curTuple.compareTo(closestDouble) > 0) {
								closestDouble = curTuple;
								page = (String) myLeaf.getBucket().get(i).get("Page Name");
							}
						}
						else {
							if ((((Double) key.get(clustKey))).compareTo(curTuple) > 0)
								continue;
							if (curTuple.compareTo(closestDouble) < 0) {
								closestDouble = curTuple;
								page = (String) myLeaf.getBucket().get(i).get("Page Name");
							}
						}
					}
					else {
						closestDouble = curTuple;
						page = (String) myLeaf.getBucket().get(i).get("Page Name");
					}

				}
				Hashtable<String, Object> hash = new Hashtable<>();
				hash.put("closest", closestDouble);
				hash.put("Page Name", page);
				return hash;
			}
			if (closest instanceof java.lang.String) {
				String  closestString = (String) closest;
			
				for (int i = 0; i < myLeaf.getBucket().size(); i++) {
					String curTuple = ((String) myLeaf.getBucket().get(i).get(clustKey)).toString().toLowerCase();
					if(!closest.equals("ZZZZZZZZZZZ")) {
						closestString=closestString.toLowerCase();
						System.out.println(closest.toString()+" "+key.get(clustKey));
						if(closestString.compareTo(((String) key.get(clustKey)).toLowerCase())<0) {
							if (curTuple.compareTo(closestString) > 0) {
								closestString = curTuple;
								page = (String) myLeaf.getBucket().get(i).get("Page Name");
							}
						}
						else {
							if ((((String) key.get(clustKey)).toLowerCase()).compareTo(curTuple) > 0)
								continue;
							if (curTuple.compareTo(closestString) < 0) {
								closestString = curTuple;
								page = (String) myLeaf.getBucket().get(i).get("Page Name");
							}
						}
					}
					else {
						closestString = curTuple;
						page = (String) myLeaf.getBucket().get(i).get("Page Name");
					}

				}
				Hashtable<String, Object> hash = new Hashtable<>();
				hash.put("closest", closestString);
				hash.put("Page Name", page);
				return hash;
			}

			if (closest instanceof java.util.Date) {
				Date closestDate = (Date) closest;
				
				for (int i = 0; i < myLeaf.getBucket().size(); i++) {
					Date curTuple = ((Date) myLeaf.getBucket().get(i).get(clustKey));
					Date date=new Date(9999-1900,12-1,31);
					if(!closestDate.equals(date)) {
						if(closestDate.compareTo(((Date) key.get(clustKey)))<0) {
							if (curTuple.compareTo(closestDate) > 0) {
								closestDate = curTuple;
								page = (String) myLeaf.getBucket().get(i).get("Page Name");
							}
						}
						else {
							if ((((Date) key.get(clustKey))).compareTo(curTuple) > 0)
								continue;
							if (curTuple.compareTo(closestDate) < 0) {
								closestDate = curTuple;
								page = (String) myLeaf.getBucket().get(i).get("Page Name");
							}
						}
					}
					else {
						closestDate = curTuple;
						page = (String) myLeaf.getBucket().get(i).get("Page Name");
					}

				}
				Hashtable<String, Object> hash = new Hashtable<>();
				hash.put("closest", closestDate);
				hash.put("Page Name", page);
				return hash;
			}
			return null;

		} else {
			NonLeaf curNonleaf = (NonLeaf) current;

			if (this.getX().equals(clustKey)) {
				boolean higherX = getHigherX(current.getMinX(), current.getMaxX(), key);
				if (!higherX) {
					if(curNonleaf.left0 instanceof Leaf && ((Leaf)curNonleaf.left0).getBucket().size()==0
							&& curNonleaf.left1 instanceof Leaf && ((Leaf)curNonleaf.left1).getBucket().size()==0
							&& curNonleaf.left2 instanceof Leaf && ((Leaf)curNonleaf.left2).getBucket().size()==0
							&& curNonleaf.left3 instanceof Leaf && ((Leaf)curNonleaf.left3).getBucket().size()==0) {
						Hashtable<String, Object> hash = new Hashtable<>();
						hash.put("closest", closest);
						hash.put("Page Name", "0");
						return hash;
						
					}
					Hashtable<String, Object> hash1 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left0,
							closest, pageName);// 000
					Hashtable<String, Object> hash2 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left1,
							closest, pageName);// 001
					Hashtable<String, Object> hash3 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left2,
							closest, pageName);// 010
					Hashtable<String, Object> hash4 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left3,
							closest, pageName);//
					
					Object val1=hash1.get("closest");
					Object val2=hash2.get("closest");
					Object val3=hash3.get("closest");
					Object val4=hash4.get("closest");

					Object closestObj=getClosest(val1,val2,val3,val4,key.get(clustKey));
					if (closestObj.equals (hash1.get("closest"))) {
						return hash1;
					}
					if (closestObj.equals (hash2.get("closest"))) {
						return hash2;
					}

					if (closestObj.equals (hash3.get("closest"))) {
						return hash3;
					}

					if (closestObj.equals (hash4.get("closest"))) {
						return hash4;
					}


				} else {
					if(curNonleaf.right3 instanceof Leaf && ((Leaf)curNonleaf.right3).getBucket().size()==0
							&& curNonleaf.right2 instanceof Leaf && ((Leaf)curNonleaf.right2).getBucket().size()==0
							&& curNonleaf.right1 instanceof Leaf && ((Leaf)curNonleaf.right1).getBucket().size()==0
							&& curNonleaf.right0 instanceof Leaf && ((Leaf)curNonleaf.right0).getBucket().size()==0) {
						Hashtable<String, Object> hash = new Hashtable<>();
						hash.put("closest", closest);
						hash.put("Page Name", "0");
						return hash;
						
					}
					Hashtable<String, Object> hash1 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right3,
							closest, pageName);// 100
					Hashtable<String, Object> hash2 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right2,
							closest, pageName);// 101
					Hashtable<String, Object> hash3 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right1,
							closest, pageName);// 110
					Hashtable<String, Object> hash4 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right0,
							closest, pageName);// 111
					Object val1=hash1.get("closest");
					Object val2=hash2.get("closest");
					Object val3=hash3.get("closest");
					Object val4=hash4.get("closest");

					Object closestObj=getClosest(val1,val2,val3,val4,key.get(clustKey));
					if (closestObj.equals (hash1.get("closest"))) {
						return hash1;
					}
					if (closestObj.equals (hash2.get("closest"))) {
						return hash2;
					}

					if (closestObj.equals (hash3.get("closest"))) {
						return hash3;
					}

					if (closestObj.equals (hash4.get("closest"))) {
						return hash4;
					}
				}

			} else {
				if (this.getY().equals(clustKey)) {
					boolean higherY = getHigherY(current.getMinY(), current.getMaxY(), key);
					if (!higherY) {
						if(curNonleaf.left0 instanceof Leaf && ((Leaf)curNonleaf.left0).getBucket().size()==0
								&& curNonleaf.left1 instanceof Leaf && ((Leaf)curNonleaf.left1).getBucket().size()==0
								&& curNonleaf.right3 instanceof Leaf && ((Leaf)curNonleaf.right3).getBucket().size()==0
								&& curNonleaf.right2 instanceof Leaf && ((Leaf)curNonleaf.right2).getBucket().size()==0) {
							Hashtable<String, Object> hash = new Hashtable<>();
							hash.put("closest", closest);
							hash.put("Page Name", "0");
							return hash;
							
						}
						Hashtable<String, Object> hash1 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left0,
								closest, pageName);//000
						Hashtable<String, Object> hash2 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left1,
								closest, pageName);//001
						Hashtable<String, Object> hash3 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right3,
								closest, pageName);//100
						Hashtable<String, Object> hash4 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right2,
								closest, pageName);//101
						Object val1=hash1.get("closest");
						Object val2=hash2.get("closest");
						Object val3=hash3.get("closest");
						Object val4=hash4.get("closest");

						Object closestObj=getClosest(val1,val2,val3,val4,key.get(clustKey));
//						System.out.println(hash1.toString()+" "+hash2.toString()+" "+hash3.toString()+" "+hash4.toString());
//						System.out.println(closestObj+"    closestObj");

						if (closestObj.equals (hash1.get("closest"))) {
							return hash1;
						}
						if (closestObj.equals (hash2.get("closest"))) {
							return hash2;
						}

						if (closestObj.equals (hash3.get("closest"))) {
							return hash3;
						}

						if (closestObj.equals (hash4.get("closest"))) {
							return hash4;
						}
					} else {
						if(curNonleaf.left2 instanceof Leaf && ((Leaf)curNonleaf.left2).getBucket().size()==0
								&& curNonleaf.left3 instanceof Leaf && ((Leaf)curNonleaf.left3).getBucket().size()==0
								&& curNonleaf.right1 instanceof Leaf && ((Leaf)curNonleaf.right1).getBucket().size()==0
								&& curNonleaf.right0 instanceof Leaf && ((Leaf)curNonleaf.right0).getBucket().size()==0) {
							Hashtable<String, Object> hash = new Hashtable<>();
							hash.put("closest", closest);
							hash.put("Page Name", "0");
							return hash;
							
						}
						Hashtable<String, Object> hash1 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left2,
								closest, pageName);
						Hashtable<String, Object> hash2 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left3,
								closest, pageName);
						Hashtable<String, Object> hash3 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right1,
								closest, pageName);
						Hashtable<String, Object> hash4 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right0,
								closest, pageName);
						Object val1=hash1.get("closest");
						Object val2=hash2.get("closest");
						Object val3=hash3.get("closest");
						Object val4=hash4.get("closest");

						Object closestObj=getClosest(val1,val2,val3,val4,key.get(clustKey));
//						System.out.println(closestObj+"    closestObj");
//						System.out.println(hash1.toString()+" "+hash2.toString()+" "+hash3.toString()+" "+hash4.toString());

						if (closestObj.equals (hash1.get("closest"))) {
							return hash1;
						}
						if (closestObj.equals (hash2.get("closest"))) {
							return hash2;
						}

						if (closestObj.equals (hash3.get("closest"))) {
							return hash3;
						}

						if (closestObj.equals (hash4.get("closest"))) {
							return hash4;
						}
					}
				} else {
					boolean higherZ = getHigherZ(current.getMinZ(), current.getMaxZ(), key);
					if (!higherZ) {
						if(curNonleaf.left0 instanceof Leaf && ((Leaf)curNonleaf.left0).getBucket().size()==0
								&& curNonleaf.left2 instanceof Leaf && ((Leaf)curNonleaf.left2).getBucket().size()==0
								&& curNonleaf.right3 instanceof Leaf && ((Leaf)curNonleaf.right3).getBucket().size()==0
								&& curNonleaf.right1 instanceof Leaf && ((Leaf)curNonleaf.right1).getBucket().size()==0) {
							Hashtable<String, Object> hash = new Hashtable<>();
							hash.put("closest", closest);
							hash.put("Page Name", "0");
							return hash;
							
						}
					
						Hashtable<String, Object> hash1 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left0,
								closest, pageName);// 000
						Hashtable<String, Object> hash2 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left2,
								closest, pageName);// 010
						Hashtable<String, Object> hash3 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right3,
								closest, pageName);// 100
						Hashtable<String, Object> hash4 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right1,
								closest, pageName);// 110
						Object val1=hash1.get("closest");
						Object val2=hash2.get("closest");
						Object val3=hash3.get("closest");
						Object val4=hash4.get("closest");

						Object closestObj=getClosest(val1,val2,val3,val4,key.get(clustKey));
						if (closestObj.equals (hash1.get("closest"))) {
							return hash1;
						}
						if (closestObj.equals (hash2.get("closest"))) {
							return hash2;
						}

						if (closestObj.equals (hash3.get("closest"))) {
							return hash3;
						}

						if (closestObj.equals (hash4.get("closest"))) {
							return hash4;
						}
					} else {
						if(curNonleaf.left1 instanceof Leaf && ((Leaf)curNonleaf.left1).getBucket().size()==0
								&& curNonleaf.left3 instanceof Leaf && ((Leaf)curNonleaf.left3).getBucket().size()==0
								&& curNonleaf.right2 instanceof Leaf && ((Leaf)curNonleaf.right2).getBucket().size()==0
								&& curNonleaf.right0 instanceof Leaf && ((Leaf)curNonleaf.right0).getBucket().size()==0) {
							Hashtable<String, Object> hash = new Hashtable<>();
							hash.put("closest", closest);
							hash.put("Page Name", "0");
							return hash;
							
						}
						Hashtable<String, Object> hash1 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left1,
								closest, pageName);
						Hashtable<String, Object> hash2 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left3,
								closest, pageName);
						Hashtable<String, Object> hash3 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right2,
								closest, pageName);
						Hashtable<String, Object> hash4 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right0,
								closest, pageName);
						Object val1=hash1.get("closest");
						Object val2=hash2.get("closest");
						Object val3=hash3.get("closest");
						Object val4=hash4.get("closest");

						Object closestObj=getClosest(val1,val2,val3,val4,key.get(clustKey));
						if (closestObj.equals (hash1.get("closest"))) {
							return hash1;
						}
						if (closestObj.equals (hash2.get("closest"))) {
							return hash2;
						}

						if (closestObj.equals (hash3.get("closest"))) {
							return hash3;
						}

						if (closestObj.equals (hash4.get("closest"))) {
							return hash4;
						}
					}
				}
			}
		}
		return null;

	}

	public String searchForPageNameUsingIndex(Hashtable<String, Object> key, String clustKey, String max) {
		Hashtable<String, Object> hash=new Hashtable<>();
		
		if(key.get(clustKey)instanceof java.lang.Integer)
			hash = searchForPageNameUsingIndex(key, clustKey, this.root, (int) 1e6, "");
		if(key.get(clustKey)instanceof java.lang.Double)
			hash = searchForPageNameUsingIndex(key, clustKey, this.root,  1e6, "");
		if(key.get(clustKey)instanceof java.lang.String)
			hash = searchForPageNameUsingIndex(key, clustKey, this.root, "ZZZZZZZZZZZ", "");
		if(key.get(clustKey)instanceof java.util.Date) {
			Date date;
			try {
				date = new SimpleDateFormat("yyyy-MM-dd").parse("9999-12-31");
				hash = searchForPageNameUsingIndex(key, clustKey, this.root,  date,"");

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		String page = (String) hash.get("Page Name");
		if (page == "")
			return "";
		else
			return page.charAt(page.length() - 1) + "";
	}

	public String getExactPage(String ClustCol, Node current, Object clust) {
		if (current == null)
			return null;

		if (current instanceof Leaf) {
			Leaf myLeaf = (Leaf) current;

			for (int i = 0; i < myLeaf.getBucket().size(); i++) {
				Hashtable<String, Object> record = myLeaf.getBucket().get(i);
				//System.out.println(record.get("Clust key")+" "+clust);
				if (clust.equals(record.get(ClustCol))) {
					return (String) record.get("Page Name");
				}

			}
			for (int i = 0; i < myLeaf.getOverflow().size(); i++) {
				Hashtable<String, Object> record = myLeaf.getBucket().get(i);

				if (clust.equals(record.get(ClustCol))) {
					return (String) record.get("Page Name");
				}
			}
			return "";
		} else {
			ArrayList<String> nextNodes = new ArrayList<String>();
			nextNodes.add("000");
			nextNodes.add("001");
			nextNodes.add("010");
			nextNodes.add("011");
			nextNodes.add("100");
			nextNodes.add("101");
			nextNodes.add("110");
			nextNodes.add("111");

			ArrayList<String> z = new ArrayList<String>();

			if (ClustCol.toLowerCase().equals(getX())) {
				Hashtable<String,Object>tmp=new Hashtable<String,Object>();
				tmp.put(getX(),clust);
				boolean higherX = getHigherX(current.getMinX(), current.getMaxX(), tmp);
				if (higherX) {
					nextNodes.remove("000");
					nextNodes.remove("001");
					nextNodes.remove("010");
					nextNodes.remove("011");
				} else {
					nextNodes.remove("100");
					nextNodes.remove("101");
					nextNodes.remove("110");
					nextNodes.remove("111");
				}

			}
			if (ClustCol.toLowerCase().equals(getY())) {
				Hashtable<String,Object>tmp=new Hashtable<String,Object>();
				tmp.put(getY(),clust);
				boolean higherY = getHigherY(current.getMinY(), current.getMaxY(), tmp);
				if (higherY) {
					nextNodes.remove("000");
					nextNodes.remove("001");
					nextNodes.remove("100");
					nextNodes.remove("101");
				} else {
					nextNodes.remove("010");
					nextNodes.remove("011");
					nextNodes.remove("110");
					nextNodes.remove("111");
				}
			}
			
			if (ClustCol.toLowerCase().equals(getZ())) {
				Hashtable<String,Object>tmp=new Hashtable<String,Object>();
				tmp.put(getZ(),clust);
				boolean higherZ = getHigherZ(current.getMinZ(), current.getMaxZ(), tmp);
				if (higherZ) {
					nextNodes.remove("000");
					nextNodes.remove("010");
					nextNodes.remove("100");
					nextNodes.remove("110");
				} else {
					nextNodes.remove("001");
					nextNodes.remove("011");
					nextNodes.remove("101");
					nextNodes.remove("111");
				}

			}
			System.out.println(nextNodes.toString());
			NonLeaf NonleafCur = (NonLeaf) current;
			String res = "";
			if (nextNodes.contains("000"))
				res += getExactPage(ClustCol, NonleafCur.left0, clust);
			if (nextNodes.contains("001"))
				res += getExactPage(ClustCol, NonleafCur.left1, clust);
			if (nextNodes.contains("010"))
				res += getExactPage(ClustCol, NonleafCur.left2, clust);
			if (nextNodes.contains("011"))
				res += getExactPage(ClustCol, NonleafCur.left3, clust);
			if (nextNodes.contains("100"))
				res += getExactPage(ClustCol, NonleafCur.right3, clust);
			if (nextNodes.contains("101"))
				res += getExactPage(ClustCol, NonleafCur.right2, clust);
			if (nextNodes.contains("110"))
				res += getExactPage(ClustCol, NonleafCur.right1, clust);
			if (nextNodes.contains("111"))
				res += getExactPage(ClustCol, NonleafCur.right0, clust);
			return res;

		}

	}
	public static Object getClosest(Object obj1,Object obj2,Object obj3,Object obj4,Object Clustkey) {
		if (obj1 instanceof java.lang.Integer){
			Integer min1,min2,min;
			ArrayList<Integer>toCompare=new ArrayList<>();
			//System.out.println(obj1.toString()+" "+obj2.toString()+" "+obj3.toString()+" "+obj4.toString());
			if(!((Integer)obj1).equals((int)1e6)) toCompare.add(((Integer)obj1));
			if(!((Integer)obj2).equals((int)1e6)) toCompare.add(((Integer)obj2));
			if(!((Integer)obj3).equals((int)1e6)) toCompare.add(((Integer)obj3));
			if(!((Integer)obj4).equals((int)1e6)) toCompare.add(((Integer)obj4));
			if(toCompare.size()==0)return ((Integer)obj1);
			min=toCompare.remove(0);
			while(toCompare.size()>0) {
				min2=toCompare.remove(0);
				if(min.compareTo(((Integer)Clustkey))<0) {
					if(min2.compareTo(((Integer)Clustkey))<0) {
						if(min.compareTo(min2)<0)
							min=min2;
						}
					else
						min=min2;
				}
				else {
					if(min2.compareTo(((Integer)Clustkey))>0) {
						if(min.compareTo(min2)>0)
							min=min2;
					}
				}
				
			}
			return min;			
							
			}
		if (obj1 instanceof java.lang.Double){
			Double min1,min2,min;
			ArrayList<Double>toCompare=new ArrayList<>();
			//System.out.println(obj1.toString()+" "+obj2.toString()+" "+obj3.toString()+" "+obj4.toString());
			if(!((Double)obj1).equals(1e6)) toCompare.add(((Double)obj1));
			if(!((Double)obj2).equals(1e6)) toCompare.add(((Double)obj2));
			if(!((Double)obj3).equals(1e6)) toCompare.add(((Double)obj3));
			if(!((Double)obj4).equals(1e6)) toCompare.add(((Double)obj4));
			if(toCompare.size()==0)return ((Double)obj1);
			min=toCompare.remove(0);
			while(toCompare.size()>0) {
				min2=toCompare.remove(0);
				if(min.compareTo(((Double)Clustkey))<0) {
					if(min2.compareTo(((Double)Clustkey))<0) {
						if(min.compareTo(min2)<0)
							min=min2;
						}
					else
						min=min2;
				}
				else {
					if(min2.compareTo(((Double)Clustkey))>0) {
						if(min.compareTo(min2)>0)
							min=min2;
					}
				}
				
			}
			return min;			
							
			
		}
		if (obj1 instanceof java.lang.String){
			String min1,min2,min;
			ArrayList<String>toCompare=new ArrayList<>();
			//System.out.println(obj1.toString()+" "+obj2.toString()+" "+obj3.toString()+" "+obj4.toString());
			if(!((String)obj1).equals("ZZZZZZZZZZZ")) toCompare.add(((String)obj1).toLowerCase());
			if(!((String)obj2).equals("ZZZZZZZZZZZ")) toCompare.add(((String)obj2).toLowerCase());
			if(!((String)obj3).equals("ZZZZZZZZZZZ")) toCompare.add(((String)obj3).toLowerCase());
			if(!((String)obj4).equals("ZZZZZZZZZZZ")) toCompare.add(((String)obj4).toLowerCase());
			if(toCompare.size()==0)return ((String)obj1);
			min=toCompare.remove(0);
			while(toCompare.size()>0) {
				min2=toCompare.remove(0);
				if(min.compareTo(((String)Clustkey).toLowerCase())<0) {
					if(min2.compareTo(((String)Clustkey).toLowerCase())<0) {
						if(min.compareTo(min2)<0)
							min=min2;
						}
					else
						min=min2;
				}
				else {
					if(min2.compareTo(((String)Clustkey).toLowerCase())>0) {
						if(min.compareTo(min2)>0)
							min=min2;
					}
				}
				
			}
			return min;			
							
			}
		if (obj1 instanceof java.util.Date){
			Date min1,min2,min;
			ArrayList<Date>toCompare=new ArrayList<>();
			Date d=new Date(9999-1900,12-1,31);
			//System.out.println(obj1.toString()+" "+obj2.toString()+" "+obj3.toString()+" "+obj4.toString());
			if(!((Date)obj1).equals(d)) toCompare.add(((Date)obj1));
			if(!((Date)obj2).equals(d)) toCompare.add(((Date)obj2));
			if(!((Date)obj3).equals(d)) toCompare.add(((Date)obj3));
			if(!((Date)obj4).equals(d)) toCompare.add(((Date)obj4));
			if(toCompare.size()==0)return ((Integer)obj1);
			min=toCompare.remove(0);
			while(toCompare.size()>0) {
				min2=toCompare.remove(0);
				if(min.compareTo(((Date)Clustkey))<0) {
					if(min2.compareTo(((Date)Clustkey))<0) {
						if(min.compareTo(min2)<0)
							min=min2;
						}
					else
						min=min2;
				}
				else {
					if(min2.compareTo(((Date)Clustkey))>0) {
						if(min.compareTo(min2)>0)
							min=min2;
					}
				}
				
			}
			return min;			
							
			}
		return null;
	}
	public static void main(String[] args) {
		Octree tree = new Octree("X", "Y", "Z", 0, 10, 0, 20, 0, 40);
		Hashtable<String, Object> key = new Hashtable<>();
		Hashtable<String, Object> key1 = new Hashtable<>();
		key1.put("x", 2);
		key1.put("y", 3);
		key1.put("z", 6);
		key1.put("Page Name", "Student0");
		tree.insertTupleInIndex(key1);// 000
		key = new Hashtable<>();
		key.put("x", 6);
		key.put("y", 3);
		key.put("z", 5);
		key.put("Page Name", "Student1");
		tree.insertTupleInIndex(key);// 100
		key = new Hashtable<>();
		key.put("x", 2);
		key.put("y", 3);
		key.put("z", 25);
		key.put("Page Name", "Student2");
		tree.insertTupleInIndex(key);// 001
		key = new Hashtable<>();
		tree.displayTree2();

		key.put("x", 2);
		key.put("y", 13);
		key.put("z", 6);
		key.put("Page Name", "Student3");
		tree.insertTupleInIndex(key);// 010
		Hashtable key4 = new Hashtable<>();
		key4.put("x", 7);
		key4.put("y", 3);
		key4.put("z", 27);
		key4.put("Page Name", "Student4");
		tree.insertTupleInIndex(key4);// 101
		Hashtable key2 = new Hashtable<>();

		key2.put("x", 2);
		key2.put("y", 3);
		key2.put("z", 9);
		key2.put("Page Name", "Student5");
		tree.insertTupleInIndex(key2);// 000
		Hashtable key3 = new Hashtable<>();

		key3.put("x", 1);
		key3.put("y", 8);
		key3.put("z", 2);
		key3.put("Page Name", "Student6");
		tree.insertTupleInIndex(key3);// 000
		key = new Hashtable<>();

		key.put("x", 7);
		key.put("y", 11);
		key.put("z", 26);
		key.put("Page Name", "Student7");
		tree.insertTupleInIndex(key);// 111
		key = new Hashtable<>();

		key.put("x", 4);
		key.put("y", 9);
		key.put("z", 18);
		key.put("Page Name", "Student8");
		tree.insertTupleInIndex(key);// 000 //4th element in 000
		System.out.println("\n\n");
//			tree.deleteTuple(key);
//			tree.deleteTuple(key1);
//			tree.deleteTuple(key2);
//			tree.deleteTuple(key3);
//			tree.deleteTuple(key4);
		tree.displayTree2();
//			Hashtable<String, Object> key9 = new Hashtable<>();
//			key9.put("X", 2);
//			key9.put("Y", 3);
//			key9.put("Z", 6);
//			key9.put("Page Name", "Student0");
//			tree.insertTupleInIndex(key9);// 000
//			tree.displayTree2();

	}
}