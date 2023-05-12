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

	public void deleteTuple(Hashtable<String, Object> key) {
		Node current = root;
		NonLeaf parent = null;
		Stack<NonLeaf> myStack = new Stack();
		while (current != null) {
			if (current instanceof Leaf) {
				Leaf myLeaf = ((Leaf) current);
				myLeaf.removeFromBucket(key);
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
				myStack.push(parent);
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
System.out.println(closest.toString()+" "+key.get(clustKey));
		if (current == null)
			return null;

		if (current instanceof Leaf) {
			Leaf myLeaf = (Leaf) current;
			String page = pageName;

			if (closest instanceof java.lang.Integer) {
				int closestInt = (Integer) closest;
				for (int i = 0; i < myLeaf.getBucket().size(); i++) {
					int curTuple = ((Integer) myLeaf.getBucket().get(i).get(clustKey));
					if (((Integer) key.get(clustKey)) > curTuple)
						continue;
					int diff1 = curTuple - ((Integer) key.get(clustKey));
					int diff2 = closestInt - ((Integer) key.get(clustKey));
					if (diff1 < diff2) {
						closestInt = diff1;
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
					if (((Double) key.get(clustKey)) > curTuple)
						continue;
					Double diff1 = curTuple - ((Double) key.get(clustKey));
					Double diff2 = closestDouble - ((Double) key.get(clustKey));
					if (diff1 < diff2) {
						closestDouble = diff1;
						page = (String) myLeaf.getBucket().get(i).get("Page Name");

					}
				}
				Hashtable<String, Object> hash = new Hashtable<>();
				hash.put("closest", closestDouble);
				hash.put("Page Name", page);
				return hash;
			}
			if (closest instanceof java.lang.String) {
				String closestString = (String) closest;
				
				for (int i = 0; i < myLeaf.getBucket().size(); i++) {
					String curTuple = ((String) myLeaf.getBucket().get(i).get(clustKey)).toString();
					System.out.println(curTuple);
					if (((String) key.get(clustKey)).compareTo(curTuple) > 0)
						continue;
					if (curTuple.compareTo(closestString) < 0) {
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
					if (((Date) key.get(clustKey)).after(curTuple))
						continue;
					if (curTuple.before(closestDate)) {
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

					Object closestObj=getClosest(val1,val2,val3,val4);
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

					Object closestObj=getClosest(val1,val2,val3,val4);
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
						Hashtable<String, Object> hash1 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left0,
								closest, pageName);
						Hashtable<String, Object> hash2 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.left1,
								closest, pageName);
						Hashtable<String, Object> hash3 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right3,
								closest, pageName);
						Hashtable<String, Object> hash4 = searchForPageNameUsingIndex(key, clustKey, curNonleaf.right2,
								closest, pageName);
						Object val1=hash1.get("closest");
						Object val2=hash2.get("closest");
						Object val3=hash3.get("closest");
						Object val4=hash4.get("closest");

						Object closestObj=getClosest(val1,val2,val3,val4);
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

						Object closestObj=getClosest(val1,val2,val3,val4);
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

						Object closestObj=getClosest(val1,val2,val3,val4);
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

						Object closestObj=getClosest(val1,val2,val3,val4);
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
			hash = searchForPageNameUsingIndex(key, clustKey, this.root, max, "");
		if(key.get(clustKey)instanceof java.util.Date) {
			Date date;
			try {
				date = new SimpleDateFormat("yyyy-MM-dd").parse(max);
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

	public String getExactPage(Hashtable<String, Object> key, Node current, Object clust) {
		if (current == null)
			return null;

		if (current instanceof Leaf) {
			Leaf myLeaf = (Leaf) current;

			for (int i = 0; i < myLeaf.getBucket().size(); i++) {
				Hashtable<String, Object> record = myLeaf.getBucket().get(i);

				if (clust.equals(record.get("Clust key"))) {
					return (String) record.get("Page Name");
				}

			}
			for (int i = 0; i < myLeaf.getOverflow().size(); i++) {
				Hashtable<String, Object> record = myLeaf.getBucket().get(i);

				if (clust.equals(record.get("Clust key"))) {
					return (String) record.get("Page Name");
				}
			}
			return "";
		} else {
			ArrayList<String> x = new ArrayList<String>();
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

			if (key.get(this.getX()) != null) {
				boolean higherX = getHigherX(current.getMinX(), current.getMaxX(), key);
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
			if (key.get(this.getY()) != null) {
				boolean higherY = getHigherY(current.getMinY(), current.getMaxY(), key);
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
			if (key.get(this.getZ()) != null) {
				boolean higherZ = getHigherZ(current.getMinZ(), current.getMaxZ(), key);
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
			NonLeaf NonleafCur = (NonLeaf) current;
			String res = "";
			if (nextNodes.contains("000"))
				res += getExactPage(key, NonleafCur.left0, clust);
			if (nextNodes.contains("001"))
				res += getExactPage(key, NonleafCur.left1, clust);
			if (nextNodes.contains("010"))
				res += getExactPage(key, NonleafCur.left2, clust);
			if (nextNodes.contains("011"))
				res += getExactPage(key, NonleafCur.left3, clust);
			if (nextNodes.contains("100"))
				res += getExactPage(key, NonleafCur.right3, clust);
			if (nextNodes.contains("101"))
				res += getExactPage(key, NonleafCur.right2, clust);
			if (nextNodes.contains("110"))
				res += getExactPage(key, NonleafCur.right1, clust);
			if (nextNodes.contains("111"))
				res += getExactPage(key, NonleafCur.right0, clust);
			return res;

		}

	}
	public static Object getClosest(Object obj1,Object obj2,Object obj3,Object obj4) {
		if (obj1 instanceof java.lang.Integer){
			int min1 = Math.min(((Integer) obj1), (Integer) obj2);
			int min2 = Math.min(((Integer) obj3), (Integer) obj4);
			int min = Math.min(min1, min2);
			return min;
		}
		if (obj1 instanceof java.lang.Double){
			Double min1 = Math.min(((Double) obj1), (Double) obj2);
			Double min2 = Math.min(((Double) obj3), (Double) obj4);
			Double min = Math.min(min1, min2);
			return min;
		}
		if (obj1 instanceof java.lang.String){
			String min1,min2,min;
			if(((String)obj1).compareTo((String)obj2)<0)
				min1=((String)obj1);
			else
				min1=((String)obj2);
			if(((String)obj3).compareTo((String)obj4)<0)
				min2=((String)obj3);
			else
				min2=((String)obj4);
			if((min1).compareTo(min2)<0)
				min=min1;
			else
				min=min2;
			
			return min;
		}
		if (obj1 instanceof java.util.Date){
			Date min1,min2,min;
			if(((Date)obj1).before((Date)obj2))
				min1=((Date)obj1);
			else
				min1=((Date)obj2);
			if(((Date)obj3).before((Date)obj4))
				min2=((Date)obj3);
			else
				min2=((Date)obj4);
			if((min1).before(min2))
				min=min1;
			else
				min=min2;
			
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