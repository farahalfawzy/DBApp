import java.io.FileInputStream;
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
public class Octree {
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
			//return 4;
		} catch (Exception ex) {
			return -1;
		}
	}

	public Octree(String X, String Y, String Z, Object MinX, Object MaxX, Object MinY, Object MaxY, Object MinZ, Object MaxZ) {
		this.root = new Leaf(MinX, MaxX, MinY, MaxY, MinZ, MaxZ);
		this.X = X;
		this.Y = Y;
		this.Z = Z;
		this.MaxRowsinNode=MaxRowsinNode();
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
				higherX = getHigherX(current.getMinX(),current.getMaxX(),key);
				higherY = getHigherY(current.getMinY(),current.getMaxY(),key);
				higherZ = getHigherZ(current.getMinZ(),current.getMaxZ(),key);
				
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

	private boolean getHigherZ(Object minZ, Object maxZ, Hashtable<String, Object> key) {
		if(minZ instanceof Integer && maxZ instanceof Integer) {
			Object midX = ((Integer.parseInt(minZ.toString())) + ((Integer.parseInt(maxZ.toString())))) / 2;
			if (Integer.parseInt(key.get(getZ()).toString()) > Integer.parseInt(midX.toString())) {
				return true;
			} else {
				return false;
			}
		}
		if(minZ instanceof Double && maxZ instanceof Double) {
			Object midX = ((Double.parseDouble(minZ.toString())) + ((Double.parseDouble(maxZ.toString())))) / 2;
			if (Double.parseDouble(key.get(getZ()).toString()) > Double.parseDouble(midX.toString())) {
				return true;
			} else {
				return false;
			}
		}
		if(minZ instanceof String && maxZ instanceof String) {
			String midX = printMiddleString(minZ.toString().toLowerCase(),maxZ.toString().toLowerCase(),minZ.toString().length());
			if(key.get(getZ()).toString().compareTo(midX)>0)
				return true;
			else
				return false;
		}
		if(minZ instanceof java.util.Date && maxZ instanceof java.util.Date) {
			try {
			Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse(minZ.toString());
			Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse(maxZ.toString());
			Date currDate = new SimpleDateFormat("yyyy-MM-dd").parse(key.get(getZ()).toString());
			Date midX = findMidPoint(minDate,maxDate);
			if(currDate.after(midX))
				return true;
			else
				return false;
			}catch(ParseException e) {}
		}
		return false;
	}

	private boolean getHigherY(Object minY, Object maxY, Hashtable<String, Object> key) {
		if(minY instanceof Integer && maxY instanceof Integer) {
			Object midX = ((Integer.parseInt(minY.toString())) + ((Integer.parseInt(maxY.toString())))) / 2;
			if (Integer.parseInt(key.get(getY()).toString()) > Integer.parseInt(midX.toString())) {
				return true;
			} else {
				return false;
			}
		}
		if(minY instanceof Double && maxY instanceof Double) {
			Object midX = ((Double.parseDouble(minY.toString())) + ((Double.parseDouble(maxY.toString())))) / 2;
			if (Double.parseDouble(key.get(getY()).toString()) > Double.parseDouble(midX.toString())) {
				return true;
			} else {
				return false;
			}
		}
		if(minY instanceof String && maxY instanceof String) {
			String midX = printMiddleString(minY.toString().toLowerCase(),maxY.toString().toLowerCase(),minY.toString().length());
			if(key.get(getY()).toString().compareTo(midX)>0)
				return true;
			else
				return false;
		}
		if(minY instanceof java.util.Date && maxY instanceof java.util.Date) {
			try {
			Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse(minY.toString());
			Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse(maxY.toString());
			Date currDate = new SimpleDateFormat("yyyy-MM-dd").parse(key.get(getY()).toString());
			Date midX = findMidPoint(minDate,maxDate);
			if(currDate.after(midX))
				return true;
			else
				return false;
			}catch(ParseException e) {}
		}
		return false;
	}

	private boolean getHigherX(Object minX, Object maxX, Hashtable<String, Object> key) {
		if(minX instanceof Integer && maxX instanceof Integer) {
			Object midX = ((Integer.parseInt(minX.toString())) + ((Integer.parseInt(maxX.toString())))) / 2;
			if (Integer.parseInt(key.get(getX()).toString()) > Integer.parseInt(midX.toString())) {
				return true;
			} else {
				return false;
			}
		}
		if(minX instanceof Double && maxX instanceof Double) {
			Object midX = ((Double.parseDouble(minX.toString())) + ((Double.parseDouble(maxX.toString())))) / 2;
			if (Double.parseDouble(key.get(getX()).toString()) > Double.parseDouble(midX.toString())) {
				return true;
			} else {
				return false;
			}
		}
		if(minX instanceof String && maxX instanceof String) {
			String midX = printMiddleString(minX.toString().toLowerCase(),maxX.toString().toLowerCase(),minX.toString().length());
			if(key.get(getX()).toString().compareTo(midX)>0)
				return true;
			else
				return false;
		}
		if(minX instanceof java.util.Date && maxX instanceof java.util.Date) {
			try {
			Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse(minX.toString());
			Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse(maxX.toString());
			Date currDate = new SimpleDateFormat("yyyy-MM-dd").parse(key.get(getX()).toString());
			Date midX = findMidPoint(minDate,maxDate);
			if(currDate.after(midX))
				return true;
			else
				return false;
			}catch(ParseException e) {}
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
	
	static String printMiddleString(String S, String T, int N)
    {
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];
 
        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int)S.charAt(i) - 97
                        + (int)T.charAt(i) - 97;
        }
 
        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
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
 
            a1[i] = (int)a1[i] / 2;
        }
 
        String r="";
        for (int i = 1; i <= N; i++) {
            r+=(char)(a1[i] + 97);
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
				level=queueInfo.poll();
				int node=queueInfo.poll();
				
				System.out.println("Level "+level+" Node no."+node);
				if(level-1>=0) {
					int parentnode=queueInfo.poll();
					System.out.println("--NonLeaf-- Parent at level:"+(level-1)+" Node "+parentnode);
				}
				else {
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
				level=queueInfo.poll();
				int node=queueInfo.poll();
				
				System.out.println("Level "+level+" Node no."+node);
				if(level-1>=0) {
					int parentnode=queueInfo.poll();
					System.out.println("--Leaf-- Parent at level:"+(level-1)+" Node "+parentnode);
				}
				else {
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

	public void insertIncorrectLeaf(Hashtable<String, Object> key, NonLeaf node) {
		boolean higherX = false, higherY = false, higherZ = false;
		Node current = node;
		higherX = getHigherX(current.getMinX(),current.getMaxX(),key);
		higherY = getHigherY(current.getMinY(),current.getMaxY(),key);
		higherZ = getHigherZ(current.getMinZ(),current.getMaxZ(),key);
		
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
		Octree tree = new Octree("X", "Y", "Z", 0, 10, 0, 20,0, 40);
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
		//tree.displayTree2();
		key = new Hashtable<>();

		key.put("X", 2);
		key.put("Y", 13);
		key.put("Z", 6);
		tree.insertTupleInIndex(key);// 010
		//tree.displayTree2();
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
		//tree.displayTree2();
		key = new Hashtable<>();

		key.put("X", 4);
		key.put("Y", 9);
		key.put("Z", 18);
		tree.insertTupleInIndex(key);// 000 //4th element in 000
		System.out.println("\n\n");
		tree.displayTree2();
	}
}