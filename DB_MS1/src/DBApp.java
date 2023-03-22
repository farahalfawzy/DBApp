import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
//try git
//TODO LAW el clustering key mawgood
public class DBApp {

	Vector<Table> allTable = new Vector<Table>();
	static int maxnoOfRows = 200;

	public void init() {
		try {
			File csv = new File("./resources/metadata.csv");
			if (csv.createNewFile()) {
				System.out.println("The CSV file was created!!");
			} else {
				System.out.println("Already exists");
			}
		} catch (IOException e) {
			System.out.println("Destination not found");
		}

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		String filePath = "./resources/metadata.csv";
		String strColName = "";

		if (!tableExits(strTableName)) {
			FileWriter writer;
			try {
				writer = new FileWriter(filePath);
				// header
				for (String key : htblColNameType.keySet()) {
					strColName = key;
					String type = htblColNameType.get(key);
					String min = htblColNameMin.get(key);
					String max = htblColNameMax.get(key);
					writer.append(strTableName + ",");
					writer.append(key + ",");
					writer.append(type + ",");
					if (key == strClusteringKeyColumn)
						writer.append("True,");
					else
						writer.append("False,");
					writer.append(min + ",");
					writer.append(max + ",");
					writer.append("\n");
				}
				writer.close();
				Table myTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
						htblColNameMax);
				allTable.add(myTable);
				System.out.println("Table created successfully!");
				serializeTable(myTable, strTableName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			throw new DBAppException("Already exists!");
		}

	}

	public void inserttIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ParseException {
		if (tableExits(strTableName)) {
			try {
				// law mafesh Page
				Table t = deserializeTable(strTableName);
				if (isValidToInsert(t, htblColNameValue)) {
					Object clustKey = htblColNameValue.get(t.getClusteringKey());
					if (clustKey instanceof java.lang.Integer) {
						Page page = binarySearchInt(t, Integer.parseInt(t.getClusteringKey()));
					}
					if (clustKey instanceof java.lang.String) {
						Page page = binarySearchString(t, t.getClusteringKey().toString());
					}
					if (clustKey instanceof java.lang.Double) {
						Page page = binarySearchDouble(t, Double.parseDouble(t.getClusteringKey()));
					}
					if (clustKey instanceof java.util.Date) {
						Page page = binarySearchDate(t, new SimpleDateFormat("YYYY-MM-DD").parse(t.getClusteringKey()));
					}
					// check if page is full
					// insert ya farah <3//shokrannn

				} else {
					throw new DBAppException();
				}
			} catch (ClassNotFoundException e) {
				System.out.println("Table not found");
			} catch (DBAppException e1) {
				System.out.println("Data type wasn't valid");
			}

		}
	}

	private boolean isValidToInsert(Table table, Hashtable<String, Object> htblColNameValue) throws ParseException {
		Hashtable<String, String> htdlColNameType = table.getColNameType();
		Hashtable<String, String> htdlColNameMin = table.getColNameMin();
		Hashtable<String, String> htdlColNameMax = table.getColNameMax();
		boolean flag = true;
		for (String key : htblColNameValue.keySet()) {
			String ogNameType = htdlColNameType.get(key);
			Object compareNameType = htblColNameValue.get(key);
			switch (ogNameType) {
			case "java.lang.Integer":
				if (compareNameType instanceof java.lang.Integer
						&& Integer.parseInt(htdlColNameMin.get(key)) < (int) htblColNameValue.get(key)
						&& Integer.parseInt(htdlColNameMax.get(key)) > (int) htblColNameValue.get(key)) {
					flag = true;
				} else
					return false;
			case "java.lang.String":
				if (compareNameType instanceof java.lang.String
						&& htdlColNameMin.get(key).compareTo(htblColNameValue.get(key).toString()) <= 0
						&& htdlColNameMax.get(key).compareTo(htblColNameValue.get(key).toString()) >= 0)
					flag = true;
				else
					return false;
			case "java.lang.Double":
				if (compareNameType instanceof java.lang.Double
						&& Double.parseDouble(htdlColNameMin.get(key)) < (double) htblColNameValue.get(key)
						&& Double.parseDouble(htdlColNameMax.get(key)) > (double) htblColNameValue.get(key))
					flag = true;
				else
					return false;
			case "java.util.Date":
				if (compareNameType instanceof java.util.Date) {
					String currDate1 = htblColNameValue.get(key).toString();
					Date currDate = new SimpleDateFormat("YYYY-MM-DD").parse(currDate1);
					Date minDate = new SimpleDateFormat("YYYY-MM-DD").parse(htdlColNameMin.get(key));
					Date maxDate = new SimpleDateFormat("YYYY-MM-DD").parse(htdlColNameMax.get(key));
					if (currDate.after(minDate) && currDate.before(maxDate)) {
						flag = true;
					} else {
						return false;
					}
				} else
					return false;
			}
		}
		return true;
	}

	private boolean tableExits(String strTableName) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("./resources/metadata.csv"));
			String line = br.readLine();
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					return true;
				}
				line = br.readLine();
			}

			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static void serializePage(Page p, String name) {
		try {
			FileOutputStream fileOut = new FileOutputStream("./resources/" + name + ".ser", false);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();
			System.out.printf("Page is serialized successfully");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Page deserializePage(String name) throws ClassNotFoundException {
		try {
			FileInputStream fileIn = new FileInputStream("./resources/" + name + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page p = (Page) in.readObject();
			in.close();
			fileIn.close();
			return p;
		} catch (IOException i) {
			i.printStackTrace();
		}
		return null;
	}

	public static void serializeTable(Table t, String name) {
		try {
			FileOutputStream fileOut = new FileOutputStream("./resources/" + name + ".ser", false);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(t);
			out.close();
			fileOut.close();
			System.out.printf("Table is serialized successfully");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Table deserializeTable(String name) throws ClassNotFoundException {
		try {
			FileInputStream fileIn = new FileInputStream("./resources/" + name + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Table table = (Table) in.readObject();
			in.close();
			fileIn.close();
			return table;
		} catch (IOException i) {
			i.printStackTrace();
		}
		return null;
	}

	private static Page binarySearchInt(Table t, int ClustKey) {
		Vector pageInfoVector = t.getPageInfo();
		int low = 0;
		int high = pageInfoVector.size() - 1;
		int mid = 0;
		while (low <= high) {

			mid = (high + low) / 2;
			if (ClustKey < ((Integer) ((PageInfo) (pageInfoVector.get(mid))).getMin())) {
				high = mid - 1;
			} else {
				if (ClustKey > ((Integer) ((PageInfo) (pageInfoVector.get(mid))).getMax())) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		try {
			String pagename = ((PageInfo) (pageInfoVector.get(mid))).getPageName();
			Page page = deserializePage(pagename);
			return page;
		} catch (ClassNotFoundException e) {

			return null;
		}

	}
	
	private static Page binarySearchDate(Table t, Date ClustKey) throws ParseException {
		Vector pageInfoVector = t.getPageInfo();
		int low = 0;
		int high = pageInfoVector.size() - 1;
		int mid = 0;
		while (low <= high) {
			
			Date minDate = new SimpleDateFormat("YYYY-MM-DD").parse(( ((PageInfo) (pageInfoVector.get(mid))).getMin()).toString());
			Date maxDate = new SimpleDateFormat("YYYY-MM-DD").parse(( ((PageInfo) (pageInfoVector.get(mid))).getMax()).toString());
			mid = (high + low) / 2;
			if (ClustKey.before(minDate)) {
				high = mid - 1;
			} else {
				if (ClustKey.after(maxDate)) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		try {
			String pagename = ((PageInfo) (pageInfoVector.get(mid))).getPageName();
			Page page = deserializePage(pagename);
			return page;
		} catch (ClassNotFoundException e) {

			return null;
		}

	}
	
	private static Page binarySearchString(Table t, String ClustKey) {
		Vector pageInfoVector = t.getPageInfo();
		int low = 0;
		int high = pageInfoVector.size() - 1;
		int mid = 0;
		while (low <= high) {

			mid = (high + low) / 2;
			if (ClustKey.compareTo( ( ((PageInfo) (pageInfoVector.get(mid))).getMin()).toString())<0) {
				high = mid - 1;
			} else {
				if (ClustKey.compareTo( ( ((PageInfo) (pageInfoVector.get(mid))).getMin()).toString())>0) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		try {
			String pagename = ((PageInfo) (pageInfoVector.get(mid))).getPageName();
			Page page = deserializePage(pagename);
			return page;
		} catch (ClassNotFoundException e) {

			return null;
		}

	}

	private static Page binarySearchDouble(Table t, double ClustKey) {
		Vector pageInfoVector = t.getPageInfo();
		int low = 0;
		int high = pageInfoVector.size() - 1;
		int mid = 0;
		while (low <= high) {

			mid = (high + low) / 2;
			if (ClustKey < ((Double) ((PageInfo) (pageInfoVector.get(mid))).getMin())) {
				high = mid - 1;
			} else {
				if (ClustKey > ((Double) ((PageInfo) (pageInfoVector.get(mid))).getMax())) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		try {
			String pagename = ((PageInfo) (pageInfoVector.get(mid))).getPageName();
			Page page = deserializePage(pagename);
			return page;
		} catch (ClassNotFoundException e) {

			return null;
		}

	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp();
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");

		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", "A");

		Hashtable htblColNameMax = new Hashtable<>();
		htblColNameMax.put("id", "10000");
		htblColNameMax.put("name", "ZZZZZZZZZZZ");

		dbApp.init();
		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
		// dbApp.deserializeTable(strTableName);
	}
}
