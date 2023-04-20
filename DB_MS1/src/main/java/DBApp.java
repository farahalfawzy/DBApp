import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

//TODO LAW el clustering key mawgood
public class DBApp {

	Vector<Table> allTable = new Vector<Table>();
	static int maxnoOfRows = getMaxRows();
	boolean isDeletingMethod=false;

	private static int getMaxRows() {
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
			prop.load(fis);
			return Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
		} catch (Exception ex) {
			return -1;
		}
	}

	public void init() {
		try {
			File csv = new File("src/main/resources/metadata.csv");
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
		String filePath = "src/main/resources/metadata.csv";
		String strColName = "";

		if (!tableExits(strTableName)) {
			FileWriter writer;
			try {
				writer = new FileWriter(filePath, true);
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

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ParseException, ClassNotFoundException {
		if (tableExits(strTableName)) {

			// law mafesh Page
			if (isValid(strTableName, htblColNameValue)) {
				Table t = deserializeTable(strTableName);
				Object clustKey = htblColNameValue.get(t.getClusteringKey());
				if (t.getCurrentMaxId() == -1) {
					Page page = new Page();
					Tuple tuple = new Tuple(clustKey, htblColNameValue);

					page.add(tuple);
					String PageName = t.getTableName() + "0";
					PageInfo pageinfo = new PageInfo(PageName, 0, clustKey, clustKey);
					t.getPageInfo().add(pageinfo);
					t.setCurrentMaxId(t.getCurrentMaxId() + 1);
					serializePage(page, PageName);
					System.out.println(PageName + " " + clustKey);
					serializeTable(t, t.getTableName());
					return;
				} else {
					int pageind = 0;
					if (clustKey instanceof java.lang.Integer) {
						pageind = binarySearchInt(t, (Integer) clustKey);
					}
					if (clustKey instanceof java.lang.String) {
						pageind = binarySearchString(t, (String) clustKey);
					}
					if (clustKey instanceof java.lang.Double) {
						pageind = binarySearchDouble(t, (Double) clustKey);
					}
					if (clustKey instanceof java.util.Date) {
						System.out.println("was here date");
						pageind = binarySearchDate(t, (Date) clustKey);
					}
					System.out.println("llc   " + pageind);

					try {
						Vector pageInfoVector = t.getPageInfo();
						String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
						Page page = deserializePage(pagename);
						Tuple tuple = new Tuple(clustKey, htblColNameValue);
						if (page.contains2(tuple)) {
							throw new DBAppException("Clustering Key already exists");
						} else {

							if (page.size() < maxnoOfRows) {
								page.add(tuple);
								Collections.sort(page);
								((PageInfo) pageInfoVector.get(pageind)).setMax(getMaxInPage(page));
								((PageInfo) pageInfoVector.get(pageind)).setMin(getMinInPage(page));
								serializePage(page, t.getTableName() + "" + pageind);
								serializeTable(t, t.getTableName());
								return;
							} else {// law fel nos
								page.add(tuple);
								Collections.sort(page);
								Tuple newtup = (Tuple) page.remove(page.size() - 1);
								((PageInfo) pageInfoVector.get(pageind)).setMax(getMaxInPage(page));
								((PageInfo) pageInfoVector.get(pageind)).setMin(getMinInPage(page));
								serializePage(page, t.getTableName() + "" + pageind);
								int ind = pageind + 1;
								while (true) {
									if (ind > t.getCurrentMaxId()) {// new page
																	// fel a5er
										Page newPage = new Page();
										PageInfo pi = new PageInfo(t.getTableName() + "" + ind, ind,
												newtup.Clusteringkey, newtup.Clusteringkey);

										pageInfoVector.add(pi);
										newPage.add(newtup);
										Collections.sort(newPage);

										serializePage(newPage, t.getTableName() + "" + ind);
										t.setCurrentMaxId(t.getCurrentMaxId() + 1);
										break;
									} else {// lesa fel nos
										try {
											Page nextpage = deserializePage(
													((PageInfo) (pageInfoVector.get(ind))).getPageName());

											if (nextpage.size() < maxnoOfRows) {
												nextpage.add(newtup);
												Collections.sort(nextpage);
												((PageInfo) pageInfoVector.get(ind)).setMax(getMaxInPage(page));
												((PageInfo) pageInfoVector.get(ind)).setMin(getMinInPage(page));
												serializePage(nextpage, t.getTableName() + "" + ind);
												break;

											} else {
												nextpage.add(newtup);
												Collections.sort(nextpage);
												newtup = (Tuple) nextpage.remove(nextpage.size() - 1);
												((PageInfo) pageInfoVector.get(ind)).setMax(getMaxInPage(nextpage));
												((PageInfo) pageInfoVector.get(ind)).setMin(getMinInPage(nextpage));
												serializePage(nextpage, t.getTableName() + "" + ind);
												ind = ind + 1;
											}
										} catch (ClassNotFoundException e) {

										}
									}
								}
							}
						}
					} catch (ClassNotFoundException e) {

					}
				}

				// check if page is full
				// insert ya farah <3//shokrannn
				serializeTable(t, t.getTableName());

			} else {
				throw new DBAppException("Invalid Data");
			}

		} else {
			throw new DBAppException("Table not found");
		}
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, ParseException {
		this.isDeletingMethod=true;
		if (tableExits(strTableName)) {
			Table t = deserializeTable(strTableName);
//			if(t.getPageInfo().size()==0)
//				throw new DBAppException("No more buckets to delete");
			if (isValid(strTableName, htblColNameValue)) {
				String myCluster = t.getClusteringKey();
				int pageind = -1;
				Tuple myTuple = new Tuple(t.getClusteringKey(), htblColNameValue);
				if (htblColNameValue.containsKey(myCluster)) {
					System.out.println("was here");
					Object myClusterType = htblColNameValue.get(myCluster);
					if (myClusterType instanceof java.lang.Integer) {
						pageind = binarySearchInt(t, (Integer) myClusterType);
					}
					if (myClusterType instanceof java.lang.String) {
						pageind = binarySearchString(t, (String) myClusterType);
					}
					if (myClusterType instanceof java.lang.Double) {
						pageind = binarySearchDouble(t, (Double) myClusterType);
					}
					if (myClusterType instanceof java.util.Date) {
						pageind = binarySearchDate(t, (Date) myClusterType);
					}
					Vector pageInfoVector = t.getPageInfo();
					if (pageInfoVector.size() != 0) {
						String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
						Page page = deserializePage(pagename);

						if (page.contains(myTuple)) {
							System.out.println("was here1");
							page.remove(myTuple);
							if (page.size() == 0) {
								deletingFiles(pageind, pageInfoVector, t);
							} else {

								Object max = getMaxInPage(page);
								Object min = getMinInPage(page);
								((PageInfo) pageInfoVector.get(pageind)).setMax(max);
								((PageInfo) pageInfoVector.get(pageind)).setMin(min);

								serializePage(page, t.getTableName() + "" + pageind);
							}
						}

					}
				} else {
					Vector pageInfoVector = t.getPageInfo();
					removeFromAllPages(pageInfoVector, myTuple, 0, t, htblColNameValue);
				}
				serializeTable(t, strTableName);
			} else {
				this.isDeletingMethod=false;
				throw new DBAppException("Reenter your values!");
			}
		} else

		{
			this.isDeletingMethod=false;
			throw new DBAppException("Table doesn't exist");
		}
	}

	private void deletingFiles(int pageind, Vector pageInfoVector, Table t) { // 3lshan
																				// lw
																				// page
																				// kolah
																				// dups
																				// yms7ha
																				// 3ala
																				// tool
		// TODO Auto-generated method stub
		File mySerial = new File("src/main/resources/Data/" + t.getTableName() + "" + pageind + ".ser");
		if (mySerial.delete())
			System.out.println("File deleted successfully");
		try {
			File oldFile;
			File newFile;
			pageInfoVector.remove(pageind);
			for (int i = pageind; i < pageInfoVector.size(); i++) {
				int temp = i + 1;
				oldFile = new File("src/main/resources/Data/" + t.getTableName() + "" + temp + ".ser");
				newFile = new File("src/main/resources/Data/" + t.getTableName() + "" + i + ".ser");
				((PageInfo) pageInfoVector.get(i)).setPageName(t.getTableName() + "" + i);
				if (oldFile.renameTo(newFile)) {
					System.out.println("File renamed successfully");
				} else {
					System.out.println("Failed to rename file");
				}
			}

		} catch (Exception e) {// msh 3ayez e3ml 7aga
		}
	}

	// 3ayez awel lma 2ms7 row mn page 2geeb mn elmin mn elpage eltanya w 27otha
	// fel page ely ana wa2ef feeha
	private void removeFromAllPages(Vector pageInfoVector, Tuple myTuple, int i, Table t, Hashtable htblColNameValue)
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		if (pageInfoVector.size() == i)
			return;
		String pagename = ((PageInfo) (pageInfoVector.get(i))).getPageName();
		Page page = deserializePage(pagename);
		page.remove(myTuple);
		if (page.size() == 0) {
			deletingFiles(i, pageInfoVector, t);
			removeFromAllPages(pageInfoVector, myTuple, i, t, htblColNameValue);
		} else {
			Object max = getMaxInPage(page);
			Object min = getMinInPage(page);
			((PageInfo) pageInfoVector.get(i)).setMax(max);
			((PageInfo) pageInfoVector.get(i)).setMin(min);
			serializePage(page, pagename);
			removeFromAllPages(pageInfoVector, myTuple, ++i, t, htblColNameValue);
		}
	}

	// private boolean isValid(Table table, Hashtable<String, Object>
	// htblColNameValue)
	// throws ParseException, DBAppException {
	// Hashtable<String, String> htdlColNameType = table.getColNameType();
	// Hashtable<String, String> htdlColNameMin = table.getColNameMin();
	// Hashtable<String, String> htdlColNameMax = table.getColNameMax();
	// boolean flag = true;
	// boolean flag2 = false;
	// for (String key : htblColNameValue.keySet()) {
	// if (key.equals(table.getClusteringKey())) {
	// flag2 = true;
	// break;
	// }
	// }
	// // if(!flag2) {
	// // throw new DBAppException("Clustering Key already existsss");
	// // }
	// for (String key : htblColNameValue.keySet()) {
	// String ogNameType = htdlColNameType.get(key);
	// Object compareNameType = htblColNameValue.get(key);
	// switch (ogNameType) {
	// case "java.lang.Integer": {
	// if (compareNameType instanceof java.lang.Integer
	// && Integer.parseInt(htdlColNameMin.get(key)) < (int)
	// htblColNameValue.get(key)
	// && Integer.parseInt(htdlColNameMax.get(key)) > (int)
	// htblColNameValue.get(key)) {
	// flag = true;
	// continue;
	// } else
	// return false;
	// }
	// case "java.lang.String": {
	//
	// if (compareNameType instanceof java.lang.String
	// &&
	// htdlColNameMin.get(key).compareTo(htblColNameValue.get(key).toString())
	// <= 0
	// &&
	// htdlColNameMax.get(key).compareTo(htblColNameValue.get(key).toString())
	// >= 0) {
	// flag = true;
	// continue;
	//
	// } else
	// return false;
	// }
	// case "java.lang.Double": {
	//
	// if (compareNameType instanceof java.lang.Double
	// && Double.parseDouble(htdlColNameMin.get(key)) < (double)
	// htblColNameValue.get(key)
	// && Double.parseDouble(htdlColNameMax.get(key)) > (double)
	// htblColNameValue.get(key)) {
	// flag = true;
	// continue;
	// }
	//
	// else
	// return false;
	// }
	// case "java.util.Date": {
	// System.out.println("here date" + compareNameType);
	//
	// if (compareNameType instanceof java.util.Date) {
	// String currDate1 = htblColNameValue.get(key).toString();
	// Date currDate = new SimpleDateFormat("YYYY-MM-DD").parse(currDate1);
	// Date minDate = new
	// SimpleDateFormat("YYYY-MM-DD").parse(htdlColNameMin.get(key));
	// Date maxDate = new
	// SimpleDateFormat("YYYY-MM-DD").parse(htdlColNameMax.get(key));
	// if (currDate.after(minDate) && currDate.before(maxDate)) {
	// flag = true;
	// } else {
	// return false;
	// }
	// } else
	// return false;
	// continue;
	// }
	// }
	// }
	// return true;
	// }
	//
	private boolean tableExits(String strTableName) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
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
			// e.printStackTrace();
		}
		return false;
	}

	private boolean isValid(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ParseException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			Hashtable<String, String[]> tableInfo = new Hashtable<String, String[]>();
			String ClustKey = "";
			boolean found = false;
			boolean ClustKeyfound = false;
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					String[] array = { x[2], x[4], x[5] };// {type,min,max}
					tableInfo.put(x[1], array);
					if (x[3].equals("True")) {
						ClustKey = x[1];
					}
					found = true;
				} else {
					if (found)
						break;
				}
				line = br.readLine();
			}
			br.close();
			for (String key : htblColNameValue.keySet()) {
				if (key.equals(ClustKey)) {
					ClustKeyfound = true;
					break;
				}
			}
			if (!ClustKeyfound && !isDeletingMethod) {			//ana 3mlt deh 3lshan fel delete lw ana msh m3aya elcluster key e3ml delete 3ady bardo
				throw new DBAppException("You have to insert Clustering Key");
			}
			boolean flag = true;

			for (String key : htblColNameValue.keySet()) {
				System.out.println(key);
				if (!tableInfo.containsKey(key)) {
					throw new DBAppException("Invalid Column");
				}
				String ogNameType = tableInfo.get(key)[0];
				Object compareNameType = htblColNameValue.get(key);
				switch (ogNameType) {
				case "java.lang.Integer": {
					if (compareNameType instanceof java.lang.Integer
							&& Integer.parseInt(tableInfo.get(key)[1]) <= (int) htblColNameValue.get(key)
							&& Integer.parseInt(tableInfo.get(key)[2]) >= (int) htblColNameValue.get(key)) {
						flag = true;
						continue;
					} else {
						System.out.println("int");
						return false;
					}
				}
				case "java.lang.String": {

					if (compareNameType instanceof java.lang.String
							&& tableInfo.get(key)[1].compareTo(htblColNameValue.get(key).toString()) <= 0
							&& tableInfo.get(key)[2].compareTo(htblColNameValue.get(key).toString()) >= 0) {
						flag = true;

						continue;

					} else {
						System.out.println("string");

						return false;
					}
				}
				case "java.lang.Double": {

					if (compareNameType instanceof java.lang.Double
							&& Double.parseDouble(tableInfo.get(key)[1]) <= (double) htblColNameValue.get(key)
							&& Double.parseDouble(tableInfo.get(key)[2]) >= (double) htblColNameValue.get(key)) {
						flag = true;

						continue;
					}

					else {
						System.out.println("double");

						return false;
					}
				}
				case "java.util.Date": {
					System.out.println("here date" + compareNameType);

					if (compareNameType instanceof java.util.Date) {
						// String currDate1 = htblColNameValue.get(key).toString();
						// Date currDate = new SimpleDateFormat("YYYY-MM-DD")
						// .parse(currDate1);
						Date currDate = (Date) compareNameType;
						Date minDate = new SimpleDateFormat("YYYY-MM-DD").parse(tableInfo.get(key)[1]);
						Date maxDate = new SimpleDateFormat("YYYY-MM-DD").parse(tableInfo.get(key)[2]);
						// System.out.println("here date" + currDate);

						if (currDate.after(minDate) && currDate.before(maxDate)) {
							flag = true;

							continue;

						} else {
							System.out.println("date1");

							return false;
						}
					} else {
						System.out.println("date2");

						return false;
					}
				}
				}
			}
			return true;

		} catch (IOException e) {
			throw new DBAppException("CSV doesn't exist");
		}
	}

	public static void serializePage(Page p, String name) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src/main/resources/Data/" + name + ".ser", false);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();
			System.out.println("Page is serialized successfully");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Page deserializePage(String name) throws ClassNotFoundException {
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/Data/" + name + ".ser");
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
			FileOutputStream fileOut = new FileOutputStream("src/main/resources/Data/" + name + ".ser", false);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(t);
			out.close();
			fileOut.close();
			System.out.println("Table is serialized successfully");
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	public static Table deserializeTable(String name) throws ClassNotFoundException {
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/Data/" + name + ".ser");
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

	private static int binarySearchInt(Table t, int ClustKey) {
		Vector pageInfoVector = t.getPageInfo();
		int low = 0;
		int high = pageInfoVector.size() - 1;
		int mid = 0;
		while (low <= high) {

			mid = (high + low) / 2;
			System.out.println(mid);
			PageInfo pi = (PageInfo) (pageInfoVector.get(mid));
			// System.out.println("PIIII " + pi.getMax() + " " + pi.getMin() +
			// " " + pi.getPageName() + " " + pi.getId());
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
		return mid;

	}

	private static int binarySearchDate(Table t, Date ClustKey) throws ParseException {
		Vector pageInfoVector = t.getPageInfo();
		int low = 0;
		int high = pageInfoVector.size() - 1;
		int mid = 0;
		while (low <= high) {

			Date minDate = new SimpleDateFormat("YYYY-MM-DD")
					.parse((((PageInfo) (pageInfoVector.get(mid))).getMin()).toString());
			Date maxDate = new SimpleDateFormat("YYYY-MM-DD")
					.parse((((PageInfo) (pageInfoVector.get(mid))).getMax()).toString());
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
		return mid;

	}

	private static int binarySearchString(Table t, String ClustKey) {
		Vector pageInfoVector = t.getPageInfo();
		int low = 0;
		int high = pageInfoVector.size() - 1;
		int mid = 0;
		while (low <= high) {

			mid = (high + low) / 2;
			if (ClustKey.compareTo((((PageInfo) (pageInfoVector.get(mid))).getMin()).toString()) < 0) {
				high = mid - 1;
			} else {
				if (ClustKey.compareTo((((PageInfo) (pageInfoVector.get(mid))).getMin()).toString()) > 0) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		return mid;

	}

	private static int binarySearchDouble(Table t, double ClustKey) {
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
		return mid;

	}

	public static Object getMinInPage(Page page) {
		Tuple min = ((Tuple) page.get(0));
		for (int i = 0; i < page.size(); i++) {
			if (((Tuple) page.get(i)).compareTo(min) < 0) {
				min = (Tuple) page.get(i);
			}
		}
		return min.Clusteringkey;
	}

	public static Object getMaxInPage(Page page) {
		Tuple max = ((Tuple) page.get(0));
		for (int i = 0; i < page.size(); i++) {
			if (((Tuple) page.get(i)).compareTo(max) > 0) {
				max = (Tuple) page.get(i);
			}
		}
		return max.Clusteringkey;
	}

	public static void getPages(String tableName) { // only for testing
		try {
			Table t = deserializeTable(tableName);
			for (int i = 0; i < t.getPageInfo().size(); i++) {
				String pagename = ((PageInfo) ((t.getPageInfo()).get(i))).getPageName();
				Page p = deserializePage(pagename);
				System.out.println(i + " " + pagename);
				for (int j = 0; j < p.size(); j++) {
					Tuple tup = (Tuple) p.get(j);
					for (String key : tup.record.keySet()) {
						System.out.println("Col : " + key + "\t\t Value : " + tup.record.get(key).toString());
					}

				}
				serializePage(p, pagename);
			}
			serializeTable(t, tableName);
		} catch (ClassNotFoundException e) {

		}
	}

	// public static Tuple
	public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {

	}
}
