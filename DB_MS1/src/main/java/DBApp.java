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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Arrays;

public class DBApp {

	// Vector<Table> allTable = new Vector<Table>();
	static int maxnoOfRows = getMaxRows();
	boolean isDeletingMethod = false;
	boolean isUpdatingMethod = false;

	private static int getMaxRows() {
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
			prop.load(fis);
			return Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
			// return 4;
		} catch (Exception ex) {
			return -1;
		}
	}

	public void init() { // throw
		try {
			File csv = new File("src/main/resources/metadata.csv");
			if (csv.createNewFile()) {
				FileWriter writer = new FileWriter("src/main/resources/metadata.csv");
				writer.append("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max");
				writer.append("\n");
				writer.close();

				// System.out.println("The CSV file was created!!");
			} else {
//				System.out.println("Already exists");
			}
		} catch (IOException e) {
//			System.out.println("Destination not found");
		}

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		String filePath = "src/main/resources/metadata.csv";
		String strColName = "";

		if (!tableExits(strTableName)) {
			FileWriter writer;
			htblColNameType = convertKeysToLowerCreate(htblColNameType);
			htblColNameMin = convertKeysToLowerCreate(htblColNameMin);
			htblColNameMax = convertKeysToLowerCreate(htblColNameMax);

			if (!canCreate(htblColNameType, htblColNameMin, htblColNameMax)) {
				throw new DBAppException("Enter Valid Data to create table");
			}
			Set keys = htblColNameType.keySet();
			if (!keys.contains(strClusteringKeyColumn.toLowerCase())) {
				throw new DBAppException("Enter Valid Data to create table");
			}
			try {
				writer = new FileWriter(filePath, true);
				// header
				// writer.append("Table Name, Column Name, Column Type, ClusteringKey,
				// IndexName,IndexType, min, max");
				// writer.append("\n");
				for (String key : htblColNameType.keySet()) {
					strColName = key;
					String type = htblColNameType.get(key);
					String min = htblColNameMin.get(key);
					String max = htblColNameMax.get(key);
					writer.append(strTableName + ",");
					writer.append(key.toLowerCase() + ",");
					writer.append(type + ",");
					if (key.equals(strClusteringKeyColumn.toLowerCase()))
						writer.append("True,");
					else
						writer.append("False,");
					writer.append("null" + ",");
					writer.append("null" + ",");
					writer.append(min + ",");
					writer.append(max + ",");
					writer.append("\n");
				}
				writer.close();
				Table myTable = new Table(strTableName, strClusteringKeyColumn.toLowerCase(), htblColNameType,
						htblColNameMin, htblColNameMax);
				// allTable.add(myTable);
				serializeTable(myTable, strTableName);
				myTable = null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		} else {
			throw new DBAppException("Tabble Already exists!");
		}

	}

	private Hashtable<String, String> convertKeysToLowerCreate(Hashtable<String, String> htblColNameValue) {
		Hashtable<String, String> result = new Hashtable<String, String>();
		for (String key : htblColNameValue.keySet()) {
			result.put(key.toLowerCase(), htblColNameValue.get(key));
		}
		return result;
	}

	private Hashtable<String, Object> convertKeysToLower(Hashtable<String, Object> htblColNameValue) {
		Hashtable<String, Object> result = new Hashtable<String, Object>();
		for (String key : htblColNameValue.keySet()) {
			result.put(key.toLowerCase(), htblColNameValue.get(key));
		}
		return result;
	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		if (strarrColName.length != 3) {
			throw new DBAppException("Have to Use 3 Columns");
		}
		if (strarrColName[0].toLowerCase().equals(strarrColName[1].toLowerCase())
				|| strarrColName[0].toLowerCase().equals(strarrColName[2].toLowerCase())
				|| strarrColName[1].toLowerCase().equals(strarrColName[2].toLowerCase())) {
			throw new DBAppException("Have to Use 3 UNIQUE Columns");

		}
		checkIndexMeta(strTableName, strarrColName);// ask

		Table t = deserializeTable(strTableName);
		if (strarrColName[0].toLowerCase().equals(t.getClusteringKey().toLowerCase())
				|| strarrColName[1].toLowerCase().equals(t.getClusteringKey().toLowerCase())
				|| strarrColName[2].toLowerCase().equals(t.getClusteringKey().toLowerCase())) {
			t.setIndexOnClustKey(true);
		}
		Octree newTree = createOctree(strTableName, strarrColName);
		String col1 = newTree.getX();
		String col2 = newTree.getY();
		String col3 = newTree.getZ();
		String Octname = t.getTableName() + col1 + col2 + col3 + "Index";
		t.getIndex().add(Octname);
		Hashtable<String, Object> htblIndex = new Hashtable<>();
		htblIndex.put(col1, Octname);
		htblIndex.put(col2, Octname);
		htblIndex.put(col3, Octname);
		t.getIndexOnCol().add(htblIndex);
		for (int i = 0; i < t.getPageInfo().size(); i++) {
			String pagename = ((PageInfo) ((t.getPageInfo()).get(i))).getPageName();
			Page p = deserializePage(pagename);

			for (int j = 0; j < p.size(); j++) {
				Tuple tup = (Tuple) p.get(j);
				if (tup.getRecord().get(col1) == null || tup.getRecord().get(col2) == null
						|| tup.getRecord().get(col3) == null) {
					throw new DBAppException("Some null values exists");

				}

				Hashtable<String, Object> recInIndex = new Hashtable<>();
				recInIndex.put(col1, tup.getRecord().get(col1));
				recInIndex.put(col2, tup.getRecord().get(col2));
				recInIndex.put(col3, tup.getRecord().get(col3));
				recInIndex.put("Page Name", pagename);
				recInIndex.put("Clust key", tup.getRecord().get(t.getClusteringKey()));
				newTree.insertTupleInIndex(recInIndex);

			}
			serializePage(t, p, pagename);
		}
		serializeIndex(newTree, Octname);
		serializeTable(t, strTableName);

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		this.isDeletingMethod = false;
		this.isUpdatingMethod = false;

		if (tableExits(strTableName)) {
			htblColNameValue = convertKeysToLower(htblColNameValue);
			System.out.println(htblColNameValue);
			if (isValid(strTableName, htblColNameValue)) {
				isValidIndex(strTableName, htblColNameValue);
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
					serializePage(t, page, PageName);
					serializeTable(t, t.getTableName());
					insertIntoIndex(strTableName, htblColNameValue, PageName);
					page = null;
					t = null;
					return;
				} else {
					int pageind = 0;
					boolean isSmallerThanmin = false;
					Vector pageInfoVector = t.getPageInfo();

					if (clustKey instanceof java.lang.Integer) {
						pageind = t.isIndexOnClustKey() ? searchForPageToInsertUsingIndex(t, htblColNameValue)
								: binarySearchInt(t, (Integer) clustKey);
						if ((Integer) clustKey < (Integer) ((PageInfo) (pageInfoVector.get(pageind))).getMin()) {
							isSmallerThanmin = true;
						}
					}
					if (clustKey instanceof java.lang.String) {
						pageind = t.isIndexOnClustKey() ? searchForPageToInsertUsingIndex(t, htblColNameValue)
								: binarySearchString(t, (String) clustKey);
						if (((String) clustKey).toLowerCase().compareTo(
								((String) (((PageInfo) (pageInfoVector.get(pageind))).getMin())).toLowerCase()) < 0) {
							isSmallerThanmin = true;

						}
					}
					if (clustKey instanceof java.lang.Double) {
						pageind = t.isIndexOnClustKey() ? searchForPageToInsertUsingIndex(t, htblColNameValue)
								: binarySearchDouble(t, (Double) clustKey);
						if ((Double) clustKey < ((Double) ((PageInfo) (pageInfoVector.get(pageind))).getMin())) {
							isSmallerThanmin = true;
						}
					}
					if (clustKey instanceof java.util.Date) {
						pageind = t.isIndexOnClustKey() ? searchForPageToInsertUsingIndex(t, htblColNameValue)
								: binarySearchDate(t, (Date) clustKey);
						if (((Date) clustKey).before(((Date) ((PageInfo) (pageInfoVector.get(pageind))).getMin()))) {
							isSmallerThanmin = true;

						}
					}

					String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
					Page page = deserializePage(pagename);
					Tuple tuple = new Tuple(clustKey, htblColNameValue);

					if (page.containsClustKey(tuple)) {
						page = null;
						t = null;
						throw new DBAppException("Clustering Key already exists");
					} else {
						if (isSmallerThanmin) {
							if (pageind - 1 > -1) {
								if (((PageInfo) (pageInfoVector.get(pageind - 1))).getCount() < maxnoOfRows) {
									serializePage(t, page, t.getTableName() + "" + pageind);
									pagename = ((PageInfo) (pageInfoVector.get(pageind - 1))).getPageName();
									page = deserializePage(pagename);
									int indexInPage = page.getIndexInPage(tuple);
									page.insertElementAt(tuple, indexInPage);
//									
									((PageInfo) pageInfoVector.get(pageind - 1)).setMax(getMaxInPage(page));
									((PageInfo) pageInfoVector.get(pageind - 1)).setMin(getMinInPage(page));
									insertIntoIndex(strTableName, htblColNameValue,
											t.getTableName() + "" + (pageind - 1));

									serializePage(t, page, t.getTableName() + "" + (pageind - 1));
									serializeTable(t, t.getTableName());
									page = null;
									t = null;
									return;
								}
							}

						}
						if (page.size() < maxnoOfRows) {
							int indexInPage = page.getIndexInPage(tuple);
							page.insertElementAt(tuple, indexInPage);
//							page.add(tuple);
//							Collections.sort(page);
							((PageInfo) pageInfoVector.get(pageind)).setMax(getMaxInPage(page));
							((PageInfo) pageInfoVector.get(pageind)).setMin(getMinInPage(page));

							serializePage(t, page, t.getTableName() + "" + pageind);
							serializeTable(t, t.getTableName());
							insertIntoIndex(strTableName, htblColNameValue, t.getTableName() + "" + pageind);

							page = null;
							t = null;

							return;
						} else {// law fel nos
							int indexInPage = page.getIndexInPage(tuple);
							page.insertElementAt(tuple, indexInPage);
//							 page.add(tuple);
//							Collections.sort(page);
							Tuple newtup = (Tuple) page.remove(page.size() - 1);// remove
							((PageInfo) pageInfoVector.get(pageind)).setMax(getMaxInPage(page));
							((PageInfo) pageInfoVector.get(pageind)).setMin(getMinInPage(page));

							serializePage(t, page, t.getTableName() + "" + pageind);
							page = null;
							insertIntoIndex(strTableName, htblColNameValue, t.getTableName() + "" + pageind);

							int ind = pageind + 1;
							while (true) {

								if (ind > t.getCurrentMaxId()) {// new page
																// fel a5er
									Page newPage = new Page();
									PageInfo pi = new PageInfo(t.getTableName() + "" + ind, ind, newtup.Clusteringkey,
											newtup.Clusteringkey);
									pageInfoVector.add(pi);
									// page.getIndex(tuple);

									newPage.add(newtup);

									serializePage(t, newPage, t.getTableName() + "" + ind);
									t.setCurrentMaxId(t.getCurrentMaxId() + 1);
									newPage = null;
									updateRefrenceInIndex(strTableName, newtup.getRecord(), t.getTableName() + "" + ind,
											t.getTableName() + "" + (ind - 1));

									break;
								} else {// lesa fel nos

									Page nextpage = deserializePage(
											((PageInfo) (pageInfoVector.get(ind))).getPageName());

									if (nextpage.size() < maxnoOfRows) {
										int newindexInPage = nextpage.getIndexInPage(newtup);
										nextpage.insertElementAt(newtup, newindexInPage);

										((PageInfo) pageInfoVector.get(ind)).setMax(getMaxInPage(nextpage));
										((PageInfo) pageInfoVector.get(ind)).setMin(getMinInPage(nextpage));
										serializePage(t, nextpage, t.getTableName() + "" + ind);
										nextpage = null;
										updateRefrenceInIndex(strTableName, newtup.getRecord(),
												t.getTableName() + "" + pageind, t.getTableName() + "" + (pageind - 1));

										break;

									} else {
										int newindexInPage = nextpage.getIndexInPage(newtup);
										nextpage.insertElementAt(newtup, newindexInPage);
										updateRefrenceInIndex(strTableName, newtup.getRecord(),
												t.getTableName() + "" + ind, t.getTableName() + "" + (ind - 1));

										newtup = (Tuple) nextpage.remove(nextpage.size() - 1);// remove
										((PageInfo) pageInfoVector.get(ind)).setMax(getMaxInPage(nextpage));
										((PageInfo) pageInfoVector.get(ind)).setMin(getMinInPage(nextpage));
										serializePage(t, nextpage, t.getTableName() + "" + ind);
										nextpage = null;
										ind = ind + 1;
									}

								}
							}
						}
					}

				}

				// check if page is full
				serializeTable(t, t.getTableName());
				t = null;

			} else {
				throw new DBAppException("Invalid Data");
			}

		} else {
			throw new DBAppException("Table not found");
		}
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Page page = null;
		Table t = null;
		try {
			if (!tableExits(strTableName))
				throw new DBAppException("Table does not exist");
			isUpdatingMethod = true;
			htblColNameValue = convertKeysToLower(htblColNameValue);
			if (isValid(strTableName, htblColNameValue)) {

				int pageind = -1;
				String type = this.getClusteringKeyType(strTableName);
				t = deserializeTable(strTableName);
				if (t.getPageInfo().size() == 0) {
					serializeTable(t, strTableName);
					t = null;
					return;
				}
				Object ClustObj = null;
				switch (type) {
				case "java.lang.Integer":
					int clust = Integer.parseInt(strClusteringKeyValue);
					pageind = getPageNumFromIndex(strTableName, htblColNameValue, clust);
					if (pageind == -1)
						pageind = binarySearchInt(t, clust);
					ClustObj = clust;

					break;
				case "java.lang.String":
					pageind = getPageNumFromIndex(strTableName, htblColNameValue, strClusteringKeyValue);
					if (pageind == -1)

						pageind = binarySearchString(t, strClusteringKeyValue);
					ClustObj = strClusteringKeyValue;
					break;
				case "java.lang.Double":
					double clustdouble = Double.parseDouble(strClusteringKeyValue);
					pageind = getPageNumFromIndex(strTableName, htblColNameValue, clustdouble);
					if (pageind == -1)
						pageind = binarySearchDouble(t, clustdouble);
					ClustObj = clustdouble;

					break;
				case "java.util.Date":
					Date date = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
					pageind = getPageNumFromIndex(strTableName, htblColNameValue, date);
					if (pageind == -1)
						pageind = binarySearchDate(t, date);
					ClustObj = date;
//					System.out.println(date);
					break;
				}
				Vector pageInfoVector = t.getPageInfo();
				String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
				page = deserializePage(pagename);
				if (page.containsKey(ClustObj)) { // hna 3'yrt containsKey
//					for (int i = 0; i < page.size(); i++) {
//						Tuple tuple = page.get(i);
//						if (tuple.getClusteringkey().equals(ClustObj)) {
//							for (String key : htblColNameValue.keySet()) {
//								tuple.getRecord().put(key, htblColNameValue.get(key));
//
//							}
//
//						}
//					}
					int indexInpage = page.getIndexInPageUsingClusteringKey(ClustObj);
					Tuple tuple = page.get(indexInpage);
					Hashtable<String, Object> oldtuple = (Hashtable<String, Object>) tuple.getRecord().clone();

					for (String key : htblColNameValue.keySet()) {
						tuple.getRecord().put(key, htblColNameValue.get(key));
					}
					serializePage(t, page, strTableName + "" + pageind);
					serializeTable(t, strTableName);

					updateTupleinIndex(strTableName, oldtuple, tuple.getRecord(), strTableName + "" + pageind,
							htblColNameValue);
					// page.replace(ClustObj, htblColNameValue);
					isUpdatingMethod = false;

					page = null;
					t = null;
				}

				else {
					page = null;
					t = null;
					isUpdatingMethod = false;
//					throw new DBAppException("clustering key doesnt exist");
					return;
				}

			} else {
				isUpdatingMethod = false;
//				System.out.println("ana hena");
				throw new DBAppException("invalid values");

			}

		} catch (DBAppException e) {
			serializeTable(t, strTableName);
			t = null;
			isUpdatingMethod = false;
			throw new DBAppException(e.toString());
		} catch (ParseException e) {
			serializeTable(t, strTableName);
			t = null;
			isUpdatingMethod = false;
			throw new DBAppException("enter valid clustring key value");
		} catch (java.lang.NumberFormatException e) {
			serializeTable(t, strTableName);
			t = null;
			isUpdatingMethod = false;
			throw new DBAppException("enter valid clustring key value");
		}

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		this.isDeletingMethod = true;
		if (tableExits(strTableName)) {
			htblColNameValue = convertKeysToLower(htblColNameValue);
//			if(t.getPageInfo().size()==0)
//				throw new DBAppException("No more buckets to delete");
			if (isValid(strTableName, htblColNameValue)) {
				Table t = deserializeTable(strTableName);
				String myCluster = t.getClusteringKey();
				if (t.getPageInfo().size() == 0) {
					serializeTable(t, strTableName);
					t = null;
					return;
				}
				// New
				Vector pageInfoVector = new Vector<>();
				Tuple myTuple = new Tuple(t.getClusteringKey(), htblColNameValue);
				Vector<String> indexNameVector = getindexname(t, htblColNameValue);
				String indexName = "";
				int count = 0;
				for (int i = 0; i < indexNameVector.size(); i++) {
					for (int j = 0; j < indexNameVector.size(); j++) {
						if (indexNameVector.get(i).equals(indexNameVector.get(j)))
							count++;
					}
					if (count == 3) {
						indexName = indexNameVector.get(i);
						break;
					}
				}
				if (!indexName.equals("")) {
					Octree myOct = deserializeOctree(indexName + "" + t.getTableName());
					Hashtable<String, Object> key = new Hashtable<>();
					key.put(myOct.getX(), htblColNameValue.get(myOct.getX()));
					key.put(myOct.getY(), htblColNameValue.get(myOct.getY()));
					key.put(myOct.getZ(), htblColNameValue.get(myOct.getZ()));
					Vector<String> pageName = myOct.getPageName(key);
					pageInfoVector = t.getPageInfo();
					if (pageInfoVector.size() != 0) {
						for (int i = 0; i < pageName.size(); i++) {
							Page page = deserializePage(pageName.get(i));
							if (page.contains(myTuple)) {
								// get all the columns that have indices
								deleteFromOctree(myTuple,t);
								page.removeBinary(myTuple);
								myOct.deleteTuple(key);
								String res = "";
								for (int j = pageName.get(i).length() - 1; j > (-1); j--) {
									if ((pageName.get(i).charAt(j)) >= '0' && pageName.get(i).charAt(j) <= '9') {
										res = pageName.get(i).charAt(j) + res;
									} else
										break;
								}
								int pageind = Integer.parseInt(res);
								if (page.size() == 0) {
									deletingFiles(pageind, pageInfoVector, t);
								} else {

									Object max = getMaxInPage(page);
									Object min = getMinInPage(page);
									((PageInfo) pageInfoVector.get(pageind)).setMax(max);
									((PageInfo) pageInfoVector.get(pageind)).setMin(min);

									serializePage(t, page, t.getTableName() + "" + pageind);
									serializeIndex(myOct, strTableName);
									myOct = null;
									page = null;
								}
							}
						}
					}
				}
				// End of the new part

				int pageind = -1;
				if (htblColNameValue.containsKey(myCluster)) {
//					 System.out.println("was hereee");
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
					// System.out.println(pageind);
					pageInfoVector = t.getPageInfo();
					if (pageInfoVector.size() != 0) {
						String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
						Page page = deserializePage(pagename);
//						System.out.println("before contains");
						if (page.contains(myTuple)) {
//							System.out.println("after contains");
							// System.out.println("was here111111");
							deleteFromOctree(myTuple,t);
							page.removeBinary(myTuple);
							if (page.size() == 0) {
								deletingFiles(pageind, pageInfoVector, t);
							} else {

								Object max = getMaxInPage(page);
								Object min = getMinInPage(page);
								((PageInfo) pageInfoVector.get(pageind)).setMax(max);
								((PageInfo) pageInfoVector.get(pageind)).setMin(min);

								serializePage(t, page, t.getTableName() + "" + pageind);
								page = null;
							}
						}

					}
				} else {
					pageInfoVector = t.getPageInfo();
					removeFromAllPages(pageInfoVector, myTuple, 0, t, htblColNameValue);
					deleteFromOctree(myTuple,t);
				}
				serializeTable(t, strTableName);
				t = null;
			} else {
				this.isDeletingMethod = false;
//				System.out.println("ana hena");
				return;
//				throw new DBAppException("Reenter your values!");
			}
		} else

		{
			this.isDeletingMethod = false;
			throw new DBAppException("Table doesn't exist");
		}
	}

	private void deleteFromOctree(Tuple myTuple, Table t) {
		for(int tableIndex=0;tableIndex<t.getIndexOnCol().size();tableIndex++) {
		String currIndex = t.getIndexOnCol().get(tableIndex).get("Name of Index").toString();
		String col1=""; 
		String col2="";
		String col3="";
		int counter=0;
		for(String tableKey:t.getIndexOnCol().get(tableIndex).keySet()) {
			if(counter==0)
				col1=tableKey;
			if(counter==1)
				col2=tableKey;
			if(counter==2)
				col3=tableKey;
			counter++;
		}
		Hashtable<String,Object> tobedeleted = new Hashtable<>();
		for(String myKey:myTuple.getRecord().keySet()) {
			if(myTuple.getRecord().get(col1).equals(myKey)) {
				tobedeleted.put(myKey, myTuple.getRecord().get(myKey).toString());
			}
			if(myTuple.getRecord().get(col2).equals(myKey)) {
				tobedeleted.put(myKey, myTuple.getRecord().get(myKey).toString());
			}
			if(myTuple.getRecord().get(col3).equals(myKey)) {
				tobedeleted.put(myKey, myTuple.getRecord().get(myKey).toString());
			}
		}
		Octree currOctree = deserializeOctree(currIndex);
		currOctree.deleteTuple(tobedeleted);
		serializeIndex(currOctree, currIndex);
	}
		
	}

	private void deletingFiles(int pageind, Vector pageInfoVector, Table t) { // 3lshan lw page kolah dups yms7ha 3ala
																				// tool

		// TODO Auto-generated method stub
		File mySerial = new File("src/main/resources/Data/" + t.getTableName() + "" + pageind + ".ser");
		if (mySerial.delete())
			// System.out.println("File deleted successfully");
			try {
				File oldFile;
				File newFile;
				pageInfoVector.remove(pageind);
				t.setCurrentMaxId(t.getCurrentMaxId() - 1);
				for (int i = pageind; i < pageInfoVector.size(); i++) {
					int temp = i + 1;
					oldFile = new File("src/main/resources/Data/" + t.getTableName() + "" + temp + ".ser");
					newFile = new File("src/main/resources/Data/" + t.getTableName() + "" + i + ".ser");
					((PageInfo) pageInfoVector.get(i)).setPageName(t.getTableName() + "" + i);
					if (oldFile.renameTo(newFile)) {
						// System.out.println("File renamed successfully");
					} else {
						// System.out.println("Failed to rename file");
					}
				}

			} catch (Exception e) {// msh 3ayez e3ml 7aga
			}
	}

	// 3ayez awel lma 2ms7 row mn page 2geeb mn elmin mn elpage eltanya w 27otha
	// fel page ely ana wa2ef feeha
	private void removeFromAllPages(Vector pageInfoVector, Tuple myTuple, int i, Table t, Hashtable htblColNameValue) {
		// TODO Auto-generated method stub
		if (pageInfoVector.size() == i)
			return;
		String pagename = ((PageInfo) (pageInfoVector.get(i))).getPageName();
		Page page = deserializePage(pagename);
		page.removeNonBinary(myTuple);
		if (page.size() == 0) {
			deletingFiles(i, pageInfoVector, t);
			removeFromAllPages(pageInfoVector, myTuple, i, t, htblColNameValue);
		} else {
			Object max = getMaxInPage(page);
			Object min = getMinInPage(page);
			((PageInfo) pageInfoVector.get(i)).setMax(max);
			((PageInfo) pageInfoVector.get(i)).setMin(min);
			serializePage(t, page, pagename);
			page = null;
			removeFromAllPages(pageInfoVector, myTuple, ++i, t, htblColNameValue);
		}
	}

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

	private boolean isValid(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
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
					String[] array = { x[2], x[6], x[7] };// {type,min,max}
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
			if (!ClustKeyfound && !isDeletingMethod && !isUpdatingMethod) { // ana 3mlt deh 3lshan fel delete lw ana msh
																			// m3aya elcluster key
				// e3ml delete 3ady bardo
				throw new DBAppException("You have to insert Clustering Key");
			}
			boolean flag = true;

			for (String key : htblColNameValue.keySet()) {
				// System.out.println(key);
				if (!tableInfo.containsKey(key)) {
					System.out.println(key);
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
						if (isDeletingMethod) {
							if (!(compareNameType instanceof java.lang.Integer))
								throw new DBAppException("Reenter your values!");
							else
								return false;

						} else
							// System.out.println("int");
							return false;
					}
				}
				case "java.lang.String": {

					if (compareNameType instanceof java.lang.String
							&& tableInfo.get(key)[1].toLowerCase()
									.compareTo(htblColNameValue.get(key).toString().toLowerCase()) <= 0
							&& tableInfo.get(key)[2].toLowerCase()
									.compareTo(htblColNameValue.get(key).toString().toLowerCase()) >= 0) {
						flag = true;

						continue;
					} else {
						if (isDeletingMethod) {

							if (!(compareNameType instanceof java.lang.String))
								throw new DBAppException("Reenter your values!");
							else
								return false;
						} else
							// System.out.println("int");
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
						if (isDeletingMethod) {
							if (!(compareNameType instanceof java.lang.Double))
								throw new DBAppException("Reenter your values!");
							else
								return false;

						} else
							// System.out.println("int");
							return false;
					}
				}
				case "java.util.Date": {
					// System.out.println("here date" + compareNameType);
					try {
						if (compareNameType instanceof java.util.Date) {

							Date currDate = (Date) compareNameType;
							Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse(tableInfo.get(key)[1]);
							Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse(tableInfo.get(key)[2]);

							if (currDate.after(minDate) && currDate.before(maxDate)) {
								flag = true;

								continue;

							} else {
//							System.out.println("date1");
//							System.out.println(currDate.after(minDate));
//							System.out.println(currDate.before(maxDate));
//							System.out.println(currDate);
//							System.out.println(maxDate+"  "+tableInfo.get(key)[2]);

								return false;
							}
						} else {

							if (isDeletingMethod)
								throw new DBAppException("Reenter your values!");
							else
								return false;

						}
					} catch (ParseException e) {
						return false;
					}

				}
				default:
					throw new DBAppException("Invalid type");
				}
			}
			return true;

		} catch (IOException e) {
			throw new DBAppException("CSV doesn't exist");
		}
	}

	// isValidIndex checks if the Tuple to be inserted has all Columns that have an
	// index on otherwise throws an exception
	private void isValidIndex(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			Hashtable<String, String[]> tableInfo = new Hashtable<String, String[]>();
			String ClustKey = "";

			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					if (!x[4].equals("null")) {
						if (htblColNameValue.get(x[1]) == null) {
							throw new DBAppException("Cant insert null values in col with index on");

						}
					}
				}
				line = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			throw new DBAppException("CSV doesn't exist");
		}

	}

	private static void serializePage(Table t, Page p, String name) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src/main/resources/Data/" + name + ".ser", false);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();
			int ind = Integer.parseInt(name.charAt(name.length() - 1) + "");
			Vector<PageInfo> piVector = t.getPageInfo();
			PageInfo pi = piVector.get(ind);
			pi.setCount(p.size());
			t = null;
			p = null;
			// System.out.println("Page is serialized successfully");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	private static Page deserializePage(String name) {
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/Data/" + name + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page p = (Page) in.readObject();
			in.close();
			fileIn.close();
			return p;
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private static void serializeTable(Table t, String name) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src/main/resources/Data/" + name + ".ser", false);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(t);
			out.close();
			fileOut.close();
			t = null;
			// System.out.println("Table is serialized successfully");
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	private static Table deserializeTable(String name) {
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/Data/" + name + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Table table = (Table) in.readObject();
			in.close();
			fileIn.close();

			return table;
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private static void serializeIndex(Octree p, String indexName) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src/main/resources/Data/" + indexName + ".ser", false);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();
			// System.out.println("Page is serialized successfully");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	private static Octree deserializeOctree(String indexName) {
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/Data/" + indexName + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Octree p = (Octree) in.readObject();
			in.close();
			fileIn.close();
			return p;
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			PageInfo pi = (PageInfo) (pageInfoVector.get(mid));

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
		// System.out.println(mid);
		return mid;

	}

	private static int binarySearchDate(Table t, Date ClustKey) {
		Vector pageInfoVector = t.getPageInfo();
		int low = 0;
		int high = pageInfoVector.size() - 1;
		int mid = 0;
		while (low <= high) {
			mid = (high + low) / 2;
			Date minDate = ((Date) ((PageInfo) (pageInfoVector.get(mid))).getMin());
			Date maxDate = ((Date) ((PageInfo) (pageInfoVector.get(mid))).getMax());

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

			mid = (high + low) / 2; // 0
			if ((ClustKey.toLowerCase())
					.compareTo((((PageInfo) (pageInfoVector.get(mid))).getMin()).toString().toLowerCase()) < 0) {
				high = mid - 1;
			} else {
				if ((ClustKey.toLowerCase())
						.compareTo((((PageInfo) (pageInfoVector.get(mid))).getMax()).toString().toLowerCase()) > 0) {
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

	private static Object getMinInPage(Page page) {
//		Tuple min = ((Tuple) page.get(0));
//		for (int i = 0; i < page.size(); i++) {
//			if (((Tuple) page.get(i)).compareTo(min) < 0) {
//				min = (Tuple) page.get(i);
//			}
//		}
//		return min.Clusteringkey;
		Tuple min = ((Tuple) page.get(0));
		return min.getClusteringkey();
	}

	private static Object getMaxInPage(Page page) {

		Tuple max = page.get(page.size() - 1);
		return max.getClusteringkey();
	}

	public static void getPages(String tableName) { // only for testing

		Table t = deserializeTable(tableName);
		System.out.println("No. of pages " + (t.getCurrentMaxId() + 1));
		for (int i = 0; i < t.getPageInfo().size(); i++) {
			String pagename = ((PageInfo) ((t.getPageInfo()).get(i))).getPageName();
			Page p = deserializePage(pagename);
			Object min = ((PageInfo) ((t.getPageInfo()).get(i))).getMin();
			Object max = ((PageInfo) ((t.getPageInfo()).get(i))).getMax();
			System.out.println(i + " " + pagename + " " + min.toString() + " " + max.toString() + " " + p.size());
			for (int j = 0; j < p.size(); j++) {
				Tuple tup = (Tuple) p.get(j);
				for (String key : tup.record.keySet()) {
					System.out.println("Col : " + key + "\t\t Value : " + tup.record.get(key).toString());
				}

			}
			serializePage(t, p, pagename);
		}
		serializeTable(t, tableName);
		System.out.println("----------------------------");

	}

	private static String getClusteringKeyType(String strTableName) {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

			String line = br.readLine();
			Hashtable<String, String[]> tableInfo = new Hashtable<String, String[]>();
			String ClustKey = "";
			boolean found = false;
			boolean ClustKeyfound = false;
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {

					if (x[3].equals("True")) {
						return x[2];
					}
					found = true;
				} else {
					if (found)
						break;
				}

				line = br.readLine();
			}
			br.close();
		} catch (Exception e) { // update exceptions throw
			// System.out.print("exception in update");
		}
		return null;
	}

	private static boolean canCreate(Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
		boolean flag = true;
		if (!(htblColNameType.size() == htblColNameMin.size() && htblColNameMax.size() == htblColNameType.size()))
			return false;
		for (String key : htblColNameType.keySet()) {
			String type = htblColNameType.get(key);
			if (type.equals("java.util.Date")) {
				// System.out.println(type);
				String min = htblColNameMin.get(key);
				String max = htblColNameMax.get(key);
				if (min == null || max == null)
					return false;
				try {
					Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse(min);
					Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse(max);
//					System.out.println(minDate);
//					System.out.println(maxDate);
					if (minDate.after(maxDate))
						throw new DBAppException("Min should be smaller than Max");

				} catch (ParseException e) {
					throw new DBAppException("Min and Max not in correct format");
				}
			} else {
				if (type.equals("java.lang.Double")) {
					String min = htblColNameMin.get(key);
					String max = htblColNameMax.get(key);
					if (min == null || max == null)
						return false;

					try {
						double minDouble = Double.parseDouble(min);
						double maxDouble = Double.parseDouble(max);
						if (minDouble > maxDouble)
							throw new DBAppException("Min should be smaller than Max");
					} catch (java.lang.NumberFormatException e) {
						throw new DBAppException("Min and Max not in correct format");
					}
				} else {
					if (type.equals("java.lang.Integer")) {
						String min = htblColNameMin.get(key);
						String max = htblColNameMax.get(key);
						if (min == null || max == null)
							return false;
						try {
							int minInt = Integer.parseInt(min);
							int maxInt = Integer.parseInt(max);
							if (minInt > maxInt)
								throw new DBAppException("Min should be smaller than Max");

						} catch (java.lang.NumberFormatException e) {
							throw new DBAppException("Min and Max not in correct format");
						}

					} else {
						if (type.equals("java.lang.String")) {
							String min = htblColNameMin.get(key);
							String max = htblColNameMax.get(key);
							if (min == null || max == null)
								return false;
							if (min.toLowerCase().compareTo(max.toLowerCase()) > 0)
								throw new DBAppException("Min should be smaller than Max");
						} else
							return false;
					}
				}

			}

		}
		return true;
	}

	private static Octree createOctree(String strTableName, String[] strarrColName) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			Object[] arrayMinAndMax = new Object[6];
			String[] arrayName = new String[3];
			int j = 0;
			int k = 0;
			int n = 0;
			while (line != null && n < 3) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					for (int i = 0; i < strarrColName.length; i++) {
						if (x[1].equals(strarrColName[i].toLowerCase())) {
							n++;
							arrayName[j++] = strarrColName[i];
							switch (x[2]) {
							case "java.lang.Integer":
								arrayMinAndMax[k++] = Integer.parseInt(x[6]);
								arrayMinAndMax[k++] = Integer.parseInt(x[7]);
								System.out.println("Are Integers");
								break;
							case "java.lang.Double":
								arrayMinAndMax[k++] = Double.parseDouble(x[6]);
								arrayMinAndMax[k++] = Double.parseDouble(x[7]);
								break;
							case "java.util.Date":
								try {
									arrayMinAndMax[k++] = new SimpleDateFormat("yyyy-MM-dd").parse(x[6]);
									arrayMinAndMax[k++] = new SimpleDateFormat("yyyy-MM-dd").parse(x[7]);

								} catch (ParseException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								break;
							default:
								arrayMinAndMax[k++] = x[6].toLowerCase();
								arrayMinAndMax[k++] = x[7].toLowerCase();

							}
							// x[4] = strarrColName[0].toLowerCase() + strarrColName[1].toLowerCase() +
							// strarrColName[2].toLowerCase() + "Index";
							// x[5] = "Octree";
						}
					}
				}
				line = br.readLine();
			}

			br.close();
			Octree tree = new Octree(arrayName[0].toLowerCase(), arrayName[1].toLowerCase(), arrayName[2].toLowerCase(),
					arrayMinAndMax[0], arrayMinAndMax[1], arrayMinAndMax[2], arrayMinAndMax[3], arrayMinAndMax[4],
					arrayMinAndMax[5]);

			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String data = "";
			line = br.readLine();
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					for (int i = 0; i < strarrColName.length; i++) {
						if (x[1].equals(strarrColName[i].toLowerCase())) {
							x[4] = arrayName[0].toLowerCase() + arrayName[1].toLowerCase() + arrayName[2].toLowerCase()
									+ "Index";
							x[5] = "Octree";
							// if(x[3].equals("TRUE"))

						}
					}
				}
				data += String.join(",", x) + "\n";

				line = br.readLine();
			}

			br.close();

			FileOutputStream out = new FileOutputStream("src/main/resources/metadata.csv");
			out.write(data.getBytes());
			out.close();

			return tree;

		} catch (IOException e) {
			throw new DBAppException("CSV doesn't exist");
		}

	}

	private static void checkIndexMeta(String strTableName, String[] strarrColName) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			boolean[] flags = { false, false, false };
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					for (int i = 0; i < strarrColName.length; i++) {
						if (x[1].equals(strarrColName[i].toLowerCase())) {
							if (!x[4].equals("null")) {
								throw new DBAppException("An Index already exits");
							}

							flags[i] = true;
						}
					}
				}
				line = br.readLine();
			}
			br.close();
			if ((flags[0] && flags[1] && flags[2]) == false) {
				System.out.println(Arrays.toString(flags));
				throw new DBAppException("A Column or More doesnt exist");
			}

		} catch (IOException e) {
			throw new DBAppException("CSV doesn't exist");
		}

	}

	// insertIntoIndex creates a hashtable with X,Y,Z and pagename for every index
	// then call method insert in Octree Class
	private void insertIntoIndex(String strTableName, Hashtable<String, Object> htblColNameValue, String pagename)
			throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			ArrayList<String> indices = new ArrayList<String>();
			String ClustKey = "";
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					if (!x[4].equals("null")) {
						if (!indices.contains(x[4]))
							indices.add(x[4]);
					}
					if (x[3].equals("True")) {
						ClustKey = x[1];
					}
				}
				line = br.readLine();
			}
			br.close();
			for (int i = 0; i < indices.size(); i++) {
				String TreeName = strTableName + indices.get(i);
				Octree Octree = deserializeOctree(TreeName);
				Object val1 = htblColNameValue.get(Octree.getX());
				Object val2 = htblColNameValue.get(Octree.getY());
				Object val3 = htblColNameValue.get(Octree.getZ());
				Hashtable<String, Object> recInIndex = new Hashtable<>();
				recInIndex.put(Octree.getX(), val1);
				recInIndex.put(Octree.getY(), val2);
				recInIndex.put(Octree.getZ(), val3);
				recInIndex.put("Page Name", pagename);
				recInIndex.put("Clust key", htblColNameValue.get(ClustKey));

				Octree.insertTupleInIndex(recInIndex);
				serializeIndex(Octree, TreeName);
				Octree = null;

			}

		} catch (IOException e) {
			throw new DBAppException("CSV doesn't exist");
		}

	}

	private void updateTupleinIndex(String strTableName, Hashtable<String, Object> oldRecord,
			Hashtable<String, Object> newRecord, String pageName, Hashtable<String, Object> updated)
			throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			ArrayList<String> indices = new ArrayList<String>();
			String ClustKey = "";
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					if (!x[4].equals("null")) {
						if (!indices.contains(x[4]))
							indices.add(x[4]);
					}
					if (x[3].equals("True")) {
						ClustKey = x[1];
					}
				}
				line = br.readLine();
			}
			br.close();
			for (int i = 0; i < indices.size(); i++) {
				String TreeName = strTableName + indices.get(i);
				Octree Octree = deserializeOctree(TreeName);
				if (updated.get(Octree.getX()) != null && updated.get(Octree.getY()) != null
						&& updated.get(Octree.getZ()) != null) {
					Object val1 = oldRecord.get(Octree.getX());
					Object val2 = oldRecord.get(Octree.getY());
					Object val3 = oldRecord.get(Octree.getZ());
					Hashtable<String, Object> recInIndex = new Hashtable<>();
					recInIndex.put(Octree.getX(), val1);
					recInIndex.put(Octree.getY(), val2);
					recInIndex.put(Octree.getZ(), val3);
					recInIndex.put("Page Name", pageName);
					recInIndex.put("Clust key", oldRecord.get(ClustKey));
					Octree.deleteTuple(recInIndex);
					val1 = newRecord.get(Octree.getX());
					val2 = newRecord.get(Octree.getY());
					val3 = newRecord.get(Octree.getZ());
					recInIndex = new Hashtable<>();
					recInIndex.put(Octree.getX(), val1);
					recInIndex.put(Octree.getY(), val2);
					recInIndex.put(Octree.getZ(), val3);
					recInIndex.put("Page Name", pageName);
					recInIndex.put("Clust key", oldRecord.get(ClustKey));

					Octree.insertTupleInIndex(recInIndex);
					Octree = null;
				}
				serializeIndex(Octree, TreeName);
				Octree = null;
			}

		} catch (IOException e) {
			throw new DBAppException("CSV doesn't exist");
		}

	}

	private void updateRefrenceInIndex(String strTableName, Hashtable<String, Object> htblColNameValue, String pageName,
			String oldPageName) throws DBAppException {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

			String line = br.readLine();
			ArrayList<String> indices = new ArrayList<String>();

			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					if (!x[4].equals("null")) {
						if (!indices.contains(x[4]))
							indices.add(x[4]);
					}
				}
				line = br.readLine();
			}
			br.close();
			for (int i = 0; i < indices.size(); i++) {
				String TreeName = strTableName + indices.get(i);
				Octree Octree = deserializeOctree(TreeName);
				Octree.updateTupleReferenceInIndex(htblColNameValue, pageName, oldPageName);
				serializeIndex(Octree, TreeName);
				Octree = null;

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("CSV doesn't exist");
		}

	}

	private static boolean hasThreeIndexMeta(String strTableName, String[] strarrColName) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			boolean[] flags = { false, false, false };
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(strTableName)) {
					for (int i = 0; i < strarrColName.length; i++) {
						if (x[1].equals(strarrColName[i])) {
							flags[i] = true;
						}
					}
				}
				line = br.readLine();
			}
			br.close();
			if ((flags[0] && flags[1] && flags[2]) == false) {
				return false;
			}
			return true;

		} catch (IOException e) {
			throw new DBAppException("CSV doesn't exist");
		}

	}

//	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
//		String indxName;
//		if (arrSQLTerms.length == 0)
//			throw new DBAppException("Insert a valid select");
//		if (!tableExits(arrSQLTerms[0].get_strTableName()))
//			throw new DBAppException("Table doesn't exist!!");
//		Vector result = new Vector<>();
//		Table t = deserializeTable(arrSQLTerms[0].get_strTableName());
//		Hashtable<String, Object> htblColValue = new Hashtable<>();
//		Vector<String> indexName = new Vector<>();
//		for (int i = 0; i < arrSQLTerms.length; i++) {
//			htblColValue.put(arrSQLTerms[i].get_strColumnName(), arrSQLTerms[i].get_objValue());
//		}
//		if (!isValid(t.getTableName(), htblColValue))
//			throw new DBAppException("Coloumn is invalid");
//		indexName = getindexname(t, htblColValue);
//		Queue<Hashtable<String, Object>> resIndex = new LinkedList<Hashtable<String, Object>>();
//		Queue<String> operators = new LinkedList<String>();
//		boolean notIndex = false;
//		int tempCounter = 0;
//		int colCounter = 0;
//		int operatorCounter = 0;
//		int outerCounter = 0;
//		Boolean[] taken = new Boolean[indexName.size()];
//		boolean once = true;
//		boolean flag = true;
//		for (int indexCounter = 0; indexCounter < indexName.size(); indexCounter++) {
//			Hashtable<String, Object> myHtbl = new Hashtable<String, Object>();
//			myHtbl.put(arrSQLTerms[colCounter].get_strColumnName(), arrSQLTerms[colCounter].get_objValue());
//			myHtbl.put("operator" + arrSQLTerms[colCounter].get_strColumnName(),
//					arrSQLTerms[colCounter].get_strOperator());
//			if (taken[colCounter]) {
//				colCounter++;
//				indexCounter--;
//				continue;
//			}
//			colCounter++;
//			for (int j = indexCounter + 1; j < indexName.size(); j++) {
//				if (myHtbl.size() == 6) {
////					operatorCounter++;
////					colCounter++;
//					break;
//				}
//				if (!strarrOperators[operatorCounter].equals("AND")) {
//					// notIndex=true;
////					operatorCounter++;
////					colCounter++;
//					break;
//				}
//				if (indexName.get(indexCounter).equals(indexName.get(j)) && myHtbl.size() < 6) {
//					myHtbl.put(arrSQLTerms[colCounter].get_strColumnName(), arrSQLTerms[colCounter].get_objValue());
//					myHtbl.put("operator" + arrSQLTerms[colCounter].get_strColumnName(),
//							arrSQLTerms[colCounter].get_strOperator());
//					indexName.remove(j);
//					j--;
////					if(flag)
////						outerCounter++;
//					taken[colCounter] = true;
//				} else {
//					if (once) {
//						outerCounter = colCounter;
//						once = false;
//					}
//					flag = false;
//				}
//				operatorCounter++;
//				colCounter++;
//			}
//			if (!flag) {
//				colCounter = outerCounter;
//				operatorCounter = outerCounter - 1;
//			}
//			once = true;
//			flag = true;
//			myHtbl.put("indxName", indexName.get(indexCounter));
//			resIndex.add(myHtbl);
//			if (resIndex.size() == 2) {
//				resIndex = compute(resIndex, operators, t);
//			}
//			if (operatorCounter < strarrOperators.length) {
//				operators.add(strarrOperators[operatorCounter]);
//				operatorCounter++;
//			}
//
////			if(tempCounter!=0)
////				operators.add(strarrOperators[operatorCounter]);
////			tempCounter++;
////			if(!notIndex && myHtbl.size()==6) {
////				//computer hashtable then insert into Queue
////				
////				resIndex.add(myHtbl);
////				if(tempCounter!=0)
////					operators.add(strarrOperators[operatorCounter]);
////				tempCounter++;
////			}
////			else if (notIndex
////					notIndex=false;
//		}
//
//		if (flag && hasThreeIndexMeta(t.getTableName(), colName) && strarrOperators[0] == "AND") {
//			indxName = getindexname(t, colName);
//			Octree o = deserializeOctree(t.getTableName() + "" + indxName);
//			Vector<String> res = o.getPageName(htblColValue);
//			Tuple tuple = new Tuple(t.getClusteringKey(), htblColValue);
//			for (int i = 0; i < res.size(); i++) {
//				Page p = deserializePage(res.get(i));
//				result.add(p.PrintTuple(tuple));
//
//			}
//		} else {
//			// TODO sequentially
//			Tuple myTuple = new Tuple(t.getClusteringKey(), htblColValue);
//			int pageind = -1;
//			String myCluster = t.getClusteringKey();
//			if (htblColValue.containsKey(myCluster)) {
////				 System.out.println("was hereee");
//				Object myClusterType = htblColValue.get(myCluster);
//				if (myClusterType instanceof java.lang.Integer) {
//					pageind = binarySearchInt(t, (Integer) myClusterType);
//				}
//				if (myClusterType instanceof java.lang.String) {
//					pageind = binarySearchString(t, (String) myClusterType);
//				}
//				if (myClusterType instanceof java.lang.Double) {
//					pageind = binarySearchDouble(t, (Double) myClusterType);
//				}
//				if (myClusterType instanceof java.util.Date) {
//					pageind = binarySearchDate(t, (Date) myClusterType);
//				}
//				// System.out.println(pageind);
//				Vector pageInfoVector = t.getPageInfo();
//				if (pageInfoVector.size() != 0) {
//					String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
//					Page page = deserializePage(pagename);
////					System.out.println("before contains");
//					if (page.contains(myTuple)) {
////						System.out.println("after contains");
//						// System.out.println("was here111111");
//						int index = page.getIndexInPage(myTuple);
//						result.add(page.get(index).toString());
//						serializePage(t, page, t.getTableName() + "" + pageind);
//						page = null;
//					}
//
//				}
//			} else {
//				for (int i = 0; i < t.getPageInfo().size(); i++) {
//					Page page = deserializePage(t.getPageInfo().get(i).getPageName());
//					for (Tuple myTuple2 : page) {
//						if (myTuple2.equals(myTuple)) {
//							result.add(myTuple2.toString());
//
//						}
//					}
//
//				}
//			}
//
//		}
//		return result.iterator();
//
//	}

//es2l 3ala 7tt elcomplexity mohma wala la2?
	public static void compute(Queue<Hashtable<String, Object>> resIndex, Queue<String> operators, Table t) {
		Hashtable<String, Object> htbloperand1 = resIndex.poll();
		Tuple operand1 = new Tuple(t, htbloperand1);
		Hashtable<String, Object> htbloperand2 = resIndex.poll();
		Tuple operand2 = new Tuple(t, htbloperand2);
		String strOperator = operators.poll();
		String value1, value2;
		value1 = htbloperand1.get("indxName").toString();
		value2 = htbloperand2.get("indxName").toString();
		Octree myOct1 = null;
		Octree myOct2 = null;
		Vector<Tuple> result = new Vector<Tuple>();
		Vector<String> pages1 = new Vector<String>();
		if (value1 != null && htbloperand1.size() == 7) {
			myOct1 = deserializeOctree(value1);
			pages1 = myOct1.getPageName(htbloperand1);
		}

		Vector<String> pages2 = new Vector<String>();
		if (value2 != null && htbloperand2.size() == 7) {
			myOct2 = deserializeOctree(value2);
			pages2 = myOct2.getPageName(htbloperand2);
		}
		boolean greaterThan1, equal1, smallerThan1, NotEqual1, greaterThanOrEqual1, SmallerThanOrEqual1, greaterThan2,
				equal2, smallerThan2, NotEqual2, greaterThanOrEqual2, SmallerThanOrEqual2, greaterThan3, equal3,
				smallerThan3, NotEqual3, greaterThanOrEqual3, SmallerThanOrEqual3;
//		boolean[] greaterThan,equal,smallerThan,notequal,greaterthanorequal,smallerthanorequal,notEqual= {false,false,false};
//		momken ne3melha as a loop fel arrays beta3et kol operator
		Hashtable<String, Integer> Bitmask1 = new Hashtable<>();
		Hashtable<String, Integer> Bitmask2 = new Hashtable<>();
		for (String key : htbloperand1.keySet()) {
			if (key.substring(0, 8).equals("operator")) {
				String newKey = key.substring(8);
				Bitmask1.put(newKey, get6BitString(htbloperand1.get(key).toString()));
			}
		}
		for (String key : htbloperand2.keySet()) {
			if (key.substring(0, 8).equals("operator")) {
				String newKey = key.substring(8);
				Bitmask2.put(newKey, get6BitString(htbloperand2.get(key).toString()));
			}
		}

		switch (strOperator) {
		case "AND":
			if (pages1.size() != 0 && pages2.size() != 0) {
				pages1.retainAll(pages2);
				for (int i = 0; i < pages1.size(); i++) {
					Page page = deserializePage(pages1.get(i));
					for (Tuple myTuple : page) {
						boolean flag = true;
						for (String key : Bitmask1.keySet()) {
							switch (Bitmask1.get(key)) {
							case 1:
								flag = smallerThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag = smallerThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag = greaterThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag = greaterThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag = NotEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag = Equal(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag)
								break;
						}
						if (flag) {
							for (String key : Bitmask2.keySet()) {
								switch (Bitmask2.get(key)) {
								case 1:
									flag = smallerThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 2:
									flag = smallerThan(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 4:
									flag = greaterThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 8:
									flag = greaterThan(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 16:
									flag = NotEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 32:
									flag = Equal(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								}
								if (!flag)
									break;
							}
						}
						if (flag) {
							result.add(myTuple);
						}
					}
					serializePage(t, page, pages1.get(i));
				}
			}
			if (pages1.size() == 0 && pages2.size() != 0) {
				for (int i = 0; i < pages2.size(); i++) {
					Page page = deserializePage(pages2.get(i));
					for (Tuple myTuple : page) {
						boolean flag = true;
						for (String key : Bitmask1.keySet()) {
							switch (Bitmask1.get(key)) {
							case 1:
								flag = smallerThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag = smallerThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag = greaterThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag = greaterThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag = NotEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag = Equal(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag)
								break;
						}
						if (flag) {
							for (String key : Bitmask2.keySet()) {
								switch (Bitmask2.get(key)) {
								case 1:
									flag = smallerThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 2:
									flag = smallerThan(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 4:
									flag = greaterThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 8:
									flag = greaterThan(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 16:
									flag = NotEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 32:
									flag = Equal(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								}
								if (!flag)
									break;
							}
						}
						if (flag) {
							result.add(myTuple);
						}
					}
					serializePage(t, page, pages2.get(i));
				}
			}
			if (pages1.size() != 0 && pages2.size() == 0) {
				for (int i = 0; i < pages1.size(); i++) {
					Page page = deserializePage(pages1.get(i));
					for (Tuple myTuple : page) {
						boolean flag = true;
						for (String key : Bitmask1.keySet()) {
							switch (Bitmask1.get(key)) {
							case 1:
								flag = smallerThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag = smallerThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag = greaterThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag = greaterThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag = NotEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag = Equal(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag)
								break;
						}
						if (flag) {
							for (String key : Bitmask2.keySet()) {
								switch (Bitmask2.get(key)) {
								case 1:
									flag = smallerThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 2:
									flag = smallerThan(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 4:
									flag = greaterThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 8:
									flag = greaterThan(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 16:
									flag = NotEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 32:
									flag = Equal(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								}
								if (!flag)
									break;
							}
						}
						if (flag) {
							result.add(myTuple);
						}
					}
					serializePage(t, page, pages1.get(i));
				}
			}
			if (pages1.size() == 0 && pages2.size() == 0) {
				for (int i = 0; i < t.getPageInfo().size(); i++) {
					Page page = deserializePage(t.getPageInfo().get(i).getPageName());
					for (Tuple myTuple : page) {
						boolean flag = true;
						for (String key : Bitmask1.keySet()) {
							switch (Bitmask1.get(key)) {
							case 1:
								flag = smallerThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag = smallerThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag = greaterThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag = greaterThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag = NotEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag = Equal(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag)
								break;
						}
						if (flag) {
							for (String key : Bitmask2.keySet()) {
								switch (Bitmask2.get(key)) {
								case 1:
									flag = smallerThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 2:
									flag = smallerThan(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 4:
									flag = greaterThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 8:
									flag = greaterThan(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 16:
									flag = NotEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								case 32:
									flag = Equal(htbloperand2.get(key), myTuple.getRecord().get(key));
									break;
								}
								if (!flag)
									break;
							}
						}
						if (flag) {
							result.add(myTuple);
						}
					}
					serializePage(t, page, t.getPageInfo().get(i).getPageName());
				}
			}
//			operand1.get(strOperator) ;
			break;
		case "OR":
			if (pages1.size() != 0 && pages2.size() != 0) {
				Vector<String> myPages = union(pages1, pages2);
				for (int i = 0; i < myPages.size(); i++) {
					Page page = deserializePage(myPages.get(i));
					for (Tuple myTuple : page) {
						boolean flag1 = true;
						boolean flag2 = true;
						for (String key : Bitmask1.keySet()) {
							switch (Bitmask1.get(key)) {
							case 1:
								flag1 = smallerThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag1 = smallerThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag1 = greaterThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag1 = greaterThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag1 = NotEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag1 = Equal(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag1)
								break;
						}
						for (String key : Bitmask2.keySet()) {
							switch (Bitmask2.get(key)) {
							case 1:
								flag2 = smallerThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag2 = smallerThan(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag2 = greaterThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag2 = greaterThan(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag2 = NotEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag2 = Equal(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag2)
								break;
						}
						if (flag1 || flag2) {
							result.add(myTuple);
						}
					}
					serializePage(t, page, myPages.get(i));
				}
			} else {
				for (int i = 0; i < t.getPageInfo().size(); i++) {
					Page page = deserializePage(t.getPageInfo().get(i).getPageName());
					for (Tuple myTuple : page) {
						boolean flag1 = true;
						boolean flag2 = true;
						for (String key : Bitmask1.keySet()) {
							switch (Bitmask1.get(key)) {
							case 1:
								flag1 = smallerThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag1 = smallerThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag1 = greaterThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag1 = greaterThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag1 = NotEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag1 = Equal(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag1)
								break;
						}
						for (String key : Bitmask2.keySet()) {
							switch (Bitmask2.get(key)) {
							case 1:
								flag2 = smallerThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag2 = smallerThan(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag2 = greaterThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag2 = greaterThan(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag2 = NotEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag2 = Equal(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag2)
								break;
						}
						if (flag1 || flag2) {
							result.add(myTuple);
						}
					}
					serializePage(t, page, t.getPageInfo().get(i).getPageName());
				}
			}
			break;
		case "XOR":
			if (pages1.size() != 0 && pages2.size() != 0) {
				Vector<String> myPages = xor(pages1, pages2);
				for (int i = 0; i < myPages.size(); i++) {
					Page page = deserializePage(myPages.get(i));
					for (Tuple myTuple : page) {
						boolean flag1 = true;
						boolean flag2 = true;
						for (String key : Bitmask1.keySet()) {
							switch (Bitmask1.get(key)) {
							case 1:
								flag1 = smallerThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag1 = smallerThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag1 = greaterThanOrEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag1 = greaterThan(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag1 = NotEqual(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag1 = Equal(htbloperand1.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag1)
								break;
						}
						for (String key : Bitmask2.keySet()) {
							switch (Bitmask2.get(key)) {
							case 1:
								flag2 = smallerThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 2:
								flag2 = smallerThan(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 4:
								flag2 = greaterThanOrEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 8:
								flag2 = greaterThan(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 16:
								flag2 = NotEqual(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							case 32:
								flag2 = Equal(htbloperand2.get(key), myTuple.getRecord().get(key));
								break;
							}
							if (!flag2)
								break;
						}
						if (flag1 ^ flag2) {
							result.add(myTuple);
						}
					}
					serializePage(t, page, myPages.get(i));
				}
			}

			break;
		}
	}

	public static Vector<String> xor(Vector<String> v1, Vector<String> v2) {
		Vector<String> result = new Vector<String>();
		for (String element : v1) {
			if (!v2.contains(element)) {
				result.add(element);
			}
		}
		for (String element : v2) {
			if (!v1.contains(element)) {
				result.add(element);
			}
		}
		return result;
	}

	public static Vector<String> union(Vector<String> v1, Vector<String> v2) {
		Vector<String> result = new Vector<String>();
		result.addAll(v1);
		for (String item : v2) {
			if (!result.contains(item)) {
				result.add(item);
			}
		}
		return result;
	}

	private static boolean Equal(Object recordValue, Object operand) {
		if (operand instanceof Integer && recordValue instanceof Integer) {
			return ((Integer) recordValue).equals((Integer) operand);
		} else if (operand instanceof Double && recordValue instanceof Double) {
			return ((Double) recordValue).equals((Double) operand);
		} else if (operand instanceof Date && recordValue instanceof Date) {
			return ((Date) recordValue).equals((Date) operand);
		} else if (operand instanceof String && recordValue instanceof String) {
			return ((String) recordValue).equals((String) operand);
		}
		return false;
	}

	private static boolean NotEqual(Object recordValue, Object operand) {
		if (operand instanceof Integer && recordValue instanceof Integer) {
			return !((Integer) recordValue).equals((Integer) operand);
		} else if (operand instanceof Double && recordValue instanceof Double) {
			return !((Double) recordValue).equals((Double) operand);
		} else if (operand instanceof Date && recordValue instanceof Date) {
			return !((Date) recordValue).equals((Date) operand);
		} else if (operand instanceof String && recordValue instanceof String) {
			return !((String) recordValue).equals((String) operand);
		}
		return false;
	}

	private static boolean greaterThan(Object recordValue, Object operand) {
		if (operand instanceof Integer && recordValue instanceof Integer) {
			return (Integer) recordValue > (Integer) operand;
		} else if (operand instanceof Double && recordValue instanceof Double) {
			return (Double) recordValue > (Double) operand;
		} else if (operand instanceof Date && recordValue instanceof Date) {
			return ((Date) recordValue).after((Date) operand);
		} else if (operand instanceof String && recordValue instanceof String) {
			return ((String) recordValue).compareTo((String) operand) > 0;
		}
		return false;
	}

	private static boolean greaterThanOrEqual(Object recordValue, Object operand) {
		if (operand instanceof Integer && recordValue instanceof Integer) {
			return (Integer) recordValue >= (Integer) operand;
		} else if (operand instanceof Double && recordValue instanceof Double) {
			return (Double) recordValue >= (Double) operand;
		} else if (operand instanceof Date && recordValue instanceof Date) {
			return ((Date) recordValue).after((Date) operand) || ((Date) recordValue).equals((Date) operand);
		} else if (operand instanceof String && recordValue instanceof String) {
			return ((String) recordValue).compareTo((String) operand) >= 0;
		}
		return false;
	}

	private static boolean smallerThan(Object recordValue, Object operand) {
		if (operand instanceof Integer && recordValue instanceof Integer) {
			return (Integer) recordValue < (Integer) operand;
		} else if (operand instanceof Double && recordValue instanceof Double) {
			return (Double) recordValue < (Double) operand;
		} else if (operand instanceof Date && recordValue instanceof Date) {
			return ((Date) recordValue).before((Date) operand);
		} else if (operand instanceof String && recordValue instanceof String) {
			return ((String) recordValue).compareTo((String) operand) < 0;
		}
		return false;
	}

	private static boolean smallerThanOrEqual(Object recordValue, Object operand) {
		if (operand instanceof Integer && recordValue instanceof Integer) {
			return (Integer) recordValue <= (Integer) operand;
		} else if (operand instanceof Double && recordValue instanceof Double) {
			return (Double) recordValue <= (Double) operand;
		} else if (operand instanceof Date && recordValue instanceof Date) {
			return ((Date) recordValue).before((Date) operand) || ((Date) recordValue).equals((Date) operand);
		} else if (operand instanceof String && recordValue instanceof String) {
			return ((String) recordValue).compareTo((String) operand) <= 0;
		}
		return false;
	}

//	size el hash 2 -> 1 String  

// Equal-NotEqual-GreaterThan-GreaterThanOrEqual,SmallerThan,SmallerThanOrEqual
//	0-0-0-0-0-0
	public static int get6BitString(String value) {
		int result = 0;
		switch (value) {
		case ">":
			result |= 0b1000;
			break;
		case ">=":
			result |= 0b100;
			break;
		case "<":
			result |= 0b10;
			break;
		case "<=":
			result |= 0b1;
			break;
		case "=":
			result |= 0b100000;
			break;
		case "!=":
			result |= 0b10000;
			break;
		}
		return result;
	}

	private static int searchForPageToInsertUsingIndex(Table table, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			String TName = table.getTableName();
			String indexname = "";
			String max = "";
			while (line != null) {
				String[] x = line.split(",");
				System.out.println(x[1] + " " + x[3]);
				if (x[0].equals(TName) && x[3].equals("True")) {
					indexname = x[4];
					max = x[7];
				}
				line = br.readLine();
			}
			br.close();
			Octree tree = deserializeOctree(table.getTableName() + "" + indexname);
			String page = tree.searchForPageNameUsingIndex(htblColNameValue, table.getClusteringKey(), max);
			if (page == "") {
				tree = null;
				System.out.println(htblColNameValue.toString());
				System.out.println(table.getCurrentMaxId());
				return table.getPageInfo().get(table.getPageInfo().size() - 1).getId();
			} else {
				table = null;
				tree = null;
				return Integer.parseInt(page);
			}
			// serializeIndex(tree,)
		} catch (FileNotFoundException e) {
			throw new DBAppException("File not found");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("IO Exception");
		}
	}

	public static Vector<String> getindexname(Table t, Hashtable<String, Object> htbl) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			Hashtable<String, String[]> tableInfo = new Hashtable<String, String[]>();
			String TName = t.getTableName();
			Vector<String> res = new Vector<>();
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(TName)) {
					for (String key : htbl.keySet()) {
						if (x[1].equals(key)) {
							res.add(x[4]);
						}
					}
				}
				line = br.readLine();
			}
			br.close();
			return res;
		} catch (FileNotFoundException e) {
			throw new DBAppException("File not found");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("IO Exception");
		}
	}

	public static int getPageNumFromIndex(String t, Hashtable<String, Object> htbl, Object clustVal)
			throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			Hashtable<String, Integer> Indices = new Hashtable<String, Integer>();
			// String TName = t.getTableName();
			// Vector<String> res = new Vector<>();
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(t)) {
					if (x[4] != null) {
						if (htbl.containsKey(x[1])) {
							if (Indices.containsKey(x[4])) {
								Indices.put(x[4], Indices.get(x[4]) + 1);
							} else {
								Indices.put(x[4], 1);
							}
						}
					}
				}
				line = br.readLine();
			}
			br.close();
			int max = -1;
			String maxIndex = "";
			for (String key : Indices.keySet()) {
				if (Indices.get(key) > max)
					maxIndex = key;
			}

			if (max == -1)
				return -1;
			String treeName = t + maxIndex;
			Octree tree = deserializeOctree(treeName);
			String page = tree.getExactPage(htbl, tree.getRoot(), clustVal);
			return Integer.parseInt(page.charAt(page.length() - 1) + "");

		} catch (FileNotFoundException e) {
			throw new DBAppException("File not found");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("IO Exception");
		}
	}

	public void displayTree(String tableName) {
		Table t = deserializeTable(tableName);
		for (int k = 0; k < t.getIndex().size(); k++) {
			String IndexName = t.getIndex().get(k);
			Octree tree = deserializeOctree(IndexName);
			System.out.println("ColX: " + tree.getX() + " ColY:" + tree.getY() + " ColZ:" + tree.getZ());
			Node Current = tree.getRoot();
			int n = 0;
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
						System.out.println("MinX " + Current.getMinX() + "MaxX " + Current.getMaxX() + " ,MinY "
								+ Current.getMinY() + " ,MaxY " + Current.getMaxY() + ", MinZ " + Current.getMinZ()
								+ ", MaxZ " + Current.getMaxZ());

					} else {
						System.out.println("--NonLeaf-- Root");
						System.out.println("MinX " + Current.getMinX() + "MaxX " + Current.getMaxX() + " ,MinY "
								+ Current.getMinY() + " ,MaxY " + Current.getMaxY() + ", MinZ " + Current.getMinZ()
								+ ", MaxZ " + Current.getMaxZ());

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
						System.out.println("--Leaf-- Parent at level:" + (level - 1) + " Node " + parentnode + " Size "
								+ temp.getSize());
						System.out.println("MinX " + Current.getMinX() + " MaxX " + Current.getMaxX() + " ,MinY "
								+ Current.getMinY() + " ,MaxY " + Current.getMaxY() + " ,MinZ " + Current.getMinZ()
								+ " ,MaxZ " + Current.getMaxZ());

					} else {
						System.out.println("--Leaf-- Root");
						System.out.println("MinX " + Current.getMinX() + " MaxX " + Current.getMaxX() + " ,MinY "
								+ Current.getMinY() + " ,MaxY " + Current.getMaxY() + " ,MinZ " + Current.getMinZ()
								+ " ,MaxZ " + Current.getMaxZ());

					}
					System.out.println("--Main Bucket--");
					for (int i = 0; i < temp.getBucket().size(); i++) {
						Hashtable h = temp.getBucket().get(i);
						System.out.println(i + " " + h.toString());

					}
					System.out.println("--Overflow Bucket--");
					for (int i = 0; i < temp.getOverflow().size(); i++) {
						Hashtable h = temp.getOverflow().get(i);
						System.out.println(i + " " + h.toString());

					}
					System.out.println("----------------------------------------");

				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {

	}
}