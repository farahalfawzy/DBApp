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
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.expression.Expression;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

public class DBApp {

	// Vector<Table> allTable = new Vector<Table>();
	static int maxnoOfRows = getMaxRows();
	boolean isDeletingMethod = false;
	boolean isUpdatingMethod = false;
	boolean isSelectingMethod = false;

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
						writer.append("true,");
					else
						writer.append("false,");
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
		if (!tableExits(strTableName)) {
			throw new DBAppException("Table not found");

		}
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
		htblIndex.put("Name of Index", Octname);
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
				// recInIndex.put("Clust key", tup.getRecord().get(t.getClusteringKey()));
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
		//	System.out.println(htblColNameValue);
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
				//	System.out.println(pagename + "  kkkkkkk");

					if (page.containsClustKey(tuple)) {
						page = null;
						t = null;
						throw new DBAppException("Clustering Key already exists");
					} else {
						if (isSmallerThanmin) {
							if (pageind - 1 > -1) {

								Object max = ((PageInfo) pageInfoVector.get(pageind - 1)).getMax();
								if (max.equals(tuple.getClusteringkey())) {
									page = null;
									t = null;
									throw new DBAppException("Clustering Key already exists");
								}

								if (((PageInfo) (pageInfoVector.get(pageind - 1))).getCount() < maxnoOfRows) {
									serializePage(t, page, t.getTableName() + "" + pageind);

									pagename = ((PageInfo) (pageInfoVector.get(pageind - 1))).getPageName();
									page = deserializePage(pagename);
									if (page.containsClustKey(tuple)) {
										page = null;
										t = null;
										throw new DBAppException("Clustering Key already exists");
									}
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
						//	System.out.println(indexInPage + "   ll   " + newtup.getClusteringkey());
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
												t.getTableName() + "" + ind, t.getTableName() + "" + (ind - 1));

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
				//	System.out.println("PAGEIND" + pageind);
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
					page = null;
					t = null;
					updateTupleinIndex(strTableName, oldtuple, tuple.getRecord(), strTableName + "" + pageind,
							htblColNameValue);
					// page.replace(ClustObj, htblColNameValue);
					isUpdatingMethod = false;

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
				Tuple myTuple = new Tuple(htblColNameValue.get(t.getClusteringKey()), htblColNameValue);
//				System.out.println(htblColNameValue);
//				System.out.println(htblColNameValue.get(t.getClusteringKey()).getClass());
				Vector<String> indexNameVector = getindexname(t, htblColNameValue);
				String indexName = "";
				int count = 0;
				int minTemp = 9999999;
				for (int i = 0; i < indexNameVector.size(); i++) {
					for (int j = i + 1; j < indexNameVector.size(); j++) {
						if (indexNameVector.get(i).equals(indexNameVector.get(j))) {
							count++;
							indexNameVector.remove(j);
							j--;
						}
					}
					if (count < minTemp) {
						indexName = indexNameVector.get(i);
					}
				}
				if (!indexName.equals("null")) {
					Octree myOct = deserializeOctree(t.getTableName() + "" + indexName);
					Hashtable<String, Object> key = new Hashtable<>();
					if (htblColNameValue.get(myOct.getX()) != null)
						key.put(myOct.getX(), htblColNameValue.get(myOct.getX()));
					if (htblColNameValue.get(myOct.getY()) != null)
						key.put(myOct.getY(), htblColNameValue.get(myOct.getY()));
					if (htblColNameValue.get(myOct.getZ()) != null)
						key.put(myOct.getZ(), htblColNameValue.get(myOct.getZ()));
					Vector<String> pageName = myOct.getPageName(key);
					pageInfoVector = t.getPageInfo();
					Vector<Integer> tobedeleted = new Vector<Integer>();
					if (pageInfoVector.size() != 0) {
						for (int i = 0; i < pageName.size(); i++) {
							Page page = deserializePage(pageName.get(i));

							if (myTuple.getRecord().containsKey(t.getClusteringKey())) {
								int index = page.getIndexInPageUsingClusteringKey2(myTuple.getClusteringkey());
						//		System.out.println(index);
								if (page.get(index).equals(myTuple)) {
									myTuple = page.get(index);
									deleteFromOctree(myTuple, t, pageName.get(i));
									page.remove(index);
								}
							} else {
								for (int k = 0; k < page.size(); k++) {
									// get all the columns that have indices

									Tuple currTuple = page.get(k);
									if (currTuple.equals(myTuple)) {
										deleteFromOctree(currTuple, t, pageName.get(i));
										page.remove(k);
										k--;
									}
								}
							}
							String res = "";
							for (int j = pageName.get(i).length() - 1; j > (-1); j--) {
								if ((pageName.get(i).charAt(j)) >= '0' && pageName.get(i).charAt(j) <= '9') {
									res = pageName.get(i).charAt(j) + res;
								} else
									break;
							}
							int pageind = Integer.parseInt(res);
							if (page.size() == 0) {
							//	System.out.println("?");
								tobedeleted.add(pageind);
								// deletingFiles(pageind, pageInfoVector, t);
							} else {

								Object max = getMaxInPage(page);
								Object min = getMinInPage(page);
								((PageInfo) pageInfoVector.get(pageind)).setMax(max);
								((PageInfo) pageInfoVector.get(pageind)).setMin(min);

								serializePage(t, page, t.getTableName() + "" + pageind);
								page = null;
							}

						}
						for (int j = 0; j < tobedeleted.size(); j++) {
							deletingFiles(tobedeleted.get(j), pageInfoVector, t);
						}
					}
					return;
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
						myTuple = new Tuple(t.getClusteringKey(), htblColNameValue);
//						System.out.println("before contains");
						if (page.contains(myTuple)) {
//							System.out.println("after contains");
							// System.out.println("was here111111");
							int currTupleIndex = page.getIndexInPageUsingClusteringKey(myClusterType);
							deleteFromOctree(page.get(currTupleIndex), t, pagename);
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
//					removeFromAllPages(pageInfoVector, myTuple, 0, t, htblColNameValue);
					for (int i = 0; i < pageInfoVector.size(); i++) {
						String pagename = ((PageInfo) (pageInfoVector.get(i))).getPageName();
			//			System.out.println("My page name is" + pagename);
						Page page = deserializePage(pagename);
					//	System.out.println(myTuple);
						for (int k = 0; k < page.size(); k++) {
							// get all the columns that have indices
							Tuple currTuple = page.get(k);
							if (currTuple.equals(myTuple)) {
								deleteFromOctree(currTuple, t, pagename);
								page.remove(k);
								k--;
							}
						}
						if (page.size() == 0) {
							deletingFiles(i, pageInfoVector, t);
						} else {
							Object max = getMaxInPage(page);
							Object min = getMinInPage(page);
							((PageInfo) pageInfoVector.get(i)).setMax(max);
							((PageInfo) pageInfoVector.get(i)).setMin(min);
							serializePage(t, page, pagename);
							page = null;
						}
					}
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

	private void deleteFromOctree(Tuple myTuple, Table t, String page) {
		for (int tableIndex = 0; tableIndex < t.getIndexOnCol().size(); tableIndex++) {
			String currIndex = t.getIndexOnCol().get(tableIndex).get("Name of Index").toString();
			String col1 = "";
			String col2 = "";
			String col3 = "";
			int counter = 0;
			for (String tableKey : t.getIndexOnCol().get(tableIndex).keySet()) {
				if (tableKey.equals("Name of Index"))
					continue;
				if (counter == 0)
					col1 = tableKey;
				if (counter == 1)
					col2 = tableKey;
				if (counter == 2)
					col3 = tableKey;
				counter++;
			}
			Hashtable<String, Object> tobedeleted = new Hashtable<>();
			counter = 0;
			tobedeleted.put(col1, myTuple.getRecord().get(col1));
			tobedeleted.put(col2, myTuple.getRecord().get(col2));
			tobedeleted.put(col3, myTuple.getRecord().get(col3));
			tobedeleted.put("Page Name", page);

			Octree currOctree = deserializeOctree(currIndex);
		//	System.out.println("myHashtableeee" + tobedeleted);
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
				System.out.println("pageinfo" + pageInfoVector.size());
				t.setCurrentMaxId(t.getCurrentMaxId() - 1);
				for (int i = pageind; i < pageInfoVector.size(); i++) {
					int temp = i + 1;
					oldFile = new File("src/main/resources/Data/" + t.getTableName() + "" + temp + ".ser");
					newFile = new File("src/main/resources/Data/" + t.getTableName() + "" + i + ".ser");
					((PageInfo) pageInfoVector.get(i)).setPageName(t.getTableName() + "" + i);

					if (oldFile.renameTo(newFile)) {
						if (t.getIndex().size() > 0) {
							Page p = deserializePage(t.getTableName() + "" + i);
							for (int j = 0; j < p.size(); j++) {
								Hashtable<String, Object> tup = p.get(j).getRecord();
								updateRefrenceInIndex(t.getTableName(), tup, t.getTableName() + "" + i,
										t.getTableName() + "" + temp);

							}
						}
					} else {
						// System.out.println("Failed to rename file");
					}
				}
				serializeTable(t, t.getTableName());

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
		deleteFromOctree(page.get((page.getIndexInPage(myTuple))), t, pagename);
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
				if (x[0].toLowerCase().equals(strTableName.toLowerCase())) {
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
					if (x[3].toLowerCase().equals("true")) {
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
			if (!ClustKeyfound && !isDeletingMethod && !isUpdatingMethod && !isSelectingMethod) { // ana 3mlt deh 3lshan
																									// fel delete lw ana
																									// msh
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
						if (isDeletingMethod || isSelectingMethod) {
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
						if (isDeletingMethod || isSelectingMethod) {

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
						if (isDeletingMethod || isSelectingMethod) {
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
								System.out.println("date");
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

							if (isDeletingMethod || isSelectingMethod)
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
			System.out.println(indexName);
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

					if (x[3].toLowerCase().equals("true")) {
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
								System.out.println("Created double");
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
							if (!x[4].toLowerCase().equals("null")) {
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
					if (x[3].toLowerCase().equals("true")) {
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
				// recInIndex.put("Clust key", htblColNameValue.get(ClustKey));

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
					if (x[3].toLowerCase().equals("true")) {
						ClustKey = x[1];
					}
				}
				line = br.readLine();
			}
			br.close();
			System.out.println("hereeeeeeeeeee");
			for (int i = 0; i < indices.size(); i++) {
				String TreeName = strTableName + indices.get(i);
				Octree Octree = deserializeOctree(TreeName);
				if (!(updated.get(Octree.getX()) == null && updated.get(Octree.getY()) == null
						&& updated.get(Octree.getZ()) == null)) {
					Object val1 = oldRecord.get(Octree.getX());
					Object val2 = oldRecord.get(Octree.getY());
					Object val3 = oldRecord.get(Octree.getZ());
					Hashtable<String, Object> recInIndex = new Hashtable<>();
					recInIndex.put(Octree.getX(), val1);
					recInIndex.put(Octree.getY(), val2);
					recInIndex.put(Octree.getZ(), val3);
					recInIndex.put("Page Name", pageName);
					// recInIndex.put("Clust key", oldRecord.get(ClustKey));
					Octree.deleteTuple(recInIndex);
					val1 = newRecord.get(Octree.getX());
					val2 = newRecord.get(Octree.getY());
					val3 = newRecord.get(Octree.getZ());
					recInIndex = new Hashtable<>();
					recInIndex.put(Octree.getX(), val1);
					recInIndex.put(Octree.getY(), val2);
					recInIndex.put(Octree.getZ(), val3);
					recInIndex.put("Page Name", pageName);
					// recInIndex.put("Clust key", oldRecord.get(ClustKey));

					Octree.insertTupleInIndex(recInIndex);
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
				System.out.println(htblColNameValue.toString() + " " + pageName + " " + oldPageName);
				Octree.updateTupleReferenceInIndex(htblColNameValue, pageName, oldPageName);
				serializeIndex(Octree, TreeName);
				Octree = null;

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("CSV doesn't exist");
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

	public static Vector<String> getindexname(Table t, Vector<Hashtable<String, Object>> tmp) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			Hashtable<String, String> tableInfo = new Hashtable<String, String>();
			String TName = t.getTableName();
			Vector<String> res = new Vector<>();
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(TName)) {
					tableInfo.put(x[1], x[4]);
				}
				line = br.readLine();
			}
			br.close();
			for(int i=0;i<tmp.size();i++) {
				String value="";
				for(String key:tmp.get(i).keySet())
					value = tableInfo.get(key);
				res.add(value);
			}
			return res;
		} catch (FileNotFoundException e) {
			throw new DBAppException("File not found");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("IO Exception");
		}
	}
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		if (arrSQLTerms.length == 0)
			throw new DBAppException("Insert a valid select");
		for (int i = 0; i < arrSQLTerms.length; i++) {
		    arrSQLTerms[i]._strColumnName = arrSQLTerms[i]._strColumnName.toLowerCase();
		}
		if (!tableExits(arrSQLTerms[0]._strTableName))
			throw new DBAppException("Table doesn't exist!!");

		Vector<Tuple> result = new Vector<>();
		Table t = deserializeTable(arrSQLTerms[0]._strTableName);
		Hashtable<String, Object> htblColValue = new Hashtable<>();
		Vector<String> indexName = new Vector<>();
		Vector<Hashtable<String, Object>> tmp1 = new Vector<>();
		this.isSelectingMethod = true;
		int counter = 0;
		for (int i = 0; i < arrSQLTerms.length; i++) {
			htblColValue.put(arrSQLTerms[i]._strColumnName, arrSQLTerms[i]._objValue);
			tmp1.add(htblColValue);
			if (!isValid(t.getTableName(), htblColValue))
				throw new DBAppException("Column is invalid");
			htblColValue = new Hashtable<>();
		}

		indexName = getindexname(t, tmp1);
		Queue<String> operators = new LinkedList<String>();
		int colCounter = 0;
		int operatorCounter = 0;

		for (int indexCounter = 0; indexCounter < indexName.size(); indexCounter++) {
			Hashtable<String, Object> myHtbl = new Hashtable<String, Object>();
			myHtbl.put(arrSQLTerms[colCounter]._strColumnName, arrSQLTerms[colCounter]._objValue);
			myHtbl.put("operator" + arrSQLTerms[colCounter]._strColumnName, arrSQLTerms[colCounter]._strOperator);
			colCounter++;
			for (int j = indexCounter + 1; j < indexName.size(); j++) {
				if (myHtbl.size() == 6) {
					break;
				}
				if (!strarrOperators[operatorCounter].equals("AND")) {
					break;
				}
				if (arrSQLTerms[colCounter - 1]._strColumnName.equals(arrSQLTerms[colCounter]._strColumnName))
					break;
				if (indexName.get(indexCounter).equals(indexName.get(j)) && !indexName.get(j).equals("null")) {
					myHtbl.put(arrSQLTerms[colCounter]._strColumnName, arrSQLTerms[colCounter]._objValue);
					myHtbl.put("operator" + arrSQLTerms[colCounter]._strColumnName,
							arrSQLTerms[colCounter]._strOperator);
					indexName.remove(j);
					j--;

				} else {
					break;
				}
				operatorCounter++;
				colCounter++;
			}
//					boolean once = true;
			if (myHtbl.size() == 6) {
				myHtbl.put("indxName", indexName.get(indexCounter));
				result = compute(myHtbl, operators, t, result);
				if (operatorCounter < strarrOperators.length) {
					operators.add(strarrOperators[operatorCounter]);
					operatorCounter++;
					// once = false;
				}
			} else {
//				int outercounter = operators.size();
				Hashtable<String, Object> temp = new Hashtable<>();
				String tmp = "";
				int mySize = myHtbl.size();
				for (String key : myHtbl.keySet()) {
					if (key.length() > 7 && key.substring(0, 8).equals("operator")) {
						temp.put(key, myHtbl.get(key));
						tmp = key.substring(8);
						myHtbl.remove(key);
						break;
					}
				}
				temp.put(tmp, myHtbl.get(tmp));
				myHtbl.remove(tmp);
//				System.out.println("myOperator"+strarrOperators[outercounter]);
				result = compute(temp, operators, t, result);
				if (mySize == 4) {
					operators.add("AND");
					result = compute(myHtbl, operators, t, result);
				}

				if (operatorCounter < strarrOperators.length) {
					operators.add(strarrOperators[operatorCounter]);
					operatorCounter++;
					// once = false;
				}
//				operatorCounter = outercounter;
			}
		}
		this.isSelectingMethod = false;
		return result.iterator();

	}

	private static void ifThereisClusteringKeyIndex(Hashtable<String, Object> myHtbl, Page page, Table t,
			Vector<Tuple> result) {
		String operatorCol1 = "";
		String operatorCol2 = "";
		String col1 = "";
		String col2 = "";
		int counter = 0;
		Object clusterValue = myHtbl.get(t.getClusteringKey());
		int indexValue = page.getIndexInPageUsingClusteringKey(clusterValue);
		String clusterOperator = myHtbl.get("operator" + (t.getClusteringKey().toString())).toString();
		myHtbl.remove(clusterValue);
		myHtbl.remove("operator" + (t.getClusteringKey().toString()));
		for (String key : myHtbl.keySet()) {
			if (key.length() > 7 && key.substring(0, 8).equals("operator") && counter == 0) {
				operatorCol1 = myHtbl.get(key).toString();
				col1 = key.substring(8);
				counter++;
			}
			if (key.length() > 7 && key.substring(0, 8).equals("operator") && counter == 1) {
				operatorCol2 = myHtbl.get(key).toString();
				col2 = key.substring(8);
				counter++;
			}
		}
		boolean flag1 = false;
		boolean flag2 = false;
		myHtbl.put(t.getClusteringKey().toString(), clusterValue);
		myHtbl.put("operator" + (t.getClusteringKey().toString()), clusterOperator);
		switch (clusterOperator) {

		case "=":
			flag1 = checkOtherColumns(myHtbl.get(col1), operatorCol1, page.get(indexValue).getRecord().get(col1));
			flag2 = checkOtherColumns(myHtbl.get(col2), operatorCol2, page.get(indexValue).getRecord().get(col2));
			if (flag1 && flag2)
				result.add(page.get(indexValue));
			break;
		case "!=":

			for (Tuple myTuple : page) {
				if (myTuple.getClusteringkey().equals(clusterValue.toString()))
					continue;
				flag1 = checkOtherColumns(myHtbl.get(col1), operatorCol1, page.get(indexValue).getRecord().get(col1));
				flag2 = checkOtherColumns(myHtbl.get(col2), operatorCol2, page.get(indexValue).getRecord().get(col2));
				if (flag1 && flag2)
					result.add(myTuple);
			}
			break;
		case ">":
			for (int j = indexValue + 1; j < page.size(); j++) {
				flag1 = checkOtherColumns(myHtbl.get(col1), operatorCol1, page.get(indexValue).getRecord().get(col1));
				flag2 = checkOtherColumns(myHtbl.get(col2), operatorCol2, page.get(indexValue).getRecord().get(col2));
				if (flag1 && flag2)
					result.add(page.get(j));
			}
			break;
		case ">=":
			for (int j = indexValue; j < page.size(); j++) {
				flag1 = checkOtherColumns(myHtbl.get(col1), operatorCol1, page.get(indexValue).getRecord().get(col1));
				flag2 = checkOtherColumns(myHtbl.get(col2), operatorCol2, page.get(indexValue).getRecord().get(col2));
				if (flag1 && flag2)
					result.add(page.get(j));
			}
			break;
		case "<":
			for (int j = 0; j < indexValue || j < page.size(); j++) {
				flag1 = checkOtherColumns(myHtbl.get(col1), operatorCol1, page.get(indexValue).getRecord().get(col1));
				flag2 = checkOtherColumns(myHtbl.get(col2), operatorCol2, page.get(indexValue).getRecord().get(col2));
				if (flag1 && flag2)
					result.add(page.get(j));
			}
			break;
		case "<=":
			for (int j = 0; j <= indexValue && j < page.size(); j++) {
				flag1 = checkOtherColumns(myHtbl.get(col1), operatorCol1, page.get(indexValue).getRecord().get(col1));
				flag2 = checkOtherColumns(myHtbl.get(col2), operatorCol2, page.get(indexValue).getRecord().get(col2));
				if (flag1 && flag2)
					result.add(page.get(j));
			}
			break;
		}
	}

	private static boolean checkOtherColumns(Object object, String operatorCol1, Object object2) {
		switch (operatorCol1) {
		case "=":
			if (compareTo1(object, object2) == 0)
				return true;
			break;
		case "!=":
			if (compareTo1(object, object2) != 0)
				return true;
			break;
		case ">":
			if (compareTo1(object, object2) > 0)
				return true;
			break;
		case ">=":
			if (compareTo1(object, object2) >= 0)
				return true;
			break;
		case "<":
			if (compareTo1(object, object2) < 0)
				return true;
			break;
		case "<=":
			if (compareTo1(object, object2) <= 0)
				return true;
			break;
		}
		return false;
	}

	private static void ifThereIsNoClusteringKey(Hashtable<String, Object> myHtbl, Page page, Vector<Tuple> result) {

		String operatorCol1 = "";
		String operatorCol2 = "";
		String operatorCol3 = "";
		String col1 = "";
		String col2 = "";
		String col3 = "";
		int counter=0;
		for (String key : myHtbl.keySet()) {
		    if (key.startsWith("operator")) {
		        String operator = myHtbl.get(key).toString();
		        String col = key.substring(8);
		        switch (counter) {
		            case 0:
		                operatorCol1 = operator;
		                col1 = col;
		                break;
		            case 1:
		                operatorCol2 = operator;
		                col2 = col;
		                break;
		            case 2:
		                operatorCol3 = operator;
		                col3 = col;
		                break;
		            default:
		                break;
		        }
		        counter++;
		    }
		}
		for (Tuple myTuple : page) {
			
			Hashtable<String, Object> record = myTuple.getRecord();
			System.out.println("MY RECORD"+record);
			boolean flag1 = true;
			boolean flag2 = true;
			boolean flag3 = true;
			switch (operatorCol1) {
			case "=":
				flag1 = record.get(col1).equals(myHtbl.get(col1));
//				System.out.println(record.get(col1)+" "+myHtbl.get(col1));
				break;
			case ">":
				if (compareTo1(record.get(col1), (myHtbl.get(col1))) > 0)
					flag1 = true;
				else
					flag1 = false;
				break;
			case ">=":
				if (compareTo1(record.get(col1), (myHtbl.get(col1))) >= 0)
					flag1 = true;
				else
					flag1 = false;
				break;
			case "<":
				if (compareTo1(record.get(col1), (myHtbl.get(col1))) < 0)
					flag1 = true;
				else
					flag1 = false;
				break;
			case "<=":
				if (compareTo1(record.get(col1), (myHtbl.get(col1))) <= 0)
					flag1 = true;
				else
					flag1 = false;
				break;
			case "!=":
				flag1 = !record.get(col1).equals(myHtbl.get(col1));
				break;
			}
			System.out.println("MY FLAG"+flag1);
			switch (operatorCol3) {
			case "=":
				flag3 = record.get(col3).equals(myHtbl.get(col3));
//				System.out.println(record.get(col3)+" "+myHtbl.get(col3));
				break;
			case ">":
				if (compareTo1(record.get(col3), (myHtbl.get(col3))) > 0)
					flag3 = true;
				else
					flag3 = false;
				break;
			case ">=":
				if (compareTo1(record.get(col3), (myHtbl.get(col3))) >= 0)
					flag3 = true;
				else
					flag3 = false;
				break;
			case "<":
				if (compareTo1(record.get(col3), (myHtbl.get(col3))) < 0)
					flag3 = true;
				else
					flag3 = false;
				break;
			case "<=":
				if (compareTo1(record.get(col3), (myHtbl.get(col3))) <= 0)
					flag3 = true;
				else
					flag3 = false;
				break;
			case "!=":
				flag3 = !record.get(col3).equals(myHtbl.get(col3));
				break;
			}
			switch (operatorCol2) {
			case "=":
				flag2 = record.get(col2).equals(myHtbl.get(col2));
//				System.out.println(record.get(col2)+" "+myHtbl.get(col2));
				break;
			case ">":
				if (compareTo1(record.get(col2), (myHtbl.get(col2))) > 0)
					flag2 = true;
				else
					flag2 = false;
				break;
			case ">=":
				if (compareTo1(record.get(col2), (myHtbl.get(col2))) >= 0)
					flag2 = true;
				else
					flag2 = false;
				break;
			case "<":
				if (compareTo1(record.get(col2), (myHtbl.get(col2))) < 0)
					flag2 = true;
				else
					flag2 = false;
				break;
			case "<=":
				if (compareTo1(record.get(col2), (myHtbl.get(col2))) <= 0)
					flag2 = true;
				else
					flag2 = false;
				break;
			case "!=":
				flag2 = !record.get(col2).equals(myHtbl.get(col2));
				break;
			}
			if (flag1 && flag2 && flag3)
				result.add(myTuple);
		}
	}

	private static void notAnIndex(Hashtable<String, Object> myHtbl, Table t, Vector<Tuple> result) {

		String operatorCol1 = "";
		String col1 = "";
		for (String key : myHtbl.keySet()) {
			if (key.length() > 8 && key.substring(0, 8).equals("operator")) {
				operatorCol1 = myHtbl.get(key).toString();
				col1 = key.substring(8);
			}
		}
		for (int i = 0; i < t.getPageInfo().size(); i++) {
			Page page = deserializePage(t.getPageInfo().get(i).getPageName());
			for (Tuple myTuple : page) {
				Hashtable<String, Object> record = myTuple.getRecord();
				boolean flag = false;
				switch (operatorCol1) {
				case "=":
					flag = record.get(col1).equals(myHtbl.get(col1));
					break;
				case ">":
					if (compareTo1(record.get(col1), (myHtbl.get(col1))) > 0)
						flag = true;
					else
						flag = false;
					break;
				case ">=":
					if (compareTo1(record.get(col1), (myHtbl.get(col1))) >= 0)
						flag = true;
					else
						flag = false;
					break;
				case "<":
					if (compareTo1(record.get(col1), (myHtbl.get(col1))) < 0)
						flag = true;
					else
						flag = false;
					break;
				case "<=":
					if (compareTo1(record.get(col1), (myHtbl.get(col1))) <= 0)
						flag = true;
					else
						flag = false;
					break;
				case "!=":
					flag = !record.get(col1).equals(myHtbl.get(col1));
					break;
				}
				if (flag) {
					result.add(myTuple);
				}
			}
		}
	}

	public static Vector<Tuple> compute(Hashtable<String, Object> myHtbl, Queue<String> operators, Table t,
			Vector<Tuple> result) {
		if (operators.isEmpty()) {
			if (myHtbl.size() == 7) {
				String myIndexName = t.getTableName() + "" + myHtbl.get("indxName").toString();
				Octree myOct = deserializeOctree(myIndexName);
				Vector<String> myPages = new Vector<>();
				myPages = myOct.getAllPages(myHtbl);

				for (int i = 0; i < myPages.size(); i++) {
					Page page = deserializePage(myPages.get(i));
					if (myHtbl.containsKey(t.getClusteringKey()))
						ifThereisClusteringKeyIndex(myHtbl, page, t, result);
					else
						ifThereIsNoClusteringKey(myHtbl, page, result);
				}
			} else {
				notAnIndex(myHtbl, t, result);
			}
		} else {
//			int counterDummy = 0;
//			System.out.println("COMPUTE"+operators);
//			while (operators.peek()!=null && operators.peek().equals("A")) {
//				counterDummy++;
//				operators.poll();
//			}
			Vector<Tuple> currResult = new Vector<>();
			boolean flag = false;
			if (myHtbl.size() == 7) {
				flag = true;
				String myIndexName = myHtbl.get("indxName").toString();
				Octree myOct = deserializeOctree(t.getTableName()+""+myIndexName);
				Vector<String> myPages = new Vector<>();
				myPages = myOct.getAllPages(myHtbl);
				for (int i = 0; i < myPages.size(); i++) {
					Page page = deserializePage(myPages.get(i));
					if (myHtbl.containsKey(t.getClusteringKey()))
						ifThereisClusteringKeyIndex(myHtbl, page, t, currResult);
					else
						ifThereIsNoClusteringKey(myHtbl, page, currResult);
				}
			}

			while (!operators.isEmpty()) {
				String myOpertator = operators.poll();
				switch (myOpertator) {
				case "AND":
					if (flag) {
						result.retainAll(currResult);
					} else {
						for (int i = 0; i < result.size(); i++) {
							String operatorKey = "";
							for (String key : myHtbl.keySet()) {
								if (key.length() > 7 && key.substring(0, 8).equals("operator")) {
									operatorKey = key;
									break;
								}
							}
							String operandKey = operatorKey.substring(8);
							switch (myHtbl.get(operatorKey).toString()) {
							case "=":
								if (!result.get(i).getRecord().get(operandKey).equals(myHtbl.get(operandKey))) {
									result.remove(i);
									i--;
								}
								break;
							case ">":
								if (compareTo1(result.get(i).getRecord().get(operandKey),
										(myHtbl.get(operandKey))) <= 0) {
									result.remove(i);
									i--;
								}
								break;
							case ">=":
								if (compareTo1(result.get(i).getRecord().get(operandKey),
										(myHtbl.get(operandKey))) < 0) {
									result.remove(i);
									i--;
								}
								break;
							case "<":
								if (compareTo1(result.get(i).getRecord().get(operandKey),
										(myHtbl.get(operandKey))) >= 0) {
									result.remove(i);
									i--;
								}
								break;
							case "<=":
								if (compareTo1(result.get(i).getRecord().get(operandKey),
										(myHtbl.get(operandKey))) > 0) {
									result.remove(i);
									i--;
								}
								break;
							case "!=":
								if (compareTo1(result.get(i).getRecord().get(operandKey),
										(myHtbl.get(operandKey))) == 0) {
									result.remove(i);
									i--;
								}
							}
						}
					}
					break;

				case "OR":
					if (flag) {
						int originalSize = currResult.size();
						for (int i = 0; i < originalSize; i++) {
							if (!result.contains(currResult.get(i))) {
								result.add(currResult.get(i));
							}
						}
					} 
					else {
						for (int j = 0; j < t.getPageInfo().size(); j++) {
							Page page = deserializePage(t.getPageInfo().get(j).getPageName());
							if(myHtbl.containsKey(t.getClusteringKey()) && compareTo1(t.getPageInfo().get(j).getMax(), myHtbl.get(t.getClusteringKey()))>=0
									&& compareTo1(t.getPageInfo().get(j).getMin(),myHtbl.get(t.getClusteringKey()))<=0) {
								
							
								int index = page.getIndexInPageUsingClusteringKey(myHtbl.get(t.getClusteringKey()));
								String operatorKey = "";
								for (String key : myHtbl.keySet()) {
									if (key.length() > 7 && key.substring(0, 8).equals("operator")) {
										operatorKey = key;
										break;
									}
								}
								String operatorValue = myHtbl.get(operatorKey).toString();
								myHtbl.remove(operatorKey);
								switch (operatorValue) {
								case "=":
									if (page.get(index).getRecord().get(t.getClusteringKey()).equals(
											myHtbl.get(t.getClusteringKey())) && !result.contains(page.get(index)))
										result.add(page.get(index));
									break;
								case ">":
									for (int i = index + 1; i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) > 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
									}
									break;
								case ">=":
									for (int i = index; i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) >= 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
									}
									break;
								case "<":
									for (int i = 0; i < index && i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) < 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
									}
									break;
								case "<=":
									for (int i = 0; i <= index && i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) <= 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
									}
									break;
								case "!=":
									for (int i = 0; i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) != 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
									}
									break;
								}
								myHtbl.put(operatorKey, operatorValue);
							} else {
								for (Tuple myTuple : page) {
									String operatorKey = "";
									for (String key : myHtbl.keySet()) {
										if (key.length() > 7 && key.substring(0, 8).equals("operator")) {
											operatorKey = key;
											break;
										}
									}
									String operandKey = operatorKey.substring(8);
									switch (myHtbl.get(operatorKey).toString()) {
									case "=":
										if (myTuple.getRecord().get(operandKey).equals(myHtbl.get(operandKey))
												&& !result.contains(myTuple)) {
											result.add(myTuple);
										}
										break;
									case ">":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) > 0 && !result.contains(myTuple)) {
											result.add(myTuple);
										}
										break;
									case ">=":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) >= 0 && !result.contains(myTuple)) {
											result.add(myTuple);
										}
										break;
									case "<":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) < 0 && !result.contains(myTuple)) {
											result.add(myTuple);
										}
										break;
									case "<=":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) <= 0 && !result.contains(myTuple)) {
											result.add(myTuple);
										}
										break;
									case "!=":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) != 0 && !result.contains(myTuple)) {
											result.add(myTuple);
										}
									}
								}
							}
						}
					}
					break;
				case "XOR":
					if (flag) {
						int orginalSize = currResult.size();
						for (int i = 0; i < orginalSize; i++) {
							if (!result.contains(currResult.get(i)))
								result.add(currResult.get(i));
							else {
								result.remove(currResult.get(i));
							}
						}
					} else {
						for (int j = 0; j < t.getPageInfo().size(); j++) {
							Page page = deserializePage(t.getPageInfo().get(j).getPageName());
							if(myHtbl.containsKey(t.getClusteringKey()) && compareTo1(t.getPageInfo().get(j).getMax(), myHtbl.get(t.getClusteringKey()))>=0
									&& compareTo1(t.getPageInfo().get(j).getMin(),myHtbl.get(t.getClusteringKey()))<=0) {
								
							
								int index = page.getIndexInPageUsingClusteringKey(myHtbl.get(t.getClusteringKey()));
								String operatorKey = "";
								for (String key : myHtbl.keySet()) {
									if (key.length() > 7 && key.substring(0, 8).equals("operator")) {
										operatorKey = key;
										break;
									}
								}
								String operatorValue = myHtbl.get(operatorKey).toString();
								myHtbl.remove(operatorKey);
								switch (operatorValue) {
								case "=":
									if (page.get(index).getRecord().get(t.getClusteringKey()).equals(
											myHtbl.get(t.getClusteringKey())) && !result.contains(page.get(index)))
										result.add(page.get(index));
									else {
										if(page.get(index).getRecord().get(t.getClusteringKey()).equals(
												myHtbl.get(t.getClusteringKey())) && !result.contains(page.get(index)))
											result.remove(page.get(index));
									}
									break;
								case ">":
									for (int i = index + 1; i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) > 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
										else {
											if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) > 0 && result.contains(page.get(i)))
											result.remove(page.get(i));
										}
									}
									break;
								case ">=":
									for (int i = index; i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) >= 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
										else {
											if(compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
													myHtbl.get(t.getClusteringKey())) >= 0 && result.contains(page.get(i)))
												result.remove(page.get(i));
										}
									}
									break;
								case "<":
									for (int i = 0; i < index && i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) < 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
										else {
											if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
													myHtbl.get(t.getClusteringKey())) < 0 && result.contains(page.get(i)))
												result.remove(page.get(i));
										}
									}
									break;
								case "<=":
									for (int i = 0; i <= index && i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) <= 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
										else {
											if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
													myHtbl.get(t.getClusteringKey())) <= 0 && result.contains(page.get(i)))
												result.remove(page.get(i));
										}
									}
									break;
								case "!=":
									for (int i = 0; i < page.size(); i++) {
										if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
												myHtbl.get(t.getClusteringKey())) != 0 && !result.contains(page.get(i)))
											result.add(page.get(i));
										else {
											if (compareTo1(page.get(i).getRecord().get(t.getClusteringKey()),
													myHtbl.get(t.getClusteringKey())) != 0 && result.contains(page.get(i)))
												result.remove(page.get(i));
										}
									}
									break;
								}
								myHtbl.put(operatorKey, operatorValue);
							} else {
								for (Tuple myTuple : page) {
									String operatorKey = "";
									for (String key : myHtbl.keySet()) {
										if (key.length() > 7 && key.substring(0, 8).equals("operator")) {
											operatorKey = key;
											break;
										}
									}
									String operandKey = operatorKey.substring(8);
									switch (myHtbl.get(operatorKey).toString()) {
									case "=":
										if (myTuple.getRecord().get(operandKey).equals(myHtbl.get(operandKey))
												&& !result.contains(myTuple)) 
											result.add(myTuple);
										else {
											if(myTuple.getRecord().get(operandKey).equals(myHtbl.get(operandKey))
												&& result.contains(myTuple))
												result.remove(myTuple);
										}
										break;
									case ">":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) > 0 && !result.contains(myTuple)) 
											result.add(myTuple);
										else {
											if (compareTo1(myTuple.getRecord().get(operandKey),
													(myHtbl.get(operandKey))) > 0 && result.contains(myTuple))
												result.remove(myTuple);
										}
										break;
									case ">=":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) >= 0 && !result.contains(myTuple))
											result.add(myTuple);
										else {
											if (compareTo1(myTuple.getRecord().get(operandKey),
													(myHtbl.get(operandKey))) >= 0 && result.contains(myTuple))
												result.remove(myTuple);
										}
										break;
									case "<":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) < 0 && !result.contains(myTuple))
											result.add(myTuple);
										else {
											if (compareTo1(myTuple.getRecord().get(operandKey),
													(myHtbl.get(operandKey))) < 0 && result.contains(myTuple))
												result.remove(myTuple);
										}
										break;
									case "<=":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) <= 0 && !result.contains(myTuple))
											result.add(myTuple);
										else {
											if (compareTo1(myTuple.getRecord().get(operandKey),
													(myHtbl.get(operandKey))) <= 0 && result.contains(myTuple))
												result.remove(myTuple);
										}
										break;
									case "!=":
										if (compareTo1(myTuple.getRecord().get(operandKey),
												(myHtbl.get(operandKey))) != 0 && !result.contains(myTuple))
											result.add(myTuple);
										else {
											if (compareTo1(myTuple.getRecord().get(operandKey),
													(myHtbl.get(operandKey))) != 0 && result.contains(myTuple))
												result.remove(myTuple);
										}
									}
								}
							}
						}
					}
					break;
				}
			}
//			if (counterDummy != 0) {
//				for (int i = 0; i < counterDummy; i++) {
//					operators.add("A");
//				}
//			}
		}
		return result;

	}

	private static int compareTo1(Object x, Object y) {
		int result = 0;
		if (x instanceof Integer && y instanceof Integer) {
			if (Integer.parseInt(x.toString()) > Integer.parseInt(y.toString()))
				result = 1;
			if (Integer.parseInt(x.toString()) == Integer.parseInt(y.toString()))
				result = 0;
			if (Integer.parseInt(x.toString()) < Integer.parseInt(y.toString()))
				result = -1;
		}
		if (x instanceof String && y instanceof String) {
			if (x.toString().compareTo(y.toString()) > 0)
				result = 1;
			if (x.toString().compareTo(y.toString()) == 0)
				result = 0;
			if (x.toString().compareTo(y.toString()) < 0)
				result = -1;
		}
		if (x instanceof Double && y instanceof Double) {
			if (Double.parseDouble(x.toString()) > Double.parseDouble(y.toString()))
				result = 1;
			if (x.toString().compareTo(y.toString()) == 0)
				result = 0;
			if (x.toString().compareTo(y.toString()) < 0)
				result = -1;
		}
		if (x instanceof java.util.Date && y instanceof java.util.Date) {
				Date date = (Date)x;
				Date date1 = (Date)y;
				if (date.after(date1))
					result = 1;
				if (date.compareTo(date1) == 0)
					result = 0;
				if (date.before(date1))
					result = -1;
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
				if (x[0].equals(TName) && x[3].toLowerCase().equals("true")) {
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

	public static int getPageNumFromIndex(String t, Hashtable<String, Object> htbl, Object clustVal)
			throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = br.readLine();
			String Indices = null;
			String col = "";
			System.out.println(t);
			// String TName = t.getTableName();
			// Vector<String> res = new Vector<>();
			while (line != null) {
				String[] x = line.split(",");
				System.out.println(Arrays.toString(x));
				if (x[0].equals(t)) {
					if (x[3].toLowerCase().equals("true") && !x[4].equals("null")) {
						System.out.println("here");
						Indices = x[4];
						col = x[1];

					}
				}
				line = br.readLine();
			}
			br.close();

			if (Indices == null)
				return -1;
			System.out.println(Indices.toString());

			String treeName = t + Indices;
			Octree tree = deserializeOctree(treeName);
			String page = tree.getExactPage(col, tree.getRoot(), clustVal);
			System.out.println("PAGE " + page);
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

	public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
		// String sqlStr="";
		// Object statement="";
		String sqlStr = strbufSQL.toString();
//		String sqlStr = "CREATE TABLE Test (X varchar(20),"
//				+ "Y int primary key,"
//				+ "Z double,"
//				+ "A int,"
//				+ "B int,"
//				+ "C int);";
//		sqlStr = "Insert into Test (x,y,z,a,b,c) values "
//				+ "('name1',59,1.7,7,29,84);";
//		sqlStr = "UPDATE Test\r\n" + "SET a = 4 , b=28 " + "WHERE y=59 ;";
		// sqlStr = "CREATE INDEX index_name\r\n" + "ON Test (a,b,c);";
//		sqlStr = "DELETE FROM Test where y=6";
//		sqlStr = "SELECT * FROM Customers where col1=2 and x=9;\r\n" + "";
		net.sf.jsqlparser.statement.Statement statement;
		try {
			statement = CCJSqlParserUtil.parse(sqlStr);
		} catch (Exception e) {
			throw new DBAppException("Cant parse this statement");
		}
		// System.out.println(statement.getClass());
		DBApp dbApp = new DBApp();
		dbApp.init();
		if (statement instanceof CreateTable) {
			CreateTable createTable = (CreateTable) statement;
			if (createTable.getTable() == null)
				throw new DBAppException("Have to Include Table");
			Hashtable htblColNameType = new Hashtable();
			Hashtable htblColNameMin = new Hashtable();
			Hashtable htblColNameMax = new Hashtable<>();

			String tableName = createTable.getTable().getName();
			String clustKey = "";
			System.out.println(createTable.getColumns());
			for (int i = 0; i < createTable.getColumnDefinitions().size(); i++) {
				ColumnDefinition colDef = createTable.getColumnDefinitions().get(i);
				System.out.println(colDef.getColumnName() + "       " + colDef.getColDataType() + "    "
						+ colDef.getColumnSpecs());
				System.out.println(colDef.getColDataType().toString().toLowerCase().split(" ")[0]);
				switch (colDef.getColDataType().toString().toLowerCase().split(" ")[0]) {
				case "int":
					htblColNameType.put(colDef.getColumnName(), "java.lang.Integer");
					htblColNameMin.put(colDef.getColumnName(), Integer.MIN_VALUE + "");
					htblColNameMax.put(colDef.getColumnName(), Integer.MAX_VALUE + "");

					break;
				case "double":
				case "float":
					htblColNameType.put(colDef.getColumnName(), "java.lang.Double");
					htblColNameMin.put(colDef.getColumnName(), Double.MIN_VALUE + "");
					htblColNameMax.put(colDef.getColumnName(), Double.MAX_VALUE + "");
					break;
				case "varchar":
					String[] dataType = colDef.getColDataType().toString().toLowerCase().replace("(", "")
							.replace(")", "").replace(",", "").split(" ");
					// System.out.println(Arrays.toString(dataType));
					htblColNameType.put(colDef.getColumnName(), "java.lang.String");
					if (dataType.length > 1) {
						int c = Integer.parseInt(dataType[1]);
						String newString = "";
						while (c > 0) {
							newString += "z";
							c--;
						}
						htblColNameMin.put(colDef.getColumnName(), "a");
						htblColNameMax.put(colDef.getColumnName(), newString);

					}
					break;
				case "date":
					htblColNameType.put(colDef.getColumnName(), "java.util.Date");

					htblColNameMin.put(colDef.getColumnName(), "0001-01-01");
					htblColNameMax.put(colDef.getColumnName(), "9999-12-31");
					break;
				}
				// System.out.println(colDef.getColumnSpecs());

				if (colDef.getColumnSpecs() != null
						&& (colDef.getColumnSpecs().contains("PRIMARY") | colDef.getColumnSpecs().contains("Primary")
								|| colDef.getColumnSpecs().contains("primary"))) {
					System.out.println(colDef.getColumnSpecs());
					if (clustKey.equals(""))
						clustKey = colDef.getColumnName();
					else
						throw new DBAppException("cant have 2 primary keys");
				}

			}

			createTable(tableName, clustKey, htblColNameType, htblColNameMin, htblColNameMax);
			return null;
		}
		if (statement instanceof Insert) {
			Insert insert = (Insert) statement;
			if (insert.getTable() == null)
				throw new DBAppException("Have to Include Table");
			// Table table = (Table) insert.getTable();
			if (insert.getColumns() == null)
				throw new DBAppException("please include the column names in your statement");
			java.util.Hashtable<String, Object> h = new Hashtable<>();
			System.out.println(insert.getColumns());
			System.out.println(insert.getItemsList());
			ExpressionList expList = (ExpressionList) insert.getItemsList();
			for (int i = 0; i < expList.getExpressions().size(); i++) {
			//	System.out.println(expList.getExpressions().get(i).getClass());
				if (expList.getExpressions().get(i) instanceof StringValue) {
					StringValue val = (StringValue) expList.getExpressions().get(i);
					String s = val.getValue();
					try {
						java.util.Date d = new SimpleDateFormat("yyyy-MM-dd").parse(val.getValue());
						h.put(insert.getColumns().get(i).toString(), d);

					} catch (ParseException e) {
						h.put(insert.getColumns().get(i).toString(), s);

					}
				} else {
					if (expList.getExpressions().get(i) instanceof LongValue) {
						LongValue val = (LongValue) expList.getExpressions().get(i);
						int value = Integer.parseInt(val.getValue() + "");
						h.put(insert.getColumns().get(i).toString(), value);

					} else {
						if (expList.getExpressions().get(i) instanceof DoubleValue) {
							DoubleValue val = (DoubleValue) expList.getExpressions().get(i);
							Double value = (Double.parseDouble(val.getValue() + ""));
					//		System.out.println(value.getClass() + " " + value);
							h.put(insert.getColumns().get(i).toString(), value);

						} else {
							if (expList.getExpressions().get(i) instanceof SignedExpression) {

								SignedExpression val = (SignedExpression) expList.getExpressions().get(i);
								if (val.getExpression().toString().contains(".")) {
									Double value = Double.parseDouble(val.getSign() + "" + val.getExpression());
									h.put(insert.getColumns().get(i).toString(), value);

								} else {
									Integer value = Integer.parseInt(val.getSign() + "" + val.getExpression());
									h.put(insert.getColumns().get(i).toString(), value);

								}
							} else {
								throw new DBAppException("Invalid values");

							}
						}
					}
				}
				// String value = "2001-10-14";
				// System.out.println( new SimpleDateFormat("yyyy-MM-dd").parse(value));
			}
			// System.out.println(h.get("Id"));
			// dbApp.getPages(table.toString());
			insertIntoTable(insert.getTable().toString(), h);
			return null;

		}
		if (statement instanceof Update) {
			System.out.println(statement.toString());
			Update update = (Update) statement;
			if (update.getTable() == null)
				throw new DBAppException("Have to Include Table");

			if (update.getWhere() == null)
				throw new DBAppException("Have to Use Clustering Key Column  to Update");
			if (!(update.getWhere() instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo))
				throw new DBAppException("Have to Use ONLY Clustering Key Column  to Update");
			String table = update.getTable().toString();
			String clustKey = ((net.sf.jsqlparser.expression.operators.relational.EqualsTo) update.getWhere())
					.getLeftExpression().toString();
			checkIfClustKey(clustKey, table);
			String valueOfClustKey = ((net.sf.jsqlparser.expression.operators.relational.EqualsTo) update.getWhere())
					.getRightExpression().toString();

//if(update.getWhere().)
//	            b.append(" WHERE ");
//	            b.append(where);
			// System.out.println(update.getWhere().getClass());

			Hashtable<String, Object> htblColNameValue = new Hashtable<>();
			// System.out.println(update.getUpdateSets());
			for (int i = 0; i < update.getUpdateSets().size(); i++) {
				UpdateSet u = update.getUpdateSets().get(i);
				// htblColNameValue.put(u.getColumns().get(0).toString(),u.getExpressions().get(0));
				// System.out.println(u.getColumns()+" "+u.getExpressions());
				if (u.getExpressions().get(0) instanceof StringValue) {
					StringValue val = (StringValue) u.getExpressions().get(0);
					String s = val.getValue();
					try {
						java.util.Date d = new SimpleDateFormat("yyyy-MM-dd").parse(val.getValue());
						htblColNameValue.put(u.getColumns().get(0).toString().toString(), d);

					} catch (ParseException e) {
						htblColNameValue.put(u.getColumns().get(0).toString(), s);

					}
				} else {
					if (u.getExpressions().get(0) instanceof LongValue) {
						LongValue val = (LongValue) u.getExpressions().get(0);
						int value = Integer.parseInt(val.getValue() + "");
						htblColNameValue.put(u.getColumns().get(0).toString(), value);

					} else {
						if (u.getExpressions().get(0) instanceof DoubleValue) {
							DoubleValue val = (DoubleValue) u.getExpressions().get(0);
							Double value = (Double.parseDouble(val.getValue() + ""));
							htblColNameValue.put(u.getColumns().get(0).toString(), value);

						} else {
							if (u.getExpressions().get(0) instanceof SignedExpression) {

								SignedExpression val = (SignedExpression) u.getExpressions().get(0);
								if (val.getExpression().toString().contains(".")) {
									Double value = Double.parseDouble(val.getSign() + "" + val.getExpression());
									htblColNameValue.put(u.getColumns().get(0).toString(), value);

								} else {
									Integer value = Integer.parseInt(val.getSign() + "" + val.getExpression());
									htblColNameValue.put(u.getColumns().get(0).toString(), value);

								}
							} else {
								throw new DBAppException("Invalid Value");

							}
						}
					}

				}
			}
			updateTable(table, valueOfClustKey, htblColNameValue);
			return null;

		}
		if (statement instanceof CreateIndex) {
			CreateIndex createInd = (CreateIndex) statement;
			if (createInd.getTable() == null)
				throw new DBAppException("Have to Include Table");
			// System.out.println(createInd.getIndex().getColumns().toArray());
			String[] strCol = new String[createInd.getIndex().getColumns().size()];
			for (int i = 0; i < createInd.getIndex().getColumns().size(); i++) {
				strCol[i] = createInd.getIndex().getColumns().get(i).toString().toLowerCase();
			}
			createIndex(createInd.getTable().toString(), strCol);
			return null;

		}
		if (statement instanceof Delete) {
			Delete delete = (Delete) statement;
//			System.out.println(delete);
//			System.out.println(delete.getJoins());
//			System.out.println(delete.getLimit());
//			System.out.println(delete.getModifierPriority());
//			System.out.println(delete.getOracleHint());
//			System.out.println(delete.getOrderByElements());
//			System.out.println(delete.getOutputClause());
//			System.out.println(delete.getReturningExpressionList());
//			System.out.println(delete.getTables());
//			System.out.println(delete.getUsingList());
			// System.out.println(delete.getWhere().getClass());
			//System.out.println(delete.getWithItemsList());
			if (delete.getWhere() == null) {
				Hashtable<String, Object> htblColNameValue = new Hashtable<>();
				dbApp.deleteFromTable(delete.getTable().getName(), htblColNameValue);
				return null;

			}
			if (!(delete.getWhere() instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo) && !(delete
					.getWhere() instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression)) {
				throw new DBAppException("Can only delete ANDED columns and only using Equalsto");

			}
			Expression exp = delete.getWhere();

			Queue<Expression> queue = new LinkedList<>();
			queue.add(delete.getWhere());
			Hashtable<String, Object> htblColNameValue = new Hashtable<>();
			while (!queue.isEmpty()) {
				exp = queue.poll();
				if (exp instanceof EqualsTo) {
					Expression right = ((EqualsTo) exp).getRightExpression();
					Expression left = ((EqualsTo) exp).getLeftExpression();

					if (right instanceof StringValue) {
						StringValue val = (StringValue) right;
						String s = val.getValue();
						try {
							java.util.Date d = new SimpleDateFormat("yyyy-MM-dd").parse(val.getValue());
							htblColNameValue.put(left.toString(), d);

						} catch (ParseException e) {
							htblColNameValue.put(left.toString(), s);

						}
					} else {
						if (right instanceof LongValue) {
							LongValue val = (LongValue) right;
							int value = Integer.parseInt(val.getValue() + "");
							htblColNameValue.put(left.toString(), value);

						} else {
							if (right instanceof DoubleValue) {
								DoubleValue val = (DoubleValue) right;
								Double value = (Double.parseDouble(val.getValue() + ""));
								htblColNameValue.put(left.toString(), value);

							} else {
								if (right instanceof SignedExpression) {

									SignedExpression val = (SignedExpression) right;
									if (val.getExpression().toString().contains(".")) {
										Double value = Double.parseDouble(val.getSign() + "" + val.getExpression());
										htblColNameValue.put(left.toString(), value);

									} else {
										Integer value = Integer.parseInt(val.getSign() + "" + val.getExpression());
										htblColNameValue.put(left.toString(), value);

									}
								} else {
									throw new DBAppException("Invalid Value");

								}
							}
						}

					}

				} else {
					if (!(exp instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo)
							&& !(exp instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression)) {
						throw new DBAppException("Can only delete ANDED columns and only using Equalsto");

					}
					if (exp instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo) {
						Expression right = ((EqualsTo) exp).getRightExpression();
						Expression left = ((EqualsTo) exp).getLeftExpression();
						queue.add(left);
						queue.add(right);
					}
					if (exp instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression) {
						Expression right = ((AndExpression) exp).getRightExpression();
						Expression left = ((AndExpression) exp).getLeftExpression();
						queue.add(left);
						queue.add(right);
					}

				}

			}
			//System.out.println(htblColNameValue);
			deleteFromTable(delete.getTable().getName(), htblColNameValue);
			return null;

		}
		if (statement instanceof Select) {
			Select select = (Select) statement;
			if (!(select.getSelectBody() instanceof net.sf.jsqlparser.statement.select.PlainSelect))
				throw new DBAppException("Can't pasre this select statement");
			if (select.getWithItemsList() != null)
				throw new DBAppException("Can't pasre this select statement");
		//	System.out.println(select.getSelectBody().getClass());
		//	System.out.println(select.getWithItemsList());
			PlainSelect plainselect = (PlainSelect) select.getSelectBody();
//			System.out.println(plainselect.getSelectItems().get(0));

			if (plainselect.getSelectItems() == null || plainselect.getSelectItems().size() > 1
					|| !plainselect.getSelectItems().get(0).toString().equals("*"))
				throw new DBAppException("Can only select all coloumns");
			// if(plainselect.getFromItem())
			if (plainselect.getFirst() != null || plainselect.getForXmlPath() != null
					|| plainselect.getDistinct() != null || plainselect.getFetch() != null
					|| plainselect.getJoins() != null || plainselect.getGroupBy() != null
					|| plainselect.getHaving() != null || plainselect.getIntoTables() != null
					|| plainselect.getKsqlWindow() != null || plainselect.getLimit() != null
					|| plainselect.getMySqlSqlCacheFlag() != null || plainselect.getOffset() != null
					|| plainselect.getOptimizeFor() != null || plainselect.getOracleHierarchical() != null
					|| plainselect.getOracleHint() != null || plainselect.getOrderByElements() != null
					|| plainselect.getSkip() != null || plainselect.getTop() != null || plainselect.getWait() != null
					|| plainselect.getWindowDefinitions() != null || plainselect.getWithIsolation() != null)
				throw new DBAppException("Can not parse this statement");
			if (plainselect.getMySqlHintStraightJoin() != false || plainselect.getMySqlSqlCalcFoundRows() != false)
				throw new DBAppException("Can not parse this statement");

	//		System.out.println(plainselect.getWhere());
			String table = plainselect.getFromItem().toString();
			Expression exp = plainselect.getWhere();

			LinkedList<Object> queue = new LinkedList<>();
			LinkedList<String> operators = new LinkedList<>();
			LinkedList<SQLTerm> sqlTerms = new LinkedList<>();
			queue.add(plainselect.getWhere());
			Hashtable<String, Object> htblColNameValue = new Hashtable<>();
			while (!queue.isEmpty()) {
				Object obj = queue.poll();
				if (!(obj instanceof Expression)) {
					String strOperator = (String) obj;
					operators.add(strOperator);
					continue;
				}
				exp = (Expression) obj;
			//	System.out.println(exp.getClass());
				if (exp instanceof EqualsTo || exp instanceof GreaterThan || exp instanceof MinorThan
						|| exp instanceof MinorThanEquals || exp instanceof NotEqualsTo
						|| exp instanceof GreaterThanEquals) {
					String operator = "";
					Expression right = null;
					Expression left = null;
					if (exp instanceof EqualsTo) {
						operator = "=";
						right = ((EqualsTo) exp).getRightExpression();
						left = ((EqualsTo) exp).getLeftExpression();
					}
					if (exp instanceof GreaterThan) {
						operator = ">";
						right = ((GreaterThan) exp).getRightExpression();
						left = ((GreaterThan) exp).getLeftExpression();
					}
					if (exp instanceof MinorThan) {
						operator = "<";
						right = ((MinorThan) exp).getRightExpression();
						left = ((MinorThan) exp).getLeftExpression();
					}
					if (exp instanceof MinorThanEquals) {
						operator = "<=";
						right = ((MinorThanEquals) exp).getRightExpression();
						left = ((MinorThanEquals) exp).getLeftExpression();
					}

					if (exp instanceof NotEqualsTo) {
						operator = "!=";
						right = ((NotEqualsTo) exp).getRightExpression();
						left = ((NotEqualsTo) exp).getLeftExpression();
					}
					if (exp instanceof GreaterThanEquals) {
						operator = ">=";
						right = ((GreaterThanEquals) exp).getRightExpression();
						left = ((GreaterThanEquals) exp).getLeftExpression();
					}

					if (right instanceof StringValue) {
						StringValue val = (StringValue) right;
						String s = val.getValue();
						try {
							java.util.Date d = new SimpleDateFormat("yyyy-MM-dd").parse(val.getValue());
							// htblColNameValue.put(left.toString(), d);
							SQLTerm sqlTerm = new SQLTerm(table, left.toString(), operator, d);
							sqlTerms.addFirst(sqlTerm);
						} catch (ParseException e) {
							// htblColNameValue.put(left.toString(), s);
							SQLTerm sqlTerm = new SQLTerm(table, left.toString(), operator, s);
							sqlTerms.add(sqlTerm);

						}
					} else {
						if (right instanceof LongValue) {
							LongValue val = (LongValue) right;
							int value = Integer.parseInt(val.getValue() + "");
							SQLTerm sqlTerm = new SQLTerm(table, left.toString(), operator, value);
							sqlTerms.add(sqlTerm);

						} else {
							if (right instanceof DoubleValue) {
								DoubleValue val = (DoubleValue) right;
								Double value = (Double.parseDouble(val.getValue() + ""));
								SQLTerm sqlTerm = new SQLTerm(table, left.toString(), operator, value);
								sqlTerms.add(sqlTerm);

							} else {
								if (right instanceof SignedExpression) {

									SignedExpression val = (SignedExpression) right;
									if (val.getExpression().toString().contains(".")) {
										Double value = Double.parseDouble(val.getSign() + "" + val.getExpression());
										SQLTerm sqlTerm = new SQLTerm(table, left.toString(), operator, value);
										sqlTerms.addFirst(sqlTerm);
									} else {
										Integer value = Integer.parseInt(val.getSign() + "" + val.getExpression());
										SQLTerm sqlTerm = new SQLTerm(table, left.toString(), operator, value);
										sqlTerms.add(sqlTerm);
									}
								} else {
									throw new DBAppException("Invalid Value");

								}
							}
						}

					}

				} else {
					if (!(exp instanceof net.sf.jsqlparser.expression.operators.conditional.OrExpression)
							&& !(exp instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression)
							&& !(exp instanceof net.sf.jsqlparser.expression.operators.conditional.XorExpression)) {
						throw new DBAppException("Cant parse this statement");

					}

					if (exp instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression) {
						Expression right = ((AndExpression) exp).getRightExpression();
						Expression left = ((AndExpression) exp).getLeftExpression();
						queue.addFirst(right);
						queue.addFirst("AND");
						queue.addFirst(left);
					}
					if (exp instanceof net.sf.jsqlparser.expression.operators.conditional.OrExpression) {
						Expression right = ((OrExpression) exp).getRightExpression();
						Expression left = ((OrExpression) exp).getLeftExpression();
						queue.addFirst(right);
						queue.addFirst("OR");
						queue.addFirst(left);
					}
					if (exp instanceof net.sf.jsqlparser.expression.operators.conditional.XorExpression) {
						Expression right = ((XorExpression) exp).getRightExpression();
						Expression left = ((XorExpression) exp).getLeftExpression();
						queue.addFirst(right);
						queue.addFirst("XOR");
						queue.addFirst(left);
					}

				}

			}
//			System.out.println(operators);
//			System.out.println(sqlTerms);
			String[] strarrOperators = new String[operators.size()];
			for (int i = 0; i < strarrOperators.length; i++) {
				strarrOperators[i] = operators.poll();
			}
			SQLTerm[] arrSQLTerms = new SQLTerm[sqlTerms.size()];
			for (int i = 0; i < arrSQLTerms.length; i++) {
				arrSQLTerms[i] = sqlTerms.poll();
			}
			return selectFromTable(arrSQLTerms, strarrOperators);
		}
//		DBApp.getPages("Test");
//		dbApp.displayTree("Test");
		throw new DBAppException("Cant parse this statement");

	}

	public static void checkIfClustKey(String clust, String tableName) throws DBAppException {

		try {
			BufferedReader br;

			br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));

			String line = br.readLine();
			Hashtable<String, String[]> tableInfo = new Hashtable<String, String[]>();
			String ClustKey = "";
			boolean found = false;
			boolean ClustKeyfound = false;
			while (line != null) {
				String[] x = line.split(",");
				if (x[0].equals(tableName)) {

					if (x[3].toLowerCase().equals("true")) {
						if (!x[1].equals(clust.toLowerCase())) {
							throw new DBAppException("Have to Use Clustering Key Column (" + x[1] + ") to Update");
						}
					}
					found = true;
				} else {
					if (found)
						break;
				}

				line = br.readLine();
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {

	}
}