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

	//Vector<Table> allTable = new Vector<Table>();
	static int maxnoOfRows = getMaxRows();
	boolean isDeletingMethod = false;
	boolean isUpdatingMethod = false;

	private static int getMaxRows() {
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
			prop.load(fis);
			return Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
			//return 4;
		} catch (Exception ex) {
			return -1;
		}
	}

	public void init() { // throw
		try {
			File csv = new File("src/main/resources/metadata.csv");
			if (csv.createNewFile()) {
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
			
			if (!CanCreate(htblColNameType, htblColNameMin, htblColNameMax)) {
				throw new DBAppException("Enter Valid Data to create table");
			}
			Set keys=htblColNameType.keySet();
			if(!keys.contains(strClusteringKeyColumn.toLowerCase())) {
				throw new DBAppException("Enter Valid Data to create table");
			}
			try {
				writer = new FileWriter(filePath, true);
				// header
				writer.append("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max");
				writer.append("\n");
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
					//just for testing: TODO
					writer.append("XYZIndex"+",");
					writer.append("Octree"+",");
					//###############
					writer.append(min + ",");
					writer.append(max + ",");
					writer.append("\n");
				}
				writer.close();
				Table myTable = new Table(strTableName, strClusteringKeyColumn.toLowerCase(), htblColNameType, htblColNameMin,
						htblColNameMax);
				//allTable.add(myTable);
				serializeTable(myTable, strTableName);
				myTable=null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		} else {
			throw new DBAppException("Tabble Already exists!");
		}

	}

	private Hashtable<String, String> convertKeysToLowerCreate(Hashtable<String, String> htblColNameValue){
		Hashtable<String, String> result = new Hashtable<String, String>();
		for(String key : htblColNameValue.keySet()) {
			result.put(key.toLowerCase(), htblColNameValue.get(key));
		}
		return result;
	}
	
	private Hashtable<String, Object> convertKeysToLower(Hashtable<String, Object> htblColNameValue){
		Hashtable<String, Object> result = new Hashtable<String, Object>();
		for(String key : htblColNameValue.keySet()) {
			result.put(key.toLowerCase(), htblColNameValue.get(key));
		}
		return result;
	}
	
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		this.isDeletingMethod=false;
		this.isUpdatingMethod = false;

		if (tableExits(strTableName)) {
			htblColNameValue = convertKeysToLower(htblColNameValue);
//			System.out.println(htblColNameValue);
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
					serializePage(t, page, PageName);
					serializeTable(t, t.getTableName());
					page=null;
					t=null;
					return;
				} else {
					int pageind = 0;
					boolean isSmallerThanmin = false;
					Vector pageInfoVector = t.getPageInfo();

					if (clustKey instanceof java.lang.Integer) {
						pageind = binarySearchInt(t, (Integer) clustKey);
						if ((Integer) clustKey < (Integer) ((PageInfo) (pageInfoVector.get(pageind))).getMin()) {
							isSmallerThanmin = true;
						}
					}
					if (clustKey instanceof java.lang.String) {
						pageind = binarySearchString(t, (String) clustKey);
						if (((String) clustKey).toLowerCase()
								.compareTo(((String) (((PageInfo) (pageInfoVector.get(pageind))).getMin())).toLowerCase()) < 0) {
							isSmallerThanmin = true;

						}
					}
					if (clustKey instanceof java.lang.Double) {
						pageind = binarySearchDouble(t, (Double) clustKey);
						if ((Double) clustKey < ((Double) ((PageInfo) (pageInfoVector.get(pageind))).getMin())) {
							isSmallerThanmin = true;
						}
					}
					if (clustKey instanceof java.util.Date) {
						pageind = binarySearchDate(t, (Date) clustKey);
						if (((Date) clustKey).before(((Date) ((PageInfo) (pageInfoVector.get(pageind))).getMin()))) {
							isSmallerThanmin = true;

						}
					}

					String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
					Page page = deserializePage(pagename);
					Tuple tuple = new Tuple(clustKey, htblColNameValue);
					if (page.contains2(tuple)) {
						page=null;
						t=null;
						throw new DBAppException("Clustering Key already exists");
					} else {
						if (isSmallerThanmin) {
							if(pageind-1>-1) {
								if(((PageInfo) (pageInfoVector.get(pageind-1))).getCount()<maxnoOfRows) {
									serializePage(t, page, t.getTableName() + "" + pageind);
									 pagename = ((PageInfo) (pageInfoVector.get(pageind-1))).getPageName();
									 page = deserializePage(pagename);
									 int indexInPage=page.getIndexInPage(tuple);
									 page.insertElementAt(tuple, indexInPage);
//									 page.add(tuple);
//									 Collections.sort(page);
									((PageInfo) pageInfoVector.get(pageind-1)).setMax(getMaxInPage(page));
									((PageInfo) pageInfoVector.get(pageind-1)).setMin(getMinInPage(page));
									serializePage(t, page, t.getTableName() + "" + (pageind-1));
									serializeTable(t, t.getTableName());
									page=null;
									t=null;
									return;
								}
							}

						}
						if (page.size() < maxnoOfRows) {
							int indexInPage=page.getIndexInPage(tuple);
							 page.insertElementAt(tuple, indexInPage);
//							page.add(tuple);
//							Collections.sort(page);
							((PageInfo) pageInfoVector.get(pageind)).setMax(getMaxInPage(page));
							((PageInfo) pageInfoVector.get(pageind)).setMin(getMinInPage(page));
							serializePage(t, page, t.getTableName() + "" + pageind);
							serializeTable(t, t.getTableName());
							page=null;
							t=null;
							return;
						} else {// law fel nos
							int indexInPage=page.getIndexInPage(tuple);
							 page.insertElementAt(tuple, indexInPage);
//							 page.add(tuple);
//							Collections.sort(page);
							Tuple newtup = (Tuple) page.remove(page.size() - 1);
							((PageInfo) pageInfoVector.get(pageind)).setMax(getMaxInPage(page));
							((PageInfo) pageInfoVector.get(pageind)).setMin(getMinInPage(page));
							serializePage(t, page, t.getTableName() + "" + pageind);
							page=null;
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
//									Collections.sort(newPage);

									serializePage(t, newPage, t.getTableName() + "" + ind);
									t.setCurrentMaxId(t.getCurrentMaxId() + 1);
									newPage=null;

									break;
								} else {// lesa fel nos

									Page nextpage = deserializePage(
											((PageInfo) (pageInfoVector.get(ind))).getPageName());

									if (nextpage.size() < maxnoOfRows) {
										 //page.getIndex(tuple);
										 int newindexInPage=nextpage.getIndexInPage(newtup);
										 nextpage.insertElementAt(newtup, newindexInPage);
										// System.out.println(newtup.Clusteringkey);
//										nextpage.add(newtup);
//										Collections.sort(nextpage);
										((PageInfo) pageInfoVector.get(ind)).setMax(getMaxInPage(nextpage));
										((PageInfo) pageInfoVector.get(ind)).setMin(getMinInPage(nextpage));
										serializePage(t, nextpage, t.getTableName() + "" + ind);
										nextpage=null;
										break;

									} else {
										 int newindexInPage=nextpage.getIndexInPage(newtup);
										 nextpage.insertElementAt(newtup, newindexInPage);
//										nextpage.add(newtup);
//										Collections.sort(nextpage);
										newtup = (Tuple) nextpage.remove(nextpage.size() - 1);
										((PageInfo) pageInfoVector.get(ind)).setMax(getMaxInPage(nextpage));
										((PageInfo) pageInfoVector.get(ind)).setMin(getMinInPage(nextpage));
										serializePage(t, nextpage, t.getTableName() + "" + ind);
										nextpage=null;
										ind = ind + 1;
									}

								}
							}
						}
					}

				}

				// check if page is full
				serializeTable(t, t.getTableName());
				t=null;

			} else {
				throw new DBAppException("Invalid Data");
			}

		} else {
			throw new DBAppException("Table not found");
		}
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Page page=null;
		Table t= null;
		try {
			if (!tableExits(strTableName))
				throw new DBAppException("Table does not exist");
			isUpdatingMethod = true;
			htblColNameValue = convertKeysToLower(htblColNameValue);
			if (isValid(strTableName, htblColNameValue)) {

				int pageind = -1;
				String type = this.getClusteringKeyType(strTableName);
				t = deserializeTable(strTableName);
				if(t.getPageInfo().size()==0) {
					serializeTable(t, strTableName);
					t=null;
					return;
				}
				Object ClustObj = null;
				switch (type) {
				case "java.lang.Integer":
					int clust = Integer.parseInt(strClusteringKeyValue);

					pageind = binarySearchInt(t, clust);
					ClustObj = clust;

					break;
				case "java.lang.String":

					pageind = binarySearchString(t, strClusteringKeyValue);
					ClustObj = strClusteringKeyValue;
					break;
				case "java.lang.Double":
					double clustdouble = Double.parseDouble(strClusteringKeyValue);
					pageind = binarySearchDouble(t, clustdouble);
					ClustObj = clustdouble;

					break;
				case "java.util.Date":
					Date date = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
					pageind = binarySearchDate(t, date);
					ClustObj = date;
//					System.out.println(date);
					break;
				}
				Vector pageInfoVector = t.getPageInfo();
				String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
				page = deserializePage(pagename);
				if (page.containsKey(ClustObj)) {					//hna 3'yrt containsKey
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
					page.replace(ClustObj,htblColNameValue);
					isUpdatingMethod = false;
					serializePage(t, page, strTableName + "" + pageind);

					serializeTable(t, strTableName);
					page=null;
					t=null;
				}

				else {
					page=null;
					t=null;
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
			t=null;
			isUpdatingMethod = false;
			throw new DBAppException(e.toString());
		}catch(ParseException e) {
			serializeTable(t, strTableName);
			t=null;
			isUpdatingMethod = false;
			throw new DBAppException("enter valid clustring key value");
		}
		catch(java.lang.NumberFormatException e) {
			serializeTable(t, strTableName);
			t=null;
			isUpdatingMethod = false;
			throw new DBAppException("enter valid clustring key value");
		}

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException{
		this.isDeletingMethod = true;
		if (tableExits(strTableName)) {
			htblColNameValue = convertKeysToLower(htblColNameValue);
//			if(t.getPageInfo().size()==0)
//				throw new DBAppException("No more buckets to delete");
			if (isValid(strTableName, htblColNameValue)) {
				Table t = deserializeTable(strTableName);
				String myCluster = t.getClusteringKey();
				if(t.getPageInfo().size()==0) {
					serializeTable(t, strTableName);
					t=null;
					return;
				}
				//New
				Octree myOct = deserializeOctree("XYZIndex");
				Tuple myTuple = new Tuple(t.getClusteringKey(), htblColNameValue);
				if(htblColNameValue.contains(myOct.getX()) && htblColNameValue.contains(myOct.getY()) &&
						htblColNameValue.contains(myOct.getZ())) {
					Hashtable<String,Object> key = new Hashtable<>();
					key.put(myOct.getX(), htblColNameValue.get(myOct.getX()));
					key.put(myOct.getY(), htblColNameValue.get(myOct.getY()));
					key.put(myOct.getZ(), htblColNameValue.get(myOct.getZ()));
					String pageName = myOct.getPageName(htblColNameValue);
					Vector pageInfoVector = t.getPageInfo();
					if (pageInfoVector.size() != 0) {
						Page page = deserializePage(pageName);
						if (page.contains(myTuple)) {
							page.removeBinary(myTuple);
							myOct.deleteTuple(key);
							String res="";
							for(int i = pageName.length()-1; i>(-1);i--) {
								if((pageName.charAt(i)) >='0' && pageName.charAt(i) <='9') {
									res=pageName.charAt(i)+res;
								}
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
								serializeIndex(t, myOct, strTableName);
								myOct=null;
								page=null;
							}
						}
					}
				}
				//End of the new part
				
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
				//	System.out.println(pageind);
					Vector pageInfoVector = t.getPageInfo();
					if (pageInfoVector.size() != 0) {
						String pagename = ((PageInfo) (pageInfoVector.get(pageind))).getPageName();
						Page page = deserializePage(pagename);
//						System.out.println("before contains");
						if (page.contains(myTuple)) {
//							System.out.println("after contains");
							// System.out.println("was here111111");
							page.removeBinary(myTuple);
							if (page.size() == 0) {
								deletingFiles(pageind, pageInfoVector, t);
							} else {

								Object max = getMaxInPage(page);
								Object min = getMinInPage(page);
								((PageInfo) pageInfoVector.get(pageind)).setMax(max);
								((PageInfo) pageInfoVector.get(pageind)).setMin(min);

								serializePage(t, page, t.getTableName() + "" + pageind);
								page=null;
							}
						}

					}
				} else {
					Vector pageInfoVector = t.getPageInfo();
					removeFromAllPages(pageInfoVector, myTuple, 0, t, htblColNameValue);
				}
				serializeTable(t, strTableName);
				t=null;
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
	private void removeFromAllPages(Vector pageInfoVector, Tuple myTuple, int i, Table t, Hashtable htblColNameValue){
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
			page=null;
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
			if (!ClustKeyfound && !isDeletingMethod && !isUpdatingMethod) { // ana 3mlt deh 3lshan fel delete lw ana msh m3aya elcluster key
														// e3ml delete 3ady bardo
				throw new DBAppException("You have to insert Clustering Key");
			}
			boolean flag = true;

			for (String key : htblColNameValue.keySet()) {
				// System.out.println(key);
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
						if(isDeletingMethod) {
							if (!(compareNameType instanceof java.lang.Integer))
								throw new DBAppException("Reenter your values!");
							else
								return false;
								
						}else
						// System.out.println("int");
							return false;
					}
				}
				case "java.lang.String": {

					if (compareNameType instanceof java.lang.String
							&& tableInfo.get(key)[1].toLowerCase().compareTo(htblColNameValue.get(key).toString().toLowerCase()) <= 0
							&& tableInfo.get(key)[2].toLowerCase().compareTo(htblColNameValue.get(key).toString().toLowerCase()) >= 0) {
						flag = true;

						continue;
					}
						else {
							if(isDeletingMethod) {

								if (!(compareNameType instanceof java.lang.String))
									throw new DBAppException("Reenter your values!");
								else
									return false;
							}else
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
						if(isDeletingMethod) {
							if (!(compareNameType instanceof java.lang.Double))
								throw new DBAppException("Reenter your values!");
							else
								return false;
								
						}else
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
							
								if(isDeletingMethod)
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
			t=null;
			p=null;
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
			t=null;
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
	
	private static void serializeIndex(Table t, Octree p, String indexName) {
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
		//System.out.println(mid);
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
			if ((ClustKey.toLowerCase()).compareTo((((PageInfo) (pageInfoVector.get(mid))).getMin()).toString().toLowerCase()) < 0) {
				high = mid - 1;
			} else {
				if ((ClustKey.toLowerCase()).compareTo((((PageInfo) (pageInfoVector.get(mid))).getMax()).toString().toLowerCase()) > 0) {
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

		Tuple max = page.get(page.size()-1);
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

	private static boolean CanCreate(Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
		boolean flag = true;
		if (!(htblColNameType.size() == htblColNameMin.size() && htblColNameMax.size() == htblColNameType.size()))
			return false;
		for (String key : htblColNameType.keySet()) {
			String type = htblColNameType.get(key);
			if (type.equals("java.util.Date")) {
				//System.out.println(type);
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

	public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {

	}
}