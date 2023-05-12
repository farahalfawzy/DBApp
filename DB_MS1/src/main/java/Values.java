import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

public class Values {

	public static void main(String[] args) throws DBAppException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp();
		dbApp.init();
//		creating(strTableName, dbApp);
//		dbApp.createIndex(strTableName, new String[] { "id", "gpa", "name" });

	//	inserting(dbApp);
		//dbApp.displayTree(strTableName);
//		Hashtable rec = new Hashtable();
//		rec.put("name", new String("Seif"));
//		rec.put("id", new Integer(67));
//		rec.put("gpa", 1.26);
//		rec.put("Date of Birth", new Date(2012 - 1900, 10 - 1, 26));
//
//		dbApp.insertIntoTable("Student", rec);

		//dbApp.getPages(strTableName);
	
	//	dbApp.displayTree(strTableName);
		Hashtable rec = new Hashtable();
		rec.put("gpa", new Double(1.29));
		rec.put("name", "Nameee3");
		deleting(strTableName,dbApp,rec);
//		updating(strTableName, dbApp);
//		dbApp.getPages("Student");
		dbApp.getPages(strTableName);
		dbApp.displayTree(strTableName);


	}

	private static void deleting(String strTableName, DBApp dbApp, Hashtable rec) throws DBAppException {
		// TODO Auto-generated method stub

		//dbApp.deleteFromTable(strTableName, rec);
		dbApp.getPages("Student");
	}

	private static void inserting(DBApp dbApp) throws DBAppException {
		// TODO Auto-generated method stub
		String strTableName="Student";
		Hashtable rec = new Hashtable();
		rec.put("id", new Integer(10));
		rec.put("name", new String("Kkk"));// 0
		rec.put("gpa", 0.95);// 0
		rec.put("Date of Birth", new Date(2015 - 1900, 9 - 1, 17));// 1
		dbApp.getPages(strTableName);

		dbApp.insertIntoTable("Student", rec);// 001
		rec.clear();
		rec.put("id", new Integer(2));
		rec.put("name", new String("jjjjj"));// 0
		rec.put("gpa", 0.19);// 0
		rec.put("Date of Birth", new Date(2013 - 1900, 9 - 1, 17));// 1
		dbApp.getPages(strTableName);

		dbApp.insertIntoTable("Student", rec);// 001
//		dbApp.getPages("Student");

		rec.clear();

		rec.put("id", new Integer(60));
		rec.put("name", new String("paula"));// 1
		rec.put("gpa", 2.9);// 1
		rec.put("Date of Birth", new Date(2005 - 1900, 9 - 1, 26));// 0
		dbApp.getPages(strTableName);

		dbApp.insertIntoTable("Student", rec);// 110
		rec.clear();

		rec.put("id", new Integer(64));
		rec.put("name", new String("malak"));
		rec.put("gpa", 3.9);
		rec.put("Date of Birth", new Date(2013 - 1900, 11 - 1, 26));
		dbApp.getPages(strTableName);

		dbApp.insertIntoTable("Student", rec);// 111

		rec.clear();

		rec.put("name", new String("nameee"));
		rec.put("id", new Integer(4));
		rec.put("gpa", 1.97);
		rec.put("Date of Birth", new Date(2013 - 1900, 10 - 1, 26));// 011
		dbApp.getPages(strTableName);

		dbApp.insertIntoTable("Student", rec);// 011

		rec.clear();

		rec.put("name", new String("Nameee3"));
		rec.put("id", new Integer(7));
		rec.put("gpa", 1.29);
		rec.put("Date of Birth", new Date(2022 - 1900, 10 - 1, 28));// 011
		dbApp.getPages(strTableName);

		dbApp.insertIntoTable("Student", rec);
//dbApp.displayTree(strTableName);
//		rec.clear();
//		rec.put("name", new String("lolat"));
//		rec.put("id", new Integer(1));
//		rec.put("gpa", 1.99);
//		rec.put("Date of Birth", new Date(2019 - 1900, 10 - 1, 26));// 011
//		dbApp.getPages(strTableName);
//
//		dbApp.insertIntoTable("Student", rec);
//
//		rec.clear();
//		rec.put("name", new String("Seif"));
//		rec.put("id", new Integer(60));
//		rec.put("gpa", 1.26);
//		rec.put("Date of Birth", new Date(2012 - 1900, 10 - 1, 26));// 011
//		dbApp.getPages(strTableName);
//
//		dbApp.insertIntoTable("Student", rec);
//		
//		rec.clear();
//		dbApp.getPages("Student");

//		rec.put("id", new Integer(1));
//		rec.put("name", new String("fafa"));
//		rec.put("Date of Birth", new Date(1990-1900,12-1,26));
//			dbApp.insertIntoTable("Student", rec);

//			
//			dbApp.getPages("Student");

		dbApp.getPages("Student");

	}

	private static void creating(String strTableName, DBApp dbApp) throws DBAppException {

		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("Date of Birth", "java.util.Date");
		htblColNameType.put("gpa", "java.lang.Double");

		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", "A");
		htblColNameMin.put("Date of Birth", "1990-01-01");
		htblColNameMin.put("gpa", "0.0");

		Hashtable htblColNameMax = new Hashtable<>();
		htblColNameMax.put("id", "100");
		htblColNameMax.put("name", "zzzzzzzzz");
		htblColNameMax.put("Date of Birth", "2022-12-31");
		htblColNameMax.put("gpa", "4.0");

		// dbApp.init();

		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);

	}

	public static void updating(String strTableName, DBApp dbApp) throws DBAppException {
		Hashtable rec = new Hashtable();

		rec.put("gpa", 4.0);
		rec.put("name", "hh");
//		rec.put("id", 919);
		dbApp.updateTable("Student", null, rec);
		dbApp.getPages("Student");
	}

}
