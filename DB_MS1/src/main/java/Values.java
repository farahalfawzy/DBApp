import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

import org.junit.jupiter.api.Assertions;

public class Values {

	public static void main(String[] args) throws DBAppException, IOException, ParseException, ClassNotFoundException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp();
//		dbApp.init();
//		
//		creating(strTableName, dbApp);
//		
		inserting(dbApp);
		
		
//		updating(strTableName, dbApp);
		
		
		
//		Hashtable rec = new Hashtable();
//		rec.put("name", "paula");
//		deleting(strTableName,dbApp,rec);

	}


	private static void deleting(String strTableName, DBApp dbApp,Hashtable rec) throws ClassNotFoundException, DBAppException, ParseException {
		// TODO Auto-generated method stub
		
		dbApp.deleteFromTable(strTableName,rec);
		dbApp.getPages("Student");
	}


	private static void inserting(DBApp dbApp) throws DBAppException, IOException, ParseException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Hashtable rec = new Hashtable();
		rec.put("id", new Integer(0));
//		rec.put("name", new String("paula"));
//		rec.put("gpa", 0.9);
//		rec.put("Date of Birth", new Date(2015-1900,9-1,17));
	
//		dbApp.getPages("Student");
		dbApp.insertIntoTable("Student", rec);
//		rec.clear();
//		rec.put("id", new Integer(2));
//		rec.put("name", new String("paula"));
//		rec.put("gpa", 0.9);
//		rec.put("Date of Birth", new Date(2013-1900,9-1,17));
//
//
//		dbApp.insertIntoTable("Student", rec);
////		dbApp.getPages("Student");
//
//		rec.clear();
//
//		rec.put("id", new Integer(6));
//		rec.put("name", new String("paula"));
//		rec.put("gpa", 0.9);
//		rec.put("Date of Birth", new Date(2005-1900,9-1,26));
//
//		dbApp.insertIntoTable("Student", rec);
//		rec.clear();
//
//		rec.put("id", new Integer(3));
//		rec.put("name", new String("paula"));
//		rec.put("gpa", 0.9);
//		rec.put("Date of Birth", new Date(2013-1900,11-1,26));
//
//		dbApp.insertIntoTable("Student", rec);
//
//		rec.clear();
//
//		rec.put("name", new String("paula"));
//		rec.put("id", new Integer(7));
//		rec.put("gpa", 1.9);
//		rec.put("Date of Birth", new Date(2013-1900,10-1,26));
//
//		dbApp.insertIntoTable("Student", rec);
//		
//		rec.clear();
		dbApp.getPages("Student");

//		rec.put("id", new Integer(1));
//		rec.put("name", new String("fafa"));
//		rec.put("Date of Birth", new Date(1990-1900,12-1,26));
//			dbApp.insertIntoTable("Student", rec);
			
//			
//			dbApp.getPages("Student");
		

		
		
	}

 

	private static void creating(String strTableName,DBApp dbApp) throws DBAppException {

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
		htblColNameMax.put("id", "9");
		htblColNameMax.put("name", "zzzzzzzzz");
		htblColNameMax.put("Date of Birth", "2022-12-31");
		htblColNameMax.put("gpa", "4.0");

		//dbApp.init();
		
		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
		
	
	}
	public static void updating (String strTableName,DBApp dbApp) throws ClassNotFoundException, DBAppException, IOException, ParseException {
		Hashtable rec = new Hashtable();

//		rec.put("id", new Integer(12));
		
		rec.put("name", "lalala");
		dbApp.updateTable("Student","0.9", rec);
		dbApp.getPages("Student");
	}

}
