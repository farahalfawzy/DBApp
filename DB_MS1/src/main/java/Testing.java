import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

public class Testing {
	public static void main(String[] args) throws DBAppException, ParseException {
//		String strTableName = "IntVal";
//		DBApp dbApp = new DBApp();
//		dbApp.init();		
//		creating(strTableName, dbApp);
//		inserting(dbApp);
//dbApp.getPages(strTableName);
//	dbApp.createIndex(strTableName,new String[] {"X","Y","Z"});	
//	dbApp.displayTree(strTableName);
		


	}
	private static void inserting(DBApp dbApp) throws DBAppException {
		// TODO Auto-generated method stub
		Hashtable rec = new Hashtable();
		rec.put("id", new Integer(0));
		rec.put("X", new Integer(2));
		rec.put("Y", new Integer(3));
		rec.put("Z", new Integer(5));
		dbApp.insertIntoTable("IntVal", rec);//000

		rec.clear();
		rec.put("id", new Integer(1));
		rec.put("X", new Integer(9));
		rec.put("Y", new Integer(3));
		rec.put("Z", new Integer(5));
		dbApp.insertIntoTable("IntVal", rec); //100

		rec.put("id", new Integer(5));
		rec.put("X", new Integer(2));
		rec.put("Y", new Integer(3));
		rec.put("Z", new Integer(6));
		dbApp.insertIntoTable("IntVal", rec);//000

		rec.put("id", new Integer(9));
		rec.put("X", new Integer(2));
		rec.put("Y", new Integer(17));
		rec.put("Z", new Integer(5));
		dbApp.insertIntoTable("IntVal", rec);//001

		rec.put("id", new Integer(3));
		rec.put("X", new Integer(8));
		rec.put("Y", new Integer(13));
		rec.put("Z", new Integer(25));
		dbApp.insertIntoTable("IntVal", rec);//111
		rec.put("id", new Integer(6));
		rec.put("x", new Integer(2));
		rec.put("y", new Integer(3));
		rec.put("z", new Integer(6));
		dbApp.insertIntoTable("IntVal", rec);//000
		rec.put("id", new Integer(18));
		rec.put("x", new Integer(2));
		rec.put("y", new Integer(3));
		rec.put("z", new Integer(6));
		dbApp.insertIntoTable("IntVal", rec);//000

	
	}
	private static void creating(String strTableName,DBApp dbApp) throws DBAppException {

		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("X", "java.lang.Integer");
		htblColNameType.put("Y", "java.lang.Integer");
		htblColNameType.put("Z", "java.lang.Integer");


		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("X", "0");
		htblColNameMin.put("Y", "0");
		htblColNameMin.put("Z", "0");


		Hashtable htblColNameMax = new Hashtable<>();
		htblColNameMax.put("id", "1000");
		htblColNameMax.put("X", "10");
		htblColNameMax.put("Y", "20");
		htblColNameMax.put("Z", "40");

		//dbApp.init();
		
		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);

	
	}

}
