import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;

public class Values {

	public static void main(String[] args) throws DBAppException, IOException, ParseException, ClassNotFoundException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp();
		creating(strTableName, dbApp);
		
		inserting(dbApp);
//		
		Hashtable rec = new Hashtable();
		rec.put("name", new String("paula"));
		//deleting(strTableName,dbApp, rec);
	}


	private static void deleting(String strTableName, DBApp dbApp,Hashtable rec) throws ClassNotFoundException, DBAppException, ParseException {
		// TODO Auto-generated method stub
		dbApp.deleteFromTable(strTableName,rec);
		dbApp.getPages("Student");
	}


	private static void inserting(DBApp dbApp) throws DBAppException, IOException, ParseException {
		// TODO Auto-generated method stub
		Hashtable rec = new Hashtable();
		rec.put("id", new Integer(5));
		rec.put("name", new String("farah"));
				
		dbApp.getPages("Student");
		dbApp.insertIntoTable("Student", rec);
		dbApp.getPages("Student");
		rec.clear();
		rec.put("id", new Integer(2));
		rec.put("name", new String("malak"));
		dbApp.insertIntoTable("Student", rec);

		rec.clear();

		rec.put("id", new Integer(6));
		rec.put("name", new String("paula"));
		dbApp.insertIntoTable("Student", rec);

		rec.put("id", new Integer(3));
		rec.put("name", new String("seif"));
		dbApp.insertIntoTable("Student", rec);

		rec.clear();

		rec.put("name", new String("tony"));
		dbApp.insertIntoTable("Student", rec);
		
		dbApp.getPages("Student");
	}


	private static void creating(String strTableName,DBApp dbApp) {
		// TODO Auto-generated method stub
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");

		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", "a");

		Hashtable htblColNameMax = new Hashtable<>();
		htblColNameMax.put("id", "10000");
		htblColNameMax.put("name", "zzzzzzzzz");
		dbApp.init();
		try {
			dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
