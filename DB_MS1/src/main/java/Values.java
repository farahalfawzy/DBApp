import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;

public class Values {

	public static void main(String[] args) throws DBAppException, IOException, ParseException, ClassNotFoundException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp();
		dbApp.init();;
		
		creating(strTableName, dbApp);
		
		inserting(dbApp);
		
		Hashtable rec = new Hashtable();
		rec.put("name", new String("malak"));
		deleting(strTableName,dbApp, rec);
		dbApp.getPages("Student");
		rec.clear();
//		rec.put("id", new Integer(2));
//		rec.put("name", new String("malak"));
//		dbApp.insertIntoTable("Student", rec);

		
//		
		dbApp.getPages("Student");
	
//		final DBApp dbApp = new DBApp();
//        dbApp.init();
//
//        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
//        String record;
//        Hashtable<String, Object> row = new Hashtable<>();
//        int c = 0;
//        int finalLine = 1;
//        while ((record = pcsTable.readLine()) != null && c <= finalLine) {
//            if(c == finalLine) {
//                String[] fields = record.split(",");
//
//                row.put("pc_id", Integer.parseInt(fields[0].trim()));
//                row.put("student_id", fields[1].trim());
//            }
//            c++;
//        }
//
//
//        String table = "pcs";
//        dbApp.deleteFromTable(table, row);
	}


	private static void deleting(String strTableName, DBApp dbApp,Hashtable rec) throws ClassNotFoundException, DBAppException, ParseException {
		// TODO Auto-generated method stub
		
		dbApp.deleteFromTable(strTableName,rec);
		dbApp.getPages("Student");
	}


	private static void inserting(DBApp dbApp) throws DBAppException, IOException, ParseException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Hashtable rec = new Hashtable();
		rec.put("id", new Integer(5));
		rec.put("name", new String("malak"));
				
		dbApp.getPages("Student");
		dbApp.insertIntoTable("Student", rec);
//		dbApp.getPages("Student");
		rec.clear();
		rec.put("id", new Integer(2));
		rec.put("name", new String("malak"));
		dbApp.insertIntoTable("Student", rec);

		rec.clear();

		rec.put("id", new Integer(6));
		rec.put("name", new String("malak"));
		dbApp.insertIntoTable("Student", rec);

		rec.put("id", new Integer(3));
		rec.put("name", new String("malak"));
		dbApp.insertIntoTable("Student", rec);

		rec.clear();

		rec.put("name", new String("malak"));
		rec.put("id", new Integer(12));
		dbApp.insertIntoTable("Student", rec);
		
		rec.clear();
		
		rec.put("id", new Integer(1));
		rec.put("name", new String("malak"));
		rec.put("Date of Birth", new Date(2002,9,26));
			dbApp.insertIntoTable("Student", rec);
			
//			
			dbApp.getPages("Student");
		


		
		
	}


	private static void creating(String strTableName,DBApp dbApp) throws DBAppException {
		
//		Calendar calendar = Calendar.getInstance();
//		calendar.set(Calendar.YEAR, 1974);
//		calendar.set(Calendar.MONTH, Calendar.JANUARY);
//		calendar.set(Calendar.DAY_OF_MONTH, 1);
//		Date minDateOfBirth = calendar.getTime();
//		
//		Calendar cal = Calendar.getInstance();
//		cal.set(Calendar.YEAR, 2974);
//		cal.set(Calendar.MONTH, Calendar.JANUARY);
//		cal.set(Calendar.DAY_OF_MONTH, 1);
//		Date maxDateOfBirth = cal.getTime();
		
		
		// TODO Auto-generated method stub
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("Date of Birth", "java.util.Date");

		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", "A");
		htblColNameMin.put("Date of Birth", "1990-01-01");

		Hashtable htblColNameMax = new Hashtable<>();
		htblColNameMax.put("id", "10000");
		htblColNameMax.put("name", "zzzzzzzzz");
		htblColNameMax.put("Date of Birth", "20222-12-31");
		
		//dbApp.init();
		
		dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
		
			// TODO Auto-generated catch block
	
	}

}
