import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class test {
	public static void main(String[] args) throws IOException {

		Hashtable t = new Hashtable();
		t.put("Name", "Seif");
		t.put("Age", "20");
		t.put("Gender", "M");
		Tuple myTuple1 = new Tuple("Name", t);

		Hashtable t1 = new Hashtable();
		t1.put("Name", "Merna");
		t1.put("Age", "16");
		t1.put("Gender", "F");
		Tuple myTuple2 = new Tuple("Name", t1);

		Hashtable t2 = new Hashtable();
		t2.put("Age", "16");
		t2.put("Name", "Merna");
//		t2.put("Gender", "F");
		Tuple myTuple3 = new Tuple("Name", t2);
		
		Vector<Tuple> myArray = new Vector<Tuple>();

		myArray.add(myTuple1);
		myArray.add(myTuple2);
		
		boolean flag = myArray.contains(myTuple3);
		System.out.println(flag);


	}
}
