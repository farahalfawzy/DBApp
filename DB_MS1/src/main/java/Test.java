import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import javax.xml.crypto.Data;

public class Test {
	static String printMiddleString(String S, String T) {
		int N = S.length() > T.length() ? S.length() : T.length();
		S = S.toLowerCase();
		T = T.toLowerCase();
		// Stores the base 26 digits after addition
		int[] a1 = new int[N + 1];

		for (int i = 0; i < N; i++) {
			if (i >= S.length()) {
				a1[i + 1] = (int) T.charAt(i) - 97;
				continue;
			}
			if (i >= T.length()) {
				a1[i + 1] = (int) S.charAt(i) - 97;
				continue;
			}
			a1[i + 1] = (int) S.charAt(i) - 97 + (int) T.charAt(i) - 97;
		}
		System.out.println(Arrays.toString(a1));
		System.out.println(Arrays.toString(S.toCharArray()));
		System.out.println(Arrays.toString(T.toCharArray()));

		// Iterate from right to left
		// and add carry to next position
		for (int i = N; i >= 1; i--) {
			a1[i - 1] += (int) a1[i] / 26;
			a1[i] %= 26;
		}
		System.out.println(Arrays.toString(a1));

		// Reduce the number to find the middle
		// string by dividing each position by 2
		for (int i = 0; i <= N; i++) {

			// If current value is odd,
			// carry 26 to the next index value
			if ((a1[i] & 1) != 0) {
				System.out.println(a1[i]);

				if (i + 1 <= N) {
					a1[i + 1] += 26;
				}
			}

			a1[i] = (int) a1[i] / 2;
		}
		String res = "";
		for (int i = 1; i <= N; i++) {
			res += (char) (a1[i] + 97);
			System.out.print((char) (a1[i] + 97));
		}
		return res;
	}

	// Driver Code
	public static void main(String[] args) {
//		int N = 4;
//		String S = "bbc";
//		String T = "bec";
//		printMiddleString(S, T);
//		String r = "";
//		String aa = "afogk";
//		String a = "asdji";
//		for (int i = 0; i < a.length(); i++) {
//			if (a.charAt(i) == aa.charAt(i)) {
//				r = r + a.charAt(i);
//
//			} else {
//				int x = a.charAt(i);
//				int y = aa.charAt(i);
//				char c = (char) ((x + y) / 2);
//				r = r + c;
//
//			}
//		}
		String pageName = "Student111190";
		String res="";
		for(int i = pageName.length()-1; i>(-1);i--) {
			if((pageName.charAt(i)) >='0' && pageName.charAt(i) <='9') {
				res=pageName.charAt(i)+res;
			}
		}
		int pageind = Integer.parseInt(res);
		System.out.println(pageind);
		String x= "operatorSeifra2asa";
		System.out.println(x.substring(0,8));
		Hashtable<String,Object> packedhash= new Hashtable<String,Object>();
		
		Vector<Hashtable<String,Object>> temp = new Vector<>();

		// create a new Hashtable and add some key-value pairs
		Hashtable<String, Object> hashtable1 = new Hashtable<>();
		hashtable1.put("key1", "value1");
		hashtable1.put("key2", 123);

		// add the first Hashtable to the Vector
		temp.add(hashtable1);

		// create a second Hashtable and add some different key-value pairs
		Hashtable<String, Object> hashtable2 = new Hashtable<>();
		hashtable2.put("key3", true);
		hashtable2.put("key4", 3.14);

		// add the second Hashtable to the Vector
		temp.add(hashtable2);

		// create a third Hashtable and add some different key-value pairs
		Hashtable<String, Object> hashtable3 = new Hashtable<>();
		hashtable3.put("key5", "hello");
		hashtable3.put("key6", 1.2);

		// add the third Hashtable to the Vector
		temp.add(hashtable3);

		for(int i=0 ; i< temp.size();i++) {
			packedhash.putAll(temp.get(i));
		}
		System.out.println(packedhash);
		
	}
}

// TODO Auto-generated method stub
