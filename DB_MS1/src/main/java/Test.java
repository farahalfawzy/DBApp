import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.crypto.Data;

public class Test {

	public static void main(String[] args) throws ParseException {

//		
		String r = "";
		String aa = "afogk";
		String a = "asdji";
		for (int i = 0; i < a.length(); i++) {
			if (a.charAt(i) == aa.charAt(i)) {
				r = r + a.charAt(i);

			} else {
				int x = a.charAt(i);
				int y = aa.charAt(i);
				char c = (char) ((x + y) / 2);
				r = r + c;

			}
//	
//		Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2002-05-30");
//		Date date2 = new SimpleDateFormat("yyyy-MM-dd").parse("2002-09-30");
//     	int year1=date.getYear();
//     	int year2=date2.getYear();
//     	int m1=date.getMonth();
//     	int m2=date2.getMonth();
//     	int d1=date.getDay();
//     	int d2=date2.getDay();
//     	
//     	int davg= (d1+d2)/2;
//     	int mavg= (m1+m2)/2;
//     	int yavg=(year1+year2)/2;
//     	String s=yavg+"-"+0+""+mavg+"-"+davg;
//     	Date r=new SimpleDateFormat("yyyy-MM-dd").parse(s);
//     	System.out.println(r);

		}
		System.out.println(r);
	}
}

// TODO Auto-generated method stub
