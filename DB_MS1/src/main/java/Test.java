import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import javax.xml.crypto.Data;

public class test {
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
	            a1[i - 1] += (int)a1[i] / 26;
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
		int N = 4;
		String S = "bbc";
		String T = "bec";
		printMiddleString(S, T);
	}

}

// TODO Auto-generated method stub
