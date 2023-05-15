
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class NonLeaf extends Node {

	public Node left0, left1, left2, left3, right3, right2, right1, right0;

	public NonLeaf(Object MinX, Object MaxX, Object MinY, Object MaxY, Object MinZ, Object MaxZ, Leaf before,
			Leaf after) {
		super(MinX, MaxX, MinY, MaxY, MinZ, MaxZ);
		CreateLeaves(before, after);
	}

	public void CreateLeaves(Leaf before, Leaf after) {
		Object midX = getAvg(getMinX(), getMaxX());
		Object midY = getAvg(getMinY(), getMaxY());
		Object midZ = getAvg(getMinZ(), getMaxZ());
		left0 = new Leaf(this.getMinX(), midX, this.getMinY(), midY, this.getMinZ(), midZ);// 000
		left1 = new Leaf(this.getMinX(), midX, this.getMinY(), midY, midZ, this.getMaxZ());// 001
		left2 = new Leaf(this.getMinX(), midX, midY, this.getMaxY(), this.getMinZ(), midZ);// 010
		left3 = new Leaf(this.getMinX(), midX, midY, this.getMaxY(), midZ, this.getMaxZ());// 011
		right3 = new Leaf(midX, this.getMaxX(), this.getMinY(), midY, this.getMinZ(), midZ);// 100
		right2 = new Leaf(midX, this.getMaxX(), this.getMinY(), midY, midZ, this.getMaxZ());// 101
		right1 = new Leaf(midX, this.getMaxX(), this.getMinY(), midY, this.getMinZ(), midZ);// 110
		right0 = new Leaf(midX, this.getMaxX(), midY, this.getMaxY(), midZ, this.getMaxZ());// 111
		((Leaf) left0).setBeforeLeaf(before);
		((Leaf) left1).setBeforeLeaf((Leaf) left0);
		((Leaf) left2).setBeforeLeaf((Leaf) left1);
		((Leaf) left3).setBeforeLeaf((Leaf) left2);
		((Leaf) right3).setBeforeLeaf((Leaf) left3);
		((Leaf) right2).setBeforeLeaf((Leaf) right3);
		((Leaf) right1).setBeforeLeaf((Leaf) right2);
		((Leaf) right0).setBeforeLeaf((Leaf) right1);

		((Leaf) left0).setAfterLeaf((Leaf) left1);
		((Leaf) left1).setAfterLeaf((Leaf) left2);
		((Leaf) left2).setAfterLeaf((Leaf) left3);
		((Leaf) left3).setAfterLeaf((Leaf) right3);
		((Leaf) right3).setAfterLeaf((Leaf) right2);
		((Leaf) right2).setAfterLeaf((Leaf) right1);
		((Leaf) right1).setAfterLeaf((Leaf) right0);
		((Leaf) right0).setAfterLeaf(after);

	}

	private Object getAvg(Object min, Object max) {// changed to directly type casting instead of parsing as some errors
													// and null values where returned
		Object midX = null;
		if (min instanceof Integer && max instanceof Integer)
			midX = (((Integer) min) + (((Integer) max))) / 2;
		if (min instanceof Double && max instanceof Double)
			midX = (((Double) min) + (((Double) max))) / 2;
		if (min instanceof String && max instanceof String)
			midX = printMiddleString(((String) min).toLowerCase(), ((String) max).toLowerCase(),
					((String) min).length());
		if (min instanceof java.util.Date && max instanceof java.util.Date) {

			Date minDate = (Date) min;
			Date maxDate = (Date) max;
			midX = findMidPoint(minDate, maxDate);

		}
		return midX;
	}

	private Object findMidPoint(Date date1, Date date2) {
		LocalDate localDate1 = date1.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		LocalDate localDate2 = date2.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

		Period period = Period.between(localDate1, localDate2);
		LocalDate middleLocalDate = localDate1.plus(dividePeriodByTwo(period));

		Instant instant = middleLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
		return Date.from(instant);
	}

	public static Period dividePeriodByTwo(Period period) {
		int years = period.getYears() / 2;
		int months = period.getMonths() / 2;
		int days = period.getDays() / 2;
		return Period.of(years, months, days);
	}

	static String printMiddleString(String S, String T, int N) {
		// Stores the base 26 digits after addition
		int[] a1 = new int[N + 1];

		for (int i = 0; i < N; i++) {
			a1[i + 1] = (int) S.charAt(i) - 97 + (int) T.charAt(i) - 97;
		}

		// Iterate from right to left
		// and add carry to next position
		for (int i = N; i >= 1; i--) {
			a1[i - 1] += (int) a1[i] / 26;
			a1[i] %= 26;
		}

		// Reduce the number to find the middle
		// string by dividing each position by 2
		for (int i = 0; i <= N; i++) {

			// If current value is odd,
			// carry 26 to the next index value
			if ((a1[i] & 1) != 0) {

				if (i + 1 <= N) {
					a1[i + 1] += 26;
				}
			}

			a1[i] = (int) a1[i] / 2;
		}

		String r = "";
		for (int i = 1; i <= N; i++) {
			r += (char) (a1[i] + 97);
		}
		return r;
	}

	@Override
	public String toString() {
		return "NonLeaf [left0=" + left0 + ", left1=" + left1 + ", left2=" + left2 + ", left3=" + left3 + ", right3="
				+ right3 + ", right2=" + right2 + ", right1=" + right1 + ", right0=" + right0 + "]";
	}

}