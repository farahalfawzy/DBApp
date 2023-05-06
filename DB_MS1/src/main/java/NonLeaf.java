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

	public NonLeaf(Object MinX, Object MaxX, Object MinY, Object MaxY, Object MinZ, Object MaxZ) {
		super(MinX, MaxX, MinY, MaxY, MinZ, MaxZ);
		CreateLeaves();
	}

	public void CreateLeaves() {
		Object midX = getAvgX(getMinX(),getMaxX());
		Object midY = getAvgY(getMinY(),getMaxY());
		Object midZ = getAvgZ(getMinZ(),getMaxZ());
		left0 = new Leaf(this.getMinX(), midX, this.getMinY(), midY, this.getMinZ(), midZ);// 000
		left1 = new Leaf(this.getMinX(), midX, this.getMinY(), midY, midZ, this.getMaxZ());// 001
		left2 = new Leaf(this.getMinX(), midX, midY, this.getMaxY(), this.getMinZ(), midZ);// 010
		left3 = new Leaf(this.getMinX(), midX, midY, this.getMaxY(), midZ, this.getMaxZ());// 011
		right3 = new Leaf(midX, this.getMaxX(), this.getMinY(), midY, this.getMinZ(), midZ);// 100
		right2 = new Leaf(midX, this.getMaxX(), this.getMinY(), midY, midZ, this.getMaxZ());// 101
		right1 = new Leaf(midX, this.getMaxX(), this.getMinY(), midY, this.getMinZ(), midZ);// 110
		right0 = new Leaf(midX, this.getMaxX(), midY, this.getMaxY(), midZ, this.getMaxZ());// 111

	}

	private Object getAvgX(Object minX, Object maxX) {
		Object midX = null;
		if(minX instanceof Integer && maxX instanceof Integer)
			midX = ((Integer.parseInt(this.getMinX().toString())) + ((Integer.parseInt(this.getMaxX().toString())))) / 2;
		if(minX instanceof Double && maxX instanceof Double)
			midX = ((Double.parseDouble(this.getMinX().toString())) + ((Double.parseDouble(this.getMaxX().toString())))) / 2;
		if(minX instanceof String && maxX instanceof String)
			midX = printMiddleString(this.getMinX().toString().toLowerCase(),this.getMaxX().toString().toLowerCase(),this.getMinX().toString().length());
		if(minX instanceof java.util.Date && maxX instanceof java.util.Date) {
			try {
			Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse(this.getMinX().toString());
			Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse(this.getMaxX().toString());
			midX = findMidPoint(minDate,maxDate);
			}catch(ParseException e) {}
		}
		return midX;
	}

	private Object getAvgY(Object minY, Object maxY) {
		Object midY = null;
		if(minY instanceof Integer && maxY instanceof Integer)
			midY = ((Integer.parseInt(this.getMinY().toString())) + ((Integer.parseInt(this.getMaxY().toString())))) / 2;
		if(minY instanceof Double && maxY instanceof Double)
			midY = ((Double.parseDouble(this.getMinY().toString())) + ((Double.parseDouble(this.getMaxY().toString())))) / 2;
		if(minY instanceof String && maxY instanceof String)
			midY = printMiddleString(this.getMinY().toString().toLowerCase(),this.getMaxY().toString().toLowerCase(),minY.toString().length());
		if(minY instanceof java.util.Date && maxY instanceof java.util.Date) {
			try {
			Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse(this.getMinY().toString());
			Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse(this.getMaxY().toString());
			midY = findMidPoint(minDate,maxDate);
			}catch(ParseException e) {}
		}
		return midY;
	}
	
	private Object getAvgZ(Object minZ, Object maxZ) {
		Object midZ = null;
		if(minZ instanceof Integer && maxZ instanceof Integer)
			midZ = ((Integer.parseInt(this.getMinZ().toString())) + ((Integer.parseInt(this.getMaxZ().toString())))) / 2;
		if(minZ instanceof Double && maxZ instanceof Double)
			midZ = ((Double.parseDouble(this.getMinZ().toString())) + ((Double.parseDouble(this.getMaxZ().toString())))) / 2;
		if(minZ instanceof String && maxZ instanceof String)
			midZ = printMiddleString(this.getMinZ().toString().toLowerCase(),this.getMaxZ().toString().toLowerCase(),this.getMinZ().toString().length());
		if(minZ instanceof java.util.Date && maxZ instanceof java.util.Date) {
			try {
			Date minDate = new SimpleDateFormat("yyyy-MM-dd").parse(this.getMinZ().toString());
			Date maxDate = new SimpleDateFormat("yyyy-MM-dd").parse(this.getMaxZ().toString());
			midZ = findMidPoint(minDate,maxDate);
			}catch(ParseException e) {}
		}
		return midZ;
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
	
	static String printMiddleString(String S, String T, int N)
    {
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];
 
        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int)S.charAt(i) - 97
                        + (int)T.charAt(i) - 97;
        }
 
        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
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
 
            a1[i] = (int)a1[i] / 2;
        }
 
        String r="";
        for (int i = 1; i <= N; i++) {
            r+=(char)(a1[i] + 97);
        }
        return r;
    }

}
