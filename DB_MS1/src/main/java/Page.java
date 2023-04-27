import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Page extends Vector<Tuple> implements Serializable {

	@Override
	public boolean contains(Object o) {
		Tuple t = (Tuple) o;
		Object clustKey = t.getClusteringkey();
		Object clustType = t.getRecord().get(clustKey);
		int index = -1;
		System.out.println("before index = "+index);
		if (clustType instanceof java.lang.Integer) {
			System.out.println("it is a integer");
			index = this.binarySearchInt((Integer) clustType);
		}
		if (clustType instanceof java.lang.Double) {
			System.out.println("it is a double");
			index = this.binarySearchDouble((Double) clustType);
		}
		if (clustType instanceof java.lang.String) {
			System.out.println("it is a string");
			index = this.binarySearchString((String) clustType);
		}
		if (clustType instanceof java.util.Date) {
			System.out.println("it is a date");
			index = this.binarySearchDate((Date) clustType);
		}
		System.out.println("after index = "+index);
		System.out.println(t.toString());
		if (index!=-1) {
			System.out.println("true contains");
			return true;
		}
		else {
			System.out.println("does not contain");
			return false;
		}
	}

	public boolean removeNotBinary(Object o) {
		boolean flag = false;
		for (int i = 0; i < this.size(); i++) {
			if (this.get(i).equals(o)) {
				this.remove(i);
				i--;
				flag = true;
			}
		}
		return flag;
	}

	public boolean removeBinary(Object o) {
		Tuple t = (Tuple) o;
		Object clustKey = t.getClusteringkey();
		Object clustType = t.getRecord().get(clustKey);
		int index = -1;
		System.out.println("before index = "+index);
		if (clustType instanceof java.lang.Integer) {
			index = this.binarySearchInt((Integer) clustType);
		}
		if (clustType instanceof java.lang.String) {
			index = this.binarySearchString((String) clustType);
		}
		if (clustType instanceof java.lang.Double) {
			index = this.binarySearchDouble((Double) clustType);
		}
		if (clustType instanceof java.util.Date) {
			index = this.binarySearchDate((Date) clustType);
		}
		System.out.println("after index = "+index);
		if (index!=-1) {
			System.out.println("true contains");
			this.remove(index);
			return true;
		}
		else {
			System.out.println("does not contain");
			return false;
		}

	}

	public void replace(Object clustKey, Hashtable<String, Object> htblColNameValue) {
		int index = -1;
		if (clustKey instanceof java.lang.Integer) {
			index = this.binarySearchInt((Integer) clustKey);
		}
		if (clustKey instanceof java.lang.String) {
			index = this.binarySearchString((String) clustKey);
		}
		if (clustKey instanceof java.lang.Double) {
			index = this.binarySearchDouble((Double) clustKey);
		}
		if (clustKey instanceof java.util.Date) {
			index = this.binarySearchDate((Date) clustKey);
		}
		Tuple tuple = this.get(index);
		for (String key : htblColNameValue.keySet()) {
			tuple.getRecord().put(key, htblColNameValue.get(key));
		}
	}

	public boolean containsKey(Object clustKey) {
		int index = -1;
		if (clustKey instanceof java.lang.Integer) {
			index = this.binarySearchInt((Integer) clustKey);
		}
		if (clustKey instanceof java.lang.String) {
			index = this.binarySearchString((String) clustKey);
		}
		if (clustKey instanceof java.lang.Double) {
			index = this.binarySearchDouble((Double) clustKey);
		}
		if (clustKey instanceof java.util.Date) {
			index = this.binarySearchDate((Date) clustKey);
		}
		Object currIndex = this.get(index).getClusteringkey();
		if (currIndex.equals(clustKey))
			return true;
		else
			return false;
	}

	private int binarySearchInt(int ClustKey) {
		int low = 0;
		int high = this.size() - 1;
		int mid = 0;
		while (low <= high) {

			mid = (high + low) / 2;
			Tuple midTuple = this.get(mid);
			if(ClustKey == ((Integer) midTuple.getClusteringkey()))
				return mid;
			
			if (ClustKey < ((Integer) midTuple.getClusteringkey())) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
		return -1;
	}

	private int binarySearchDate(Date ClustKey) {
		int low = 0;
		int high = this.size() - 1;
		int mid = 0;
		while (low <= high) {
			mid = (high + low) / 2;
			Date midDate = (Date) (this.get(mid).getClusteringkey());
			if(ClustKey.equals(midDate))
				return mid;
			if (ClustKey.before(midDate)) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
		return -1;

	}

	private int binarySearchString(String ClustKey) {
		int low = 0;
		int high = this.size() - 1;
		int mid = 0;
		while (low <= high) {
			mid = (high + low) / 2; // 0
			Tuple midTuple = this.get(mid);
			if(ClustKey.equals(midTuple.getClusteringkey().toString()))
				return mid;
			if (ClustKey.compareTo(midTuple.getClusteringkey().toString()) < 0) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
		return -1;

	}

	private int binarySearchDouble(double ClustKey) {
		int low = 0;
		int high = this.size() - 1;
		int mid = 0;
		System.out.println(((Double) this.get(mid).getClusteringkey()));
		while (low <= high) {

			mid = (high + low) / 2;
			Tuple midTuple = this.get(mid);
			System.out.println(((Double) midTuple.getClusteringkey()));
			if(ClustKey == ((Double) midTuple.getClusteringkey()))
				return mid;
			
			if (ClustKey < ((Double) midTuple.getClusteringkey())) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
		return -1;
	}

}