import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Page extends Vector<Tuple> implements Serializable {

	@Override
	public boolean contains(Object o) {
		Tuple t = (Tuple) o;
		Object clustKey = t.getClusteringkey();
		int index=-1;
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
		if(t.equals(this.get(index)))
			return true;
		else
			return false;
	}
	
	@Override
	public boolean remove(Object o) {
		boolean flag = false;
		for(int i=0;i<this.size();i++) {
			if(this.get(i).equals(o)) {
				this.remove(i);
				i--;
				flag=true;
			}
		}
		return flag;
	}

	public void replace(Object clustKey, Hashtable<String, Object> htblColNameValue) {
		int index=-1;
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
		int index=-1;
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
		if(currIndex.equals(clustKey))
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

			if (ClustKey < ((Integer) midTuple.getClusteringkey())) {
				high = mid - 1;
			} else {
				if (ClustKey > ((Integer) midTuple.getClusteringkey())) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		return mid;

	}

	private int binarySearchDate(Date ClustKey)  {
		int low = 0;
		int high = this.size() - 1;
		int mid = 0;
		while (low <= high) {
			mid = (high + low) / 2;
			Date midDate = (Date) (this.get(mid).getClusteringkey());
			if (ClustKey.before(midDate)) {
				high = mid - 1;
			} else {
				if (ClustKey.after(midDate)) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		return mid;

	}

	private int binarySearchString(String ClustKey) {
		int low = 0;
		int high = this.size() - 1;
		int mid = 0;
		while (low <= high) {
			mid = (high + low) / 2; // 0
			Tuple midTuple = this.get(mid);
			;
			if (ClustKey.compareTo(midTuple.getClusteringkey().toString()) < 0) {
				high = mid - 1;
			} else {
				if (ClustKey.compareTo(midTuple.getClusteringkey().toString()) > 0) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		return mid;

	}

	private int binarySearchDouble(double ClustKey) {
		int low = 0;
		int high = this.size() - 1;
		int mid = 0;
		while (low <= high) {

			mid = (high + low) / 2;
			Tuple midTuple = this.get(mid);

			if (ClustKey < ((Double) midTuple.getClusteringkey())) {
				high = mid - 1;
			} else {
				if (ClustKey > ((Double) midTuple.getClusteringkey())) {
					low = mid + 1;
				} else {
					break;
				}
			}
		}
		return mid;
	}

	
	
	
	
	
}