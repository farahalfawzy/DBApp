import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Objects;

public class Tuple implements Comparable, Serializable {
	Object Clusteringkey;
	Hashtable<String, Object> record;

	public Tuple(Object Clustkey, Hashtable<String, Object> rec) {
		this.Clusteringkey = Clustkey;
		this.record = rec;
	}

	public Object getClusteringkey() {
		return Clusteringkey;
	}

	public void setClusteringkey(Object clusteringkey) {
		Clusteringkey = clusteringkey;
	}

	public Hashtable<String, Object> getRecord() {
		return record;
	}

	public void setRecord(Hashtable<String, Object> record) {
		this.record = record;
	}

	@Override
	public int compareTo(Object obj) {
		Tuple tup = (Tuple) obj;
		if (this.Clusteringkey instanceof java.lang.Integer) {
			return ((Integer) this.Clusteringkey).compareTo((Integer) tup.Clusteringkey);

		}
		if (this.Clusteringkey instanceof java.lang.String) {
			return this.Clusteringkey.toString().toLowerCase().compareTo(tup.Clusteringkey.toString().toLowerCase());

		}
		if (this.Clusteringkey instanceof java.lang.Double) {
			return ((Double) this.Clusteringkey).compareTo((Double) tup.Clusteringkey);

		}
		if (((Date) this.Clusteringkey).before((Date) tup.Clusteringkey))
			return -1;
		return 1;

	}

	@Override
	public String toString() {
		return "Tuple [Clusteringkey=" + Clusteringkey + ", record=" + record + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(Clusteringkey, record);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (other.getRecord().size() < this.getRecord().size()) {
			System.out.println("deleting was herezzz other.getRecord().size() < this.getRecord().size()");
			for (String key : other.getRecord().keySet()) {
				Object otherValue = other.getRecord().get(key);
				Object thisValue = this.getRecord().get(key);
				if (otherValue instanceof java.lang.String && thisValue instanceof java.lang.String) {
					if (!(((String) otherValue).toLowerCase().equals(((String) thisValue).toLowerCase())))
						return false;
				} else {
					if (!(otherValue.equals(thisValue)))
						return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}
	
//	public boolean greaterThan (Object obj) {
//		if (obj == null)
//			return false;
//		if (this == obj)
//			return true;
//		if (getClass() != obj.getClass())
//			return false;
//		Tuple other = (Tuple) obj;
//		for (String key : other.getRecord().keySet()) {
//			Object otherValue = other.getRecord().get(key);
//			Object thisValue = this.getRecord().get(key);
//			if (otherValue instanceof java.lang.String && thisValue instanceof java.lang.String) {
//				if (!(((String) otherValue).toLowerCase().compareTo(((String) thisValue).toLowerCase())>0))
//					return false;
//			} else {
//				if (otherValue<(thisValue))
//					return false;
//			}
//		}
//		return true;
//	}

}
