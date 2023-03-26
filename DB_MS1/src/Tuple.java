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
			return this.Clusteringkey.toString().compareTo(tup.Clusteringkey.toString());

		}
		if (this.Clusteringkey instanceof java.lang.Double) {
			return ((Double) this.Clusteringkey).compareTo((Double) tup.Clusteringkey);

		}

		return ((Date) this.Clusteringkey).compareTo((Date) tup.Clusteringkey);

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
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		return Objects.equals(Clusteringkey, other.Clusteringkey) && Objects.equals(record, other.record);
	}
	
}
