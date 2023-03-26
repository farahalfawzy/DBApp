import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;

public class Tuple implements Comparable,Serializable {
	Object Clusteringkey;
	Hashtable <String,Object>record;
	public Tuple(Object Clustkey, Hashtable<String,Object>rec) {
		this.Clusteringkey = Clustkey;
		this.record=rec;
	}
	@Override
	public int compareTo(Object obj) {
		Tuple tup=(Tuple)obj;
		if(this.Clusteringkey instanceof java.lang.Integer){
			return ((Integer)this.Clusteringkey).compareTo((Integer)tup.Clusteringkey);

		}
		if(this.Clusteringkey instanceof java.lang.String){
			return this.Clusteringkey.toString().compareTo(tup.Clusteringkey.toString());
			
		}
		if(this.Clusteringkey instanceof java.lang.Double){
			return ((Double)this.Clusteringkey).compareTo((Double)tup.Clusteringkey);
			
		}
		
			return ((Date)this.Clusteringkey).compareTo((Date)tup.Clusteringkey);
			
		
	}


}
