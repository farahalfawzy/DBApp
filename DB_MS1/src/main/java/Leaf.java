import java.util.Hashtable;
import java.util.Vector;

public class Leaf extends Node{

private	Vector<Hashtable<String,Object>> Bucket;
	
	
	
	 public Leaf(Object MinX,Object MaxX,Object MinY,Object MaxY,Object MinZ,Object MaxZ) {
		 super( MinX,MaxX, MinY, MaxY, MinZ, MaxZ);
		 Bucket = new Vector<Hashtable<String,Object>>();
		 
	 }

	 

	@Override
	public String toString() {
		return "Leaf [Bucket=" + Bucket + "]";
	}



	public Vector<Hashtable<String,Object>> getBucket() {
		return Bucket;
	}




	public void setBucket(Vector<Hashtable<String,Object>> bucket) {
		Bucket = bucket;
	}
		
	public void tranformToLeaf(NonLeaf nonLeaf) {
		
	}
	 
	 
	 
	 
	}

