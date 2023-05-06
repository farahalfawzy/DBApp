import java.util.Hashtable;
import java.util.Vector;

public class Leaf extends Node{

private	Vector<Hashtable> Bucket;
	
	
	
	 public Leaf(Object MinX,Object MaxX,Object MinY,Object MaxY,Object MinZ,Object MaxZ) {
		 super( MinX,MaxX, MinY, MaxY, MinZ, MaxZ);
		 Bucket = new Vector<Hashtable>();
		 
	 }




	public Vector<Hashtable> getBucket() {
		return Bucket;
	}




	public void setBucket(Vector<Hashtable> bucket) {
		Bucket = bucket;
	}
		
	
	 
	 
	 
	 
	}

