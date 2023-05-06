import java.util.Hashtable;
import java.util.Vector;

public class Leaf extends Node{

private	Vector<Hashtable> Bucket;
	
	
	
	 public Leaf(int MinX,int MaxX,int MinY,int MaxY,int MinZ,int MaxZ) {
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

