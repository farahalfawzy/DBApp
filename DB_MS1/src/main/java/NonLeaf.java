import java.util.Hashtable;
import java.util.Vector;

public class NonLeaf extends Node  {


	 public NonLeaf(Object MinX,Object MaxX,Object MinY,Object MaxY,Object MinZ,Object MaxZ) {
		 super( MinX,MaxX, MinY, MaxY, MinZ, MaxZ);
		 if(MaxX instanceof String) {
			String Maxx=(String) MaxX;
			String Minx=(String) MinX;
			int size=0;
			String r="";
			if(Maxx.length()<Minx.length()) {
				size=Maxx.length();
			}
			else {
				size=Minx.length();
				
				
			}
			for(int i=0;i<size;i++) {
				if (Maxx.charAt(i)==Minx.charAt(i))	{
					r=r+Maxx.charAt(i);
					
				}
				else {
					int x =Maxx.charAt(i);
					int y=Minx.charAt(i);
					char c =(char) ((x+y)/2);
					r=r+c;
					
					
				}
					
				}
			 
			 
		 }
		 
		 
		 
		 
//		 Leaf leaf0= new Leaf(MinX,MaxX/2, MinY, MaxY/2, MinZ, MaxZ/2);
		 
		  
			 
			 
		 
		
		 
	 }
		

}
