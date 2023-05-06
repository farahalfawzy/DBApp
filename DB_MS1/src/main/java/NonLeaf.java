import java.util.Hashtable;
import java.util.Vector;

public class NonLeaf extends Node  {

	public Node left0, left1, left2, left3, right3, right2, right1, right0;
	 public NonLeaf(int MinX,int MaxX,int MinY,int MaxY,int MinZ,int MaxZ) {
		 super( MinX,MaxX, MinY, MaxY, MinZ, MaxZ);
		 CreateLeaves();
	 }
	 public void CreateLeaves() {
		 int midX = (this.getMinX() + this.getMaxX()) / 2;
			int midY = (this.getMinY() + this.getMaxY()) / 2;
			int midZ = (this.getMinZ() + this.getMaxZ()) / 2;
			left0=new Leaf(this.getMinX(),midX,this.getMinY(),midY,this.getMinZ(),midZ);//000
			left1=new Leaf(this.getMinX(),midX,this.getMinY(),midY,midZ,this.getMaxZ());//001
			left2=new Leaf(this.getMinX(),midX,midY,this.getMaxY(),this.getMinZ(),midZ);//010
			left3=new Leaf(this.getMinX(),midX,midY,this.getMaxY(),midZ,this.getMaxZ());//011
			right3=new Leaf(midX,this.getMaxX(),this.getMinY(),midY,this.getMinZ(),midZ);//100
			right2=new Leaf(midX,this.getMaxX(),this.getMinY(),midY,midZ,this.getMaxZ());//101
			right1=new Leaf(midX,this.getMaxX(),this.getMinY(),midY,this.getMinZ(),midZ);//110
			right0=new Leaf(midX,this.getMaxX(),midY,this.getMaxY(),midZ,this.getMaxZ());//111
	

	 }
//		 if(MaxX instanceof String) {
//			String Maxx=(String) MaxX;
//			String Minx=(String) MinX;
//			int size=0;
//			String r="";
//			if(Maxx.length()<Minx.length()) {
//				size=Maxx.length();
//			}
//			else {
//				size=Minx.length();
//				
//				
//			}
//			for(int i=0;i<size;i++) {
//				if (Maxx.charAt(i)==Minx.charAt(i))	{
//					r=r+Maxx.charAt(i);
//					
//				}
//				else {
//					int x =Maxx.charAt(i);
//					int y=Minx.charAt(i);
//					char c =(char) ((x+y)/2);
//					r=r+c;
//					
//					
//				}
//					
//				}
//			 
//			 
//		 }
//		 
		 
		 
		 
//		 Leaf leaf0= new Leaf(MinX,MaxX/2, MinY, MaxY/2, MinZ, MaxZ/2);
		 
		  
			 
			 
		 
		
		 
	 }
		
		

