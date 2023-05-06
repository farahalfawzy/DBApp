import java.util.Vector;

public abstract class Node {

private	int MinX;
private	int MaxX;
private	int MinY;
private	int MaxY;
private	int MinZ;
private	int MaxZ;
	
	
	
  	public Node(int MinX,int MaxX,int MinY,int MaxY,int MinZ,int MaxZ) {
		this.MinX=MinX;
		this.MaxX=MaxX;
		this.MinY=MinY;
		this.MaxY=MaxY;
		this.MinZ=MinZ;
		this.MaxZ=MaxZ;
		
		
	}







	public int getMinX() {
		return MinX;
	}



	public void setMinX(int minX) {
		MinX = minX;
	}



	public int getMaxX() {
		return MaxX;
	}



	public void setMaxX(int maxX) {
		MaxX = maxX;
	}



	public int getMinY() {
		return MinY;
	}



	public void setMinY(int minY) {
		MinY = minY;
	}



	public int getMaxY() {
		return MaxY;
	}



	public void setMaxY(int maxY) {
		MaxY = maxY;
	}



	public int getMinZ() {
		return MinZ;
	}



	public void setMinZ(int minZ) {
		MinZ = minZ;
	}



	public int getMaxZ() {
		return MaxZ;
	}



	public void setMaxZ(int maxZ) {
		MaxZ = maxZ;
	}
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	

}
