import java.util.Vector;

public abstract class Node {

private	Vector<Node> Nodes;
private	Object MinX;
private	Object MaxX;
private	Object MinY;
private	Object MaxY;
private	Object MinZ;
private	Object MaxZ;
	
	
	
  	public Node(Object MinX,Object MaxX,Object MinY,Object MaxY,Object MinZ,Object MaxZ) {
		Nodes = new Vector<Node>();
		this.MinX=MinX;
		this.MaxX=MaxX;
		this.MinY=MinY;
		this.MaxY=MaxY;
		this.MinZ=MinZ;
		this.MaxZ=MaxZ;
		
		
	}



	public Vector<Node> getNodes() {
		return Nodes;
	}



	public void setNodes(Vector<Node> nodes) {
		Nodes = nodes;
	}



	public Object getMinX() {
		return MinX;
	}



	public void setMinX(Object minX) {
		MinX = minX;
	}



	public Object getMaxX() {
		return MaxX;
	}



	public void setMaxX(Object maxX) {
		MaxX = maxX;
	}



	public Object getMinY() {
		return MinY;
	}



	public void setMinY(Object minY) {
		MinY = minY;
	}



	public Object getMaxY() {
		return MaxY;
	}



	public void setMaxY(Object maxY) {
		MaxY = maxY;
	}



	public Object getMinZ() {
		return MinZ;
	}



	public void setMinZ(Object minZ) {
		MinZ = minZ;
	}



	public Object getMaxZ() {
		return MaxZ;
	}



	public void setMaxZ(Object maxZ) {
		MaxZ = maxZ;
	}
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	
  	

}
