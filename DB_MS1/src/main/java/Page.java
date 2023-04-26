import java.io.Serializable;
import java.util.Vector;

public class Page extends Vector<Tuple> implements Serializable {

	@Override
	public boolean contains(Object o) {
		Tuple t = (Tuple) o;
		for(int i=0;i<this.size();i++) {
			if(this.get(i).equals(t)) {
				return true;
			}
		}
		return false;
	}

	public boolean contains2(Object o) {
		Tuple t = (Tuple) o;
		for(int i=0;i<this.size();i++) {
			Tuple curTuple=(Tuple) this.get(i);
			if(curTuple.Clusteringkey.equals(t.Clusteringkey)) {
				System.out.println("tuple alreadyy");
				return true;
			}
		}
		return false;
	}
	public boolean containsKey(	Object clustKey) {
		for(int i=0;i<this.size();i++) {
			Tuple curTuple=(Tuple) this.get(i);
			if(curTuple.Clusteringkey.equals(clustKey)) {
				return true;
			}
		}
		return false;
	}
	@Override
	public boolean remove(Object o) {
		boolean flag = false;
		for(int i=0;i<this.size();i++) {
			if(this.get(i).equals(o)) {
				this.remove(i);
				i--;
				flag=true;
			}
		}
		return flag;
	}
	
	
}
