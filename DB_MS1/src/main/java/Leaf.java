import java.util.Hashtable;
import java.util.Vector;

public class Leaf extends Node {

	private Vector<Hashtable<String, Object>> Bucket;

	private int size = 0;
	private Leaf beforeLeaf;

	private Leaf afterLeaf;
	private Vector<Hashtable<String, Object>> overflow;

	public Leaf(Object MinX, Object MaxX, Object MinY, Object MaxY, Object MinZ, Object MaxZ) {
		super(MinX, MaxX, MinY, MaxY, MinZ, MaxZ);
		Bucket = new Vector<Hashtable<String, Object>>();
		overflow = new Vector<Hashtable<String, Object>>();

	}

	public Vector<Hashtable<String, Object>> getOverflow() {
		return overflow;
	}

	public void setOverflow(Vector<Hashtable<String, Object>> overflow) {
		this.overflow = overflow;
	}

	@Override
	public String toString() {
		return "Leaf [Bucket=" + Bucket + "]";
	}

	public Vector<Hashtable<String, Object>> getBucket() {
		return Bucket;
	}

	public void setBucket(Vector<Hashtable<String, Object>> bucket) {
		Bucket = bucket;
	}

	public boolean IsDup(Hashtable<String, Object> keyrec) {
		for (int i = 0; i < Bucket.size(); i++) {
			int n = 0;
			for (String key : this.getBucket().get(i).keySet()) {
				if (key.equals("Page Name"))
					continue;
				if (keyrec.get(key).equals(this.getBucket().get(i).get(key))) {
					n++;
				}

			}
			if (n == 3) {
				return true;
			}

		}
		return false;

	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setBeforeLeaf(Leaf beforeLeaf) {
		this.beforeLeaf = beforeLeaf;
	}

	public Leaf getAfterLeaf() {
		return afterLeaf;
	}

	public Leaf getBeforeLeaf() {
		return beforeLeaf;
	}

	public void setAfterLeaf(Leaf afterLeaf) {
		this.afterLeaf = afterLeaf;
	}

	public void removeFromBucket(Hashtable<String, Object> htbl) {
		for (int i = 0; i < Bucket.size(); i++) {
			if (Bucket.get(i).equals(htbl)) {
				Bucket.remove(i);
				i--;
			}
		}
	}

}
