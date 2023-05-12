import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table extends Vector implements Serializable {

	private String TableName;
	private String ClusteringKey;
	private Hashtable<String, String> ColNameType;
	private Hashtable<String, String> ColNameMin;
	private Hashtable<String, String> ColNameMax;
	private int currentMaxId=-1;
	private Vector<PageInfo>  PageInfo=new Vector<PageInfo>();
	private Vector <String> index=new Vector<>();
	private boolean indexOnClustKey;
	private Vector<Hashtable<String,Object>> indexOnCol = new Vector<>();
	

	public Table(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {

		TableName = tableName;
		ClusteringKey = clusteringKey;
		ColNameType = colNameType;
		ColNameMin = colNameMin;
		ColNameMax = colNameMax;
	}

	public String getTableName() {
		return TableName;
	}

	public void setTableName(String tableName) {
		TableName = tableName;
	}

	public String getClusteringKey() {
		return ClusteringKey;
	}

	public void setClusteringKey(String clusteringKey) {
		ClusteringKey = clusteringKey;
	}

	public Hashtable<String, String> getColNameType() {
		return ColNameType;
	}

	public void setColNameType(Hashtable<String, String> colNameType) {
		ColNameType = colNameType;
	}

	public Hashtable<String, String> getColNameMin() {
		return ColNameMin;
	}

	public void setColNameMin(Hashtable<String, String> colNameMin) {
		ColNameMin = colNameMin;
	}

	public Hashtable<String, String> getColNameMax() {
		return ColNameMax;
	}

	public void setColNameMax(Hashtable<String, String> colNameMax) {
		ColNameMax = colNameMax;
	}
	public int getCurrentMaxId() {
		return currentMaxId;
	}

	public void setCurrentMaxId(int currentMaxId) {
		this.currentMaxId = currentMaxId;
	}

	public Vector<PageInfo> getPageInfo() {
		return PageInfo;
	}

	public void setPageInfo(Vector<PageInfo> pageInfo) {
		PageInfo = pageInfo;
	}

	public Vector<String> getIndex() {
		return index;
	}

	public void setIndex(Vector<String> index) {
		this.index = index;
	}
	public boolean isIndexOnClustKey() {
		return indexOnClustKey;
	}

	public void setIndexOnClustKey(boolean indexOnClustKey) {
		this.indexOnClustKey = indexOnClustKey;
	}

	public Vector<Hashtable<String, Object>> getIndexOnCol() {
		return indexOnCol;
	}

	public void setIndexOnCol(Vector<Hashtable<String, Object>> indexOnCol) {
		this.indexOnCol = indexOnCol;
	}
	
	
}