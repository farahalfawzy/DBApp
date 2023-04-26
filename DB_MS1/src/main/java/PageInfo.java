import java.io.Serializable;

public class PageInfo implements Serializable{
	private String PageName;
	private int id;
	private Object max;
	private Object min;
	private int count;

	public PageInfo(String name, int id, Object max, Object min) {
		this.PageName = name;
		this.id = id;
		this.max = max;
		this.min = min;
		//this.count = 0;
	}

	public String getPageName() {
		return PageName;
	}

	public void setPageName(String pageName) {
		PageName = pageName;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Object getMax() {
		return max;
	}

	public void setMax(Object max) {
		this.max = max;
	}

	public Object getMin() {
		return min;
	}

	public void setMin(Object min) {
		this.min = min;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
