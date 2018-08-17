package com.xghl.dtc.phoenix;

/**
 * @author luodf
 * @description:Phoenix 关键字sql关键字信息
 * @since 2017/06/12
 */
public class SQLContition implements java.lang.Comparable<SQLContition> {
	private String name;
	private int location=-1;
	private int prior=-100;
	private String sql;
	
	public SQLContition(String name,int location,int prior,String sql)
	{
		this.name=name;
		this.location=location;
		this.sql=sql;
		this.prior=prior;
	}
	
	public Boolean isContain()
	{
		return location>=0;
	}
	
	public SQLContition()
	{
		
	}

	

	@Override
	public String toString() {
		return "SQLContition [name=" + name + ", location=" + location + ", prior=" + prior + ", sql=" + sql + "]";
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getLocation() {
		return location;
	}

	public void setLocation(int location) {
		this.location = location;
	}

	public int getPrior() {
		return prior;
	}

	public void setPrior(int prior) {
		this.prior = prior;
	}

	@Override
	public int compareTo(SQLContition that) {
		if(that==null)
		{
			return 0;
		}
		int k= this.prior-that.getPrior();
		if(k>0)
		{
			return 1;
		}
		else 	if(k<0)
		{
			return -1;
		}
		return 0;
	}

}
