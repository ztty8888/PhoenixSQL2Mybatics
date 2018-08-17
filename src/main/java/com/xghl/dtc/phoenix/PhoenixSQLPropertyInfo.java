package com.xghl.dtc.phoenix;

/**
 * @author luodf
 * @description:Phoenix对数据库操作xml配置属性信息
 * @since 2017/06/12
 */
public class PhoenixSQLPropertyInfo {
    private String dbKey;

    private String beanKey;
    private Class beanClass;

    private String beanClassKey;

    public String getBeanClassKey() {
        return beanClassKey;
    }

    public void setBeanClassKey(String beanClassKey) {
        this.beanClassKey = beanClassKey;
    }

    public PhoenixSQLPropertyInfo() {

    }

    public PhoenixSQLPropertyInfo(String dbKey, String beanKey, Class beanClass) {
        super();
        this.dbKey = dbKey;
        this.beanKey = beanKey;
        this.beanClass = beanClass;
    }

    public String getDbKey() {
        return dbKey;
    }

    public void setDbKey(String dbKey) {
        this.dbKey = dbKey;
    }

    public String getBeanKey() {
        return beanKey;
    }

    public void setBeanKey(String beanKey) {
        this.beanKey = beanKey;
    }

    public Class getBeanClass() {
        return beanClass;
    }

    public void setBeanClass(Class beanClass) {
        this.beanClass = beanClass;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dbKey == null) ? 0 : dbKey.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
			return true;
		}
        if (obj == null) {
			return false;
		}
        if (getClass() != obj.getClass()) {
			return false;
		}
        PhoenixSQLPropertyInfo other = (PhoenixSQLPropertyInfo) obj;
        if (dbKey == null) {
            if (other.dbKey != null) {
				return false;
			}
        } else if (!dbKey.equals(other.dbKey)) {
			return false;
		}
        return true;
    }

    @Override
    public String toString() {
        return "PhoenixSQLPropertyInfo [dbKey=" + dbKey + ", beanKey=" + beanKey + ", beanClass=" + beanClass + "]";
    }

}
