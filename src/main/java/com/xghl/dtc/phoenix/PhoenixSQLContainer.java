package com.xghl.dtc.phoenix;

import com.xghl.dtc.zeusthunder.phoenix.PhoenixSQLOperation;

import java.util.Map;

/**
 * @author luodf
 * @description:Phoenix  sql容器
 * @since 2017/8/2
 */
public class PhoenixSQLContainer {
    private Map<String, PhoenixSQLOperation> sqlMap;

    public Map<String, PhoenixSQLOperation> getSqlMap() {
        return sqlMap;
    }

    public void setSqlMap(Map<String, PhoenixSQLOperation> sqlMap) {
        this.sqlMap = sqlMap;
    }

    public PhoenixSQLOperation getPhoenixSQLOperation(String key) {
        return sqlMap.get(key);
    }

}   
