package com.xghl.dtc.phoenix;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.SqlDateConverter;
import org.apache.commons.beanutils.converters.SqlTimestampConverter;
import org.apache.commons.pool.ObjectPool;
import org.apache.phoenix.jdbc.PhoenixConnection;

import com.xghl.dtc.zeusthunder.utils.ObjectMapConverUtils;
import com.xghl.dtc.zeusthunder.utils.SysLog;

/**
 * @author luodf
 * @description:phoenix操作dao
 * @since 2017/07/20
 */
public abstract class BasePhoenixDAO {

    //    public static final SysLogger SysLog = SysLoggerFactory.getSysLogger(BasePhoenixDAO.class);
    public static final TimeZone TIMEZONE = TimeZone.getTimeZone("Asia/Shanghai");
    protected PhoenixConnectionManager phoenixConnectionManager;
    public final static String FORMAT_DATETIME = "yyyy-MM-dd HH:mm:ss";
    protected String tableName;

    private int tryCount = 10;
    private String areaName;
    private Boolean tableHasAreaNameColumn = false;
    private Boolean useAreaName = false;
    private List<PhoenixSQLOperation> sqlList;

    private Map<String, PhoenixSQLOperation> sqlMap;

    private int batchLength = 3000;

    protected     ObjectPool   objectPool ;

    /**
      * 注册转换类
     */
    static {
        ConvertUtils.register(new SqlDateConverter(null), java.sql.Date.class);
        ConvertUtils.register(new SqlTimestampConverter(null), java.sql.Timestamp.class);
    }

    public void setPhoenixConnectionManager(PhoenixConnectionManager phoenixConnectionManager) {
        this.phoenixConnectionManager = phoenixConnectionManager;
    }

    /**
     * 初始化消息
     */
    public PhoenixConnection getPhoenixConnection() {
        for (int i = 0; i < tryCount; i++) {
            PhoenixConnection conn = phoenixConnectionManager.getConn();
            if (conn != null) {
                return conn;
            }
        }
        return null;
    }

    /**
     * 初始化配置消息
     */
    public void init() {
        sqlMap = new HashMap<String, PhoenixSQLOperation>();
        for (PhoenixSQLOperation sql : sqlList) {
            sql.setAreaName(areaName);
            sql.setTableHasAreaNameColumn(tableHasAreaNameColumn);
            sql.setUseAreaName(useAreaName);
            if (sql.isOKSQL()) {
                sql.init();
                sql.getParsedParameters();
                sql.formatSQL();
                sql.formatBatchQuery();
                sqlMap.put(sql.getKey(), sql);

            } else {
                SysLog.error("PhoenixSQLOperation  check error!  " + sql, null);
            }
        }
        TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
        TimeZone.setDefault(tz);
        System.setProperty("user.timezone", "Asia/Shanghai");
    }


    /**
      * 查询一行数据
     */
    public Object queryOneObject(Object bean, String sqlKey) {

        PhoenixSQLOperation sqlOperation = this.sqlMap.get(sqlKey);
        if (sqlOperation == null) {
            SysLog.error("SQLOperation not exist! sqlKey : " + sqlKey);
//            closeConnection(conn);
            return null;
        }

        if (!sqlOperation.isOKSQL()) {
            SysLog.error("SQLOperation parameters error! Detail sql:   " + sqlOperation);
            return null;
        }

        String sql = sqlOperation.getSql();

        Class cls = sqlOperation.getResultClassCls();
        Map<String, PhoenixSQLPropertyInfo> resultClassHandledMapping = sqlOperation.getResultClassHandledMapping();

        List<Object> parameters = new ArrayList<Object>();
        if (sqlOperation.getHasParameter()) {
            if (bean != null) {
                List<String> parsedParameters = sqlOperation.getParsedParameters();
                this.objectToSQLValueList(bean, parsedParameters, parameters, sqlOperation);
            }

        }
        Object[] array = null;
        if (parameters.isEmpty()) {
            array = null;
        } else {
            array = parameters.toArray(new Object[parameters.size()]);
        }
        PhoenixConnection conn = this.getPhoenixConnection();
        try {
            return PhoenixOperUtil.executeQueryForOne(sqlOperation.getKey(), resultClassHandledMapping, cls, conn, sql, parameters);
        } catch (Exception e) {
            e.printStackTrace();
            SysLog.error("sql key: " + sqlOperation.getKey() + " error !BasePhoenixDAO queryOneObject  error!", e);
        } finally {
            closeConnection(conn);
            parameters.clear();
        }
        return null;
    }


    public List batchQueryObject(Object bean, String sqlKey) {
        PhoenixSQLOperation sqlOperation = this.sqlMap.get(sqlKey);
        if (sqlOperation == null) {
            SysLog.error("SQLOperation not exist! sqlKey : " + sqlKey);
//            closeConnection(conn);
            return null;
        }
        if (!sqlOperation.isOKSQL()) {
            SysLog.error("SQLOperation parameters error! Detail sql:   " + sqlOperation);
            return null;
        }
        String sql = sqlOperation.getBatchQueryIdSQL();
        List<String> ids = batchQuery(sql, bean, String.class, sqlOperation, true);

        String queryObjectSQL = sqlOperation.getBatchQueryObjectSQL();
        if (ids != null && !ids.isEmpty()) {
            int idLen = ids.size();
            List resultList = new ArrayList();
            StringBuilder queryStr = new StringBuilder();
            List<String> subIdList = new ArrayList<>();
            for (int i = idLen - 1; i >= 0; i--) {
                subIdList.add(ids.get(i));
                if (i % batchLength == 0 || i == 0) {
                    int idLength = subIdList.size();
                    for (int j = 0; j < idLength; j++) {
                        if (j < idLength - 1) {
                            queryStr.append("'");
                            queryStr.append(subIdList.get(j));
                            queryStr.append("' ,");
                        } else {
                            queryStr.append("'");
                            queryStr.append(subIdList.get(j));
                            queryStr.append("'");
                        }
                    }
                    String queryIds = queryObjectSQL.replace("#tableName#", tableName)
                            .replace("#queryArray#", queryStr.toString());
                    List batchList = batchQuery(queryIds, bean, sqlOperation.getResultClassCls(), sqlOperation, false);
                    if (batchList != null && !batchList.isEmpty()) {
                        resultList.addAll(batchList);
                        queryStr.setLength(0);
                    }
                    subIdList.clear();
                }
            }

//            for (int i = 0; i <numberTime ; i++) {
//                if(i==numberTime-1){
//                       subIdList  = ids.subList(0,ids.size()-(batchLength*i));
//                }else {
//                       subIdList  = ids.subList(0,batchLength);
//                }
//
//
//
//            }

            return resultList;

        } else {
            return null;
        }
    }

    /**
     * 查询多行数据
     */
    public List<Object> queryMoreObject(Object bean, String sqlKey) {

        PhoenixSQLOperation sqlOperation = this.sqlMap.get(sqlKey);
        if (sqlOperation == null) {
            SysLog.error("SQLOperation not exist! sqlKey : " + sqlKey, null);
//            closeConnection(conn);
            return null;
        }

        if (!sqlOperation.isOKSQL()) {
            SysLog.error("SQLOperation parameters error! Detail sql:   " + sqlOperation, null);
//            closeConnection(conn);
            return null;
        }

        String sql = sqlOperation.getSql();

        Class cls = sqlOperation.getResultClassCls();
        Map<String, PhoenixSQLPropertyInfo> resultClassHandledMapping = sqlOperation.getResultClassHandledMapping();
        List<Object> parameters = new ArrayList<Object>();
        if (sqlOperation.getHasParameter()) {
            if (bean != null) {
                List<String> parsedParameters = sqlOperation.getParsedParameters();
                this.objectToSQLValueList(bean, parsedParameters, parameters, sqlOperation);
            }
        }

        Object[] array = null;
        if (parameters.isEmpty()) {
            array = null;
        } else {
            array = parameters.toArray(new Object[parameters.size()]);
        }
        
        PhoenixConnection conn = this.getPhoenixConnection();
        try {
            return PhoenixOperUtil.executeQueryMore(sqlOperation.getKey(), resultClassHandledMapping, cls, conn, sql, array);
        } catch (Exception e) {
            e.printStackTrace();
            SysLog.error("sql key: " + sqlOperation.getKey() + " error ! BasePhoenixDAO queryOneObject  error!", e);
        } finally {
            closeConnection(conn);
            parameters.clear();
        }
        return null;
    }


    public List batchQuery(String sql, Object bean, Class cls, PhoenixSQLOperation sqlOperation, boolean isHasePaeameter) {
        Map<String, PhoenixSQLPropertyInfo> resultClassHandledMapping = sqlOperation.getResultClassHandledMapping();
        List<Object> parameters = new ArrayList<Object>();
        if (sqlOperation.getHasParameter()) {
            if (bean != null) {
                List<String> parsedParameters = sqlOperation.getParsedParameters();
                this.objectToSQLValueList(bean, parsedParameters, parameters, sqlOperation);
            }
        }
        Object[] array = null;
        if (isHasePaeameter) {
            if (parameters.isEmpty()) {
                array = null;
            } else {
                array = parameters.toArray(new Object[parameters.size()]);
            }
        } else {

            array = null;

        }
        PhoenixConnection conn = this.getPhoenixConnection();
        try {
            return PhoenixOperUtil.executeQueryMore(sqlOperation.getKey(), resultClassHandledMapping, cls, conn, sql, array);
        } catch (Exception e) {
            e.printStackTrace();
            SysLog.error("sql key: " + sqlOperation.getKey() + " error ! BasePhoenixDAO queryOneObject  error!", e);
        } finally {
            closeConnection(conn);
            parameters.clear();
        }
        return null;
    }


    /**
     * 批量新增或修改删除
     */
    public Boolean batchUpSertDelObject(List beanList, String sqlKey) {
        PhoenixSQLOperation sqlOperation = this.sqlMap.get(sqlKey);
        if (sqlOperation == null) {
            SysLog.error("batchUpSertDelObject SQLOperation not exist! sqlKey : " + sqlKey);
            return false;
        }

        if (!sqlOperation.isOKSQL()) {
            SysLog.error("batchUpSertDelObject SQLOperation parameters error! Detail sql:   " + sqlOperation);
            return false;
        }

        String sql = sqlOperation.getSql();
        sql = sql.toUpperCase();
        List<List<Object>> parameterList = new ArrayList<List<Object>>();
        for (Object bean : beanList) {
            List<Object> parameters = new ArrayList<Object>();
            if (sqlOperation.getHasParameter()) {
                if (bean != null) {
                    List<String> parsedParameters = sqlOperation.getParsedParameters();
                    this.objectToSQLValueList(bean, parsedParameters, parameters, sqlOperation);
                }
            }
            parameterList.add(parameters);
        }

        PhoenixConnection conn = this.getPhoenixConnection();
        try {
            int count = PhoenixOperUtil.batchExecuteUpdate(sqlOperation.getKey(), conn, sql, parameterList);
            if (count > 0) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            SysLog.error("sql key: " + sqlOperation.getKey() + " error !BasePhoenixDAO batchUpSertDelObject  error!", e);
        } finally {
            closeConnection(conn);
            for (List<Object> list : parameterList) {
                list.clear();
            }
            parameterList.clear();

        }
        return false;
    }

    /**
     * 单行新增或修改删除
     */
    public Boolean upSertDelObject(Object bean, String sqlKey) {

        PhoenixSQLOperation sqlOperation = this.sqlMap.get(sqlKey);
        if (sqlOperation == null) {
            SysLog.error("upsertCommonObject SQLOperation not exist! sqlKey : " + sqlKey);
            return false;
        }

        if (!sqlOperation.isOKSQL()) {
            SysLog.error("upsertCommonObject SQLOperation parameters error! Detail sql:   " + sqlOperation);
            return false;
        }

        String sql = sqlOperation.getSql();
        sql = sql.toUpperCase();
        List<Object> parameters = new ArrayList<Object>();
        if (sqlOperation.getHasParameter()) {
            if (bean != null) {
                List<String> parsedParameters = sqlOperation.getParsedParameters();
                this.objectToSQLValueList(bean, parsedParameters, parameters, sqlOperation);
            }

        }
        PhoenixConnection conn = this.getPhoenixConnection();
        try {
            int count = PhoenixOperUtil.executeUpdate(sqlOperation.getKey(), conn, sql, parameters);
            if (count > 0) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            SysLog.error("sql key: " + sqlOperation.getKey() + " error ! BasePhoenixDAO upsertCommonObject  error! sql: " + sql, e);
        } finally {
            closeConnection(conn);
            parameters.clear();

        }
        return false;
    }

    private void closeConnection(PhoenixConnection conn) {
        if (conn != null) {
            this.returnPhoenixConnection(conn);
        }
    }


    /**
     * 转换object对象为map list
     */
    public void objectToSQLValueList(Object bean, List<String> parsedParameters, List<Object> sqlValueParameters, PhoenixSQLOperation sqlOperation) {
        int size = parsedParameters.size();
        Map<String, Object> data = null;
        if (bean instanceof Map) {
            data = (Map<String, Object>) bean;
        } else {
            data = (Map<String, Object>) ObjectMapConverUtils.objectToMap(bean);
        }


        for (int i = 0; i < size; i++) {
            String key = parsedParameters.get(i);
            if (key != null) {
                String realKey = key;
                if (key.contains(".")) {
                    int k = key.indexOf(".");
                    realKey = key.substring(k + 1);
                }
                Object value = data.get(realKey);
                if (value != null && value instanceof java.util.Date) {
                    java.util.Date d1 = (java.util.Date) value;
                    java.sql.Date sqlDate = new java.sql.Date(d1.getTime());

                    sqlValueParameters.add(sqlDate);


//					SimpleDateFormat  sf=new SimpleDateFormat(FORMAT_DATETIME);
//				    String dtStr=	sf.format(d1);
//				    String dateValue=" TO_DATE( '"+dtStr+"', '"+FORMAT_DATETIME+"' )  ";
//				    map.put(key, dateValue);

                } else {
//					map.put(key, value);

                    sqlValueParameters.add(value);
                }

            }
        }

        if (useAreaName && sqlOperation.getCheckAreaNameOK()) {
            sqlValueParameters.add(areaName);
        }

        data.clear();
    }


    public Map<String, Object> objectToSQLMap(Object bean, List<String> parsedParameters) {
        int size = parsedParameters.size();
        Map<String, Object> data = (Map<String, Object>) ObjectMapConverUtils.objectToMap(bean);
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i < size; i++) {
            String key = parsedParameters.get(i);
            if (key != null) {
                String realKey = key;
                if (key.contains(".")) {
                    int k = key.indexOf(".");
                    realKey = key.substring(k + 1);
                }
                Object value = data.get(realKey);
                if (value != null && value instanceof java.util.Date) {
                    java.util.Date d1 = (java.util.Date) value;
//					java.sql.Date sqlDate = new java.sql.Date(d1.getTime());
//					map.put(key, sqlDate);


                    SimpleDateFormat sf = new SimpleDateFormat(FORMAT_DATETIME);
                    String dtStr = sf.format(d1);
                    String dateValue = " TO_DATE( '" + dtStr + "', '" + FORMAT_DATETIME + "' )  ";
                    map.put(key, dateValue);

                } else {
                    map.put(key, value);
                }

            }
        }
        // Set<Entry<String, Object>> entrySet= data.entrySet();
        // for(Entry<String, Object> entry : entrySet)
        // {
        // if(!(entry.getKey() instanceof String))
        // {
        // data.remove(entry.getKey());
        // continue;
        // }
        // Object value=entry.getValue();
        // if(value!=null&& value instanceof java.util.Date)
        // {
        // java.util.Date d1=(java.util.Date) value;
        // java.sql.Date sqlDate=new java.sql.Date(d1.getTime());
        // data.put(entry.getKey(), sqlDate);
        // }
        // }
        // Map<String, Object> map=new HashMap<String, Object> ();
        // map.putAll(data);
        data.clear();
        return map;
    }


    public void setTableHasAreaNameColumn(Boolean tableHasAreaNameColumn) {
        this.tableHasAreaNameColumn = tableHasAreaNameColumn;
    }


    public void setAreaName(String areaName) {
        this.areaName = areaName;
    }


    public void setUseAreaName(Boolean useAreaName) {
        this.useAreaName = useAreaName;
    }

    public void returnPhoenixConnection(PhoenixConnection conn) {
        this.phoenixConnectionManager.returnConnection(conn);
    }


    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public void setSqlList(List<PhoenixSQLOperation> sqlList) {
        this.sqlList = sqlList;
    }

    public void setObjectPool(ObjectPool objectPool) {
        this.objectPool = objectPool;
    }
}
