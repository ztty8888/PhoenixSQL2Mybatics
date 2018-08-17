package com.xghl.dtc.phoenix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.phoenix.jdbc.PhoenixConnection;

import com.google.common.collect.Maps;
import com.xghl.dtc.zeusthunder.utils.ObjectMapConverUtils;
import com.xghl.dtc.zeusthunder.utils.SysLog;

/**
 * @author luodf
 * @description:Phoenix操作工具
 * @since 2017/8/2
 */
public class PhoenixOperUtil {
//    public static final SysLogger SysLog = SysLoggerFactory.getSysLogger(PhoenixOperUtil.class);
    public static final int MAXBATCHSIZSE = 1000;

    public static int executeUpdate(String sqlKey,PhoenixConnection conn, String sql, Object... parameters)
            throws SQLException {
        List<Object> tempParas = null;
        if (parameters != null) {
            tempParas = Arrays.asList(parameters);
        }
        return executeUpdate( sqlKey,conn, sql, tempParas);
    }


    public static int batchExecuteUpdate(String sqlKey,PhoenixConnection conn, String sql, List<List<Object>> parameterList)
            throws SQLException {
//        SysLog.debug("批量执行SQL开始:" + sql);
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            int rowCount = 0;
            for (List<Object> parameters : parameterList) {
                setParameters(stmt, parameters);
                stmt.execute();
                rowCount++;
                if (rowCount % MAXBATCHSIZSE == 0) {
                    conn.commit();
//                    SysLog.debug("batch Rows upserted this commit : " + rowCount + "  sql:" + sql);
                }
            }

            conn.commit();
//            SysLog.debug("total Rows upserted: " + rowCount + "  sql: " + sql);
            return rowCount;
        } catch (Exception e) {
            SysLog.error(sqlKey+ " execute sql error!  sql: " + sql, e);
        } finally {
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return 0;
    }


    public static int executeUpdate(String sqlKey,PhoenixConnection conn, String sql, List<Object> parameters)
            throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        PreparedStatement stmt = null;

        int updateCount = 0;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            updateCount = stmt.executeUpdate();
            conn.commit();
        } catch (Exception e) {
        	e.printStackTrace();
            SysLog.error(sqlKey+ " execute sql error! sql:  " + sql, e);
        } finally {
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return updateCount;
    }


    public static boolean execute(String sqlKey,PhoenixConnection conn, String sql, Object... parameters)
            throws SQLException {
        List<Object> tempParas = null;
        if (parameters != null) {
            tempParas = Arrays.asList(parameters);
        }
        return execute(sqlKey,conn, sql, tempParas);
    }


    public static boolean execute(String sqlKey,PhoenixConnection conn, String sql, List<Object> parameters)
            throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        PreparedStatement stmt = null;
        boolean result = false;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            result = stmt.execute();
            conn.commit();
        } catch (Exception e) {
        	e.printStackTrace();
            SysLog.error(sqlKey+ " execute sql error! sql : " + sql+"  ", e);
        } finally {
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return result;
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    public static List<Object> executeQueryMore(String sqlKey,Map<String, PhoenixSQLPropertyInfo> resultClassHandledMapping, Class cls, PhoenixConnection conn, String sql,
                                                Object... parameters) throws SQLException {
        List<Object> tempParas = null;
        if (parameters != null) {
            tempParas = Arrays.asList(parameters);
        }

        List<Map<String, Object>> resultMapList = executeQuery(sqlKey,conn, sql, tempParas);
        if (resultMapList.isEmpty()) {
            return null;
        }
        List resultObjList = new ArrayList<>();
        for (Map<String, Object> map : resultMapList) {
            Object obj = makeMappingResult2Class(map, resultClassHandledMapping, cls);
            if (obj != null) {
                resultObjList.add(obj);
            }
        }

        if (resultMapList != null) {
            resultMapList.clear();
        }
        return resultObjList;
    }


    public static Object executeQueryForOne(String sqlKey,Map<String, PhoenixSQLPropertyInfo> resultClassHandledMapping, Class cls, PhoenixConnection conn, String sql,
                                            List<Object> parameters) throws SQLException {
        Map<String, Object> row = executeQueryOne(sqlKey,conn, sql, parameters);
        if (row == null) {
            return null;
        }
        return makeMappingResult2Class(row, resultClassHandledMapping, cls);
    }

    private static Object makeMappingResult2Class(Map<String, Object> row, Map<String, PhoenixSQLPropertyInfo> resultClassHandledMapping, Class cls) {
        if (row != null) {
            if (cls == java.lang.String.class || cls == java.lang.Integer.class || cls == java.lang.Long.class || cls == java.lang.Double.class || cls == java.lang.Float.class) {
                Set<Entry<String, Object>> entrySet = row.entrySet();
                for (Entry<String, Object> entry : entrySet) {
                    Object value = entry.getValue();
                    if (value instanceof Integer) {
                        return new Integer(String.valueOf(value));
                    } else if (value instanceof Long) {
                        return new Long(String.valueOf(value));
                    } else if (value instanceof Double) {
                        return new Double(String.valueOf(value));
                    } else if (value instanceof Float) {
                        return new Float(String.valueOf(value));
                    } else if (value instanceof String) {
                        return value;
                    }
                }
            }
            Map<String, Object> keyValueMap = new HashMap<String, Object>();
            Set<Entry<String, PhoenixSQLPropertyInfo>> entrySet = resultClassHandledMapping.entrySet();
            for (Entry<String, PhoenixSQLPropertyInfo> entry : entrySet) {
                Object value = row.get(entry.getKey());

                PhoenixSQLPropertyInfo pInfo = entry.getValue();

                if (value == null) {
                    keyValueMap.put(pInfo.getBeanKey(), value);
                } else {
                    if (value.getClass() == pInfo.getBeanClass()) {
                        keyValueMap.put(pInfo.getBeanKey(), value);
                    } else {
                        switch (pInfo.getBeanClassKey()) {
                            case "java.util.date":
                                java.sql.Date sqlDate = (java.sql.Date) value;
                                java.util.Date d = new java.util.Date(sqlDate.getTime());
                                keyValueMap.put(pInfo.getBeanKey(), d);
                                break;
                            default:
                                keyValueMap.put(pInfo.getBeanKey(), value);
                        }
                    }
                }

            }

            Object obj = ObjectMapConverUtils.mapToObject(keyValueMap, cls);
            keyValueMap.clear();
            return obj;
        }
        return null;
    }


    public static List<Map<String, Object>> executeQuery(String sqlKey,PhoenixConnection conn, String sql,
                                                         Object... parameters) throws SQLException {
        return executeQuery(sqlKey,conn, sql, Arrays.asList(parameters));
    }

    public static Map<String, Object> executeQueryOne(String sqlKey,PhoenixConnection conn, String sql,
                                                      List<Object> parameters) throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            rs = stmt.executeQuery();

            ResultSetMetaData rsMeta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    row.put(columName, value);
                }

                return row;
            }
        } catch (Exception e) {
        	e.printStackTrace();
            SysLog.error(sqlKey+ " execute sql error! sql: " + sql+"  ", e);
        } finally {
            close(rs);
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return null;
    }


    public static List<Map<String, Object>> executeQuery(String sqlKey,PhoenixConnection conn, String sql,
                                                         List<Object> parameters) throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            rs = stmt.executeQuery();

            ResultSetMetaData rsMeta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    row.put(columName, value);
                }

                rows.add(row);
            }
        } catch (Exception e) {
        	e.printStackTrace();
            SysLog.error(sqlKey+ " execute sql error! sql: " +sql+"  ", e);
        } finally {
            close(rs);
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return rows;
    }


    public static Map<String, Object> executeQueryReturnMap(String sqlKey,Class cls, PhoenixConnection conn, String sql,
                                                            String key, Object... parameters) throws SQLException {
        Map<String, Map<String, Object>> resultMapList = executeQueryReturnMap(sqlKey,conn, sql,
                key, parameters);


        Set<Entry<String, Map<String, Object>>> entrySet = resultMapList.entrySet();
        Map<String, Object> resultObjList = new HashMap<String, Object>();
        for (Entry<String, Map<String, Object>> mapEntry : entrySet) {
            Object obj = ObjectMapConverUtils.mapToObject(mapEntry.getValue(), cls);
            if (obj != null) {
                resultObjList.put(mapEntry.getKey(), obj);
            }

        }
        if (resultMapList != null) {
            resultMapList.clear();
        }
        return resultObjList;
    }


    public static Map<String, Map<String, Object>> executeQueryReturnMap(String sqlKey,PhoenixConnection conn, String sql,
                                                                         String key, Object... parameters) throws SQLException {
        List<Object> tempParas = null;
        if (parameters != null) {
            tempParas = Arrays.asList(parameters);
        }
        return executeQueryReturnMap(sqlKey,conn, sql, key, tempParas);
    }


    public static Map<String, Map<String, Object>> executeQueryReturnMap(String sqlKey,PhoenixConnection conn, String sql,
                                                                         String key, List<Object> parameters) throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        Map<String, Map<String, Object>> rows = Maps.newHashMap();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            rs = stmt.executeQuery();

            ResultSetMetaData rsMeta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    row.put(columName, value);
                }
                rows.put(rs.getString(key), row);
            }
        } catch (Exception e) {
            SysLog.error(sqlKey+ " execute sql error! sql: " + sql+"  ", e);
        } finally {
            close(rs);
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return rows;
    }


    public static Map<Object, Map<String, Object>> executeQueryMap(String sqlKey,String key, Connection conn, String sql,
                                                                   Object... parameters) throws SQLException {
        List<Object> tempParas = null;
        if (parameters != null) {
            tempParas = Arrays.asList(parameters);
        }
        return executeQueryMap(sqlKey,key, conn, sql, tempParas);
    }


    public static Map<Object, Map<String, Object>> executeQueryMap(String sqlKey,String key, Connection conn, String sql,
                                                                   List<Object> parameters) throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        Map<Object, Map<String, Object>> rows = new HashMap<Object, Map<String, Object>>();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            rs = stmt.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            Object keyToValue = null;
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    if (key.equalsIgnoreCase(columName)) {
                        keyToValue = value;
                    }
                    row.put(columName, value);
                }
                rows.put(keyToValue, row);
            }
        } catch (Exception e) {
        	e.printStackTrace();
            SysLog.error(sqlKey+ " execute sql error! sql: " + sql+"  ", e);
        } finally {
            close(rs);
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return rows;
    }


    public static Map<Object, Map<Object, Map<String, Object>>> executeQueryMap(String sqlKey,String key1, String key2,
                                                                                Connection conn, String sql, Object... parameters) throws SQLException {
        return executeQueryMap(sqlKey,key1, key2, conn, sql, Arrays.asList(parameters));
    }


    public static Map<Object, Map<Object, Map<String, Object>>> executeQueryMap(String sqlKey,String key1, String key2,
                                                                                Connection conn, String sql, List<Object> parameters) throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        Map<Object, Map<Object, Map<String, Object>>> rows = Maps.newHashMap();

        Map<Object, Map<String, Object>> record = null;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            rs = stmt.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            Object key1ToValue = null;
            Object key2ToValue = null;
            while (rs.next()) {
                Map<String, Object> row = Maps.newHashMap();
                for (int i = 0; i < rsMeta.getColumnCount(); ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    if (key1.equalsIgnoreCase(columName)) {
                        key1ToValue = value;
                    }
                    if (key2.equalsIgnoreCase(columName)) {
                        key2ToValue = value;
                    }
                    row.put(columName, value);
                }

                if (rows.containsKey(key1ToValue)) {
                    Map<Object, Map<String, Object>> data2 = rows.get(key1ToValue);
                    data2.put(key2ToValue, row);
                } else {
                    record = Maps.newHashMap();
                    record.put(key2ToValue, row);
                    rows.put(key1ToValue, record);
                }
            }
        } catch (Exception e) {
            SysLog.error(sqlKey+ " execute sql error! sql: " + sql+"  ", e);
        } finally {
            close(rs);
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return rows;
    }


    public static Map<Object, Map<String, Object>> executeQueryMap1(String sqlKey,String key1, String key2,
                                                                    Connection conn, String sql, Object... parameters) throws SQLException {
        return executeQueryMap1(sqlKey,key1, key2, conn, sql, Arrays.asList(parameters));
    }


    public static Map<Object, Map<String, Object>> executeQueryMap1(String sqlKey,String key1, String key2,
                                                                    Connection conn, String sql, List<Object> parameters) throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        Map<Object, Map<String, Object>> rows = Maps.newHashMap();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            rs = stmt.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            Object key1ToValue = null;
            Object key2ToValue = null;
            while (rs.next()) {
                Map<String, Object> row = Maps.newHashMap();
                for (int i = 0; i < rsMeta.getColumnCount(); ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    if (key1.equalsIgnoreCase(columName)) {
                        key1ToValue = value;
                    }
                    if (key2.equalsIgnoreCase(columName)) {
                        key2ToValue = value;
                    }
                    row.put(columName, value);
                }
                rows.put(key1ToValue + "" + key2ToValue, row);
            }
        } catch (Exception e) {
            SysLog.error(sqlKey+ " execute sql error! sql: " + sql+"  ", e);
        } finally {
            close(rs);
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);

        return rows;
    }


    private static void setParameters(PreparedStatement stmt, List<Object> parameters) throws SQLException {
        if (parameters != null) {
            int size = parameters.size();
            for (int i = 0; i < size; ++i) {
                Object param = parameters.get(i);
                stmt.setObject(i + 1, param);
            }
        }

    }


    public static void insertToTable(String sqlKey,PhoenixConnection conn, String tableName, Map<String, Object> data)
            throws SQLException {
        String sql = makeInsertToTableSql(sqlKey,tableName, data.keySet());
        List<Object> parameters = new ArrayList<Object>(data.values());
        execute(sqlKey,conn, sql, parameters);
    }


    public static String makeInsertToTableSql(String sqlKey,String tableName, Collection<String> names) {
        StringBuilder sql = new StringBuilder()
                .append("upsert into ")
                .append(tableName)
                .append("(");

        int nameCount = 0;
        for (String name : names) {
            if (nameCount > 0) {
                sql.append(",");
            }
            sql.append(name);
            nameCount++;
        }
        sql.append(") values (");
        for (int i = 0; i < nameCount; ++i) {
            if (i != 0) {
                sql.append(",");
            }
            sql.append("?");
        }
        sql.append(")");
        return sql.toString();
    }


    /**
     * 使用普通的Statement进行批量处理
     */
    public static int updateBatchStatement(String sqlKey,PhoenixConnection conn, List<String> sqls) throws SQLException {
//        SysLog.debug("执行SQL开始:" + sqls);
        Statement stmt = null;
        int num = 0;
        try {
            stmt = conn.createStatement();
            conn.setAutoCommit(false);
            for (int i = 0; i < sqls.size(); i++) {
                stmt.addBatch(sqls.get(i));

                if ((i + 1) % 100 == 0) {
                    num += sum(stmt.executeBatch());
                    stmt.clearBatch();
                    conn.commit();
                }
            }
            num += sum(stmt.executeBatch());
            conn.commit();
            conn.setAutoCommit(true);
        } catch (Exception e) {
        	e.printStackTrace();
            SysLog.error(sqlKey+ " execute sql error!   "+sqls, e);
            try {
                conn.rollback();
            } finally {
                close(stmt);
            }
        } finally {
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sqls);
        return num;
    }


    /**
     * 使用PreparedStatement进行批量处理
     */
    public static int updateBatchPreparedStatement(String sqlKey,PhoenixConnection conn, String sql, List<List> datas)
            throws SQLException {
        return updateBatchPreparedStatement(sqlKey,conn, sql, datas, MAXBATCHSIZSE);
    }


    /**
     * 使用PreparedStatement进行批量处理
     */
    public static int updateBatchPreparedStatement(String sqlKey,PhoenixConnection conn, String sql, List<List> datas,
                                                   int batchNum) throws SQLException {
//        SysLog.debug("执行SQL开始:" + sql);
        PreparedStatement stmt = null;
        int num = 0;
        try {
            stmt = conn.prepareStatement(sql);
            for (int i = 0; i < datas.size(); i++) {
                List data = (List) datas.get(i);
                for (int j = 0; j < data.size(); j++) {
                    stmt.setObject(j + 1, data.get(j));
                }
                stmt.addBatch();
                if ((i + 1) % batchNum == 0) {
                    stmt.executeBatch();
                    num += stmt.getUpdateCount();
                    stmt.clearBatch();
                    conn.commit();
                }
            }
            stmt.executeBatch();
            num += stmt.getUpdateCount();
            conn.commit();
        } catch (Exception e) {
        	e.printStackTrace();
            SysLog.error(sqlKey+ " execute sql error!  "+sql+"  ", e);
        } finally {
            close(stmt);
        }
//        SysLog.debug("执行SQL结束:" + sql);
        return num;
    }


    private static int sum(int[] arr) {
        int num = 0;
        for (int i = 0; i < arr.length; i++) {
            num += arr[i];
        }
        return num;
    }


    private final static void close(AutoCloseable x) {
        if (x != null) {
            try {
                x.close();
            } catch (Exception e) {
                SysLog.error("close error", e);
            }
        }
    }
}
