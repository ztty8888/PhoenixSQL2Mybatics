package com.xghl.dtc.phoenix;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.taglibs.standard.extra.spath.Path;

import com.xghl.dtc.zeusthunder.utils.SysLog;


/**
 * @author luodf
 * @description:Phoenix连接池工厂
 * @since 2017/06/12
 */
public class PhoenixPoolableObjectFactory implements PoolableObjectFactory<PhoenixConnection> {
//    public static final SysLogger SysLog = SysLoggerFactory.getSysLogger(PhoenixPoolableObjectFactory.class);

    private String jdbcUrl;
    private int validateTimeout;
    private String tenantId;
    private String mutateMaxSize;
    private String hbaseConfFile;
    private String mutateBatchSize;


    @Override
    public PhoenixConnection makeObject() throws Exception {
        return genConn();
    }


    @Override
    public boolean validateObject(PhoenixConnection obj) {

        PhoenixConnection conn = (PhoenixConnection) obj;
        try {
            if (conn.isValid(validateTimeout)) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            SysLog.info("validateObject fail:", e);
        }
        return false;
    }


    @Override
    public void destroyObject(PhoenixConnection obj) throws Exception {
        if (obj instanceof PhoenixConnection) {
            PhoenixConnection conn = (PhoenixConnection) obj;
            if (!conn.isClosed()) {
                conn.close();
            }
        }
    }


    /**
     * 激活对象
     */
    @Override
    public void activateObject(PhoenixConnection conn) throws Exception {
        if (conn.isClosed() || !conn.isValid(validateTimeout)) {
			conn = genConn();
		}
    }


    /**
     * 钝化对象
     */
    @Override
    public void passivateObject(PhoenixConnection obj) throws Exception {

    }


    private PhoenixConnection genConn() {
        PhoenixConnection pconn = null;
        try {
            Properties connProps = new Properties();
            connProps.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");
            connProps.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "30000000");
            connProps.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "30000000");
            connProps.setProperty(QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB, "524288000");
            connProps.setProperty("hbase.rpc.timeout", "3000000");
            connProps.setProperty("ipc.socket.timeout","3000000");
            connProps.setProperty("hbase.client.operation.timeout","3000000");
            connProps.setProperty("hbase.client.scanner.timeout.period","30000000");
            
            //改变默认的500000                                             
//            connProps.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB,"50000000");
           
//          connProps.setProperty("hbase.client.scanner.timeout.period","300000");
//          connProps.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "1800000");
//          connProps.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "1800000");
//          connProps.setProperty(QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB, "524288000");
            
            /**
             * 客户端属性指定查询在客户端上超时之后的毫秒数。默认值是10分钟=600000。
             */
            connProps.setProperty("phoenix.query.timeoutMs","1500000");
            
            /**
             * 在执行UPSERT SELECT或DELETE语句期间，批处理和自动提交的行数。通过指定UpsertBatchSize属性值，可以在连接时重写此属性。
             * 请注意，当这些语句在服务器端完全执行时，连接属性值不会影响协处理器使用的批量大小。
             */
            connProps.setProperty("phoenix.mutate.batchSize","100000");
            
            /**
             * 	客户端在等待更多内存可用时将阻塞的最长时间。经过这段时间后，会引发InsufficientMemoryException。默认是10秒=10000。
             */
            connProps.setProperty("phoenix.query.maxGlobalMemoryWaitMs","20000");
            
            
            
            if (!StringUtils.isEmpty(tenantId)) {
				connProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
			}

            // 配置jdbc url,如果未配置或配置的为空，则从hbase.conf.file中获取jdbc url
            if (StringUtils.isEmpty(jdbcUrl)) {
                // 初始化HBaseConfiguration
                Configuration conf = HBaseConfiguration.create();
                conf.addResource(new Path(hbaseConfFile));
                jdbcUrl = QueryUtil.getConnectionUrl(connProps, conf);
                SysLog.warn("phoenix jdbc url:" + jdbcUrl);
            }
            // 创建phoenix连接
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            pconn = DriverManager.getConnection(jdbcUrl, connProps).unwrap(PhoenixConnection.class);
            SysLog.warn("获取Phoenix连接成功......");
            return pconn;

        } catch (Exception e) {
            SysLog.error("获取Phoenix连接失败:" + e.getMessage());
            SysLog.error("PhoenixPoolableObjectFactory makeObject fail,reason:", e);
            throw new RuntimeException(e);
        }
    }


    public String getJdbcUrl() {
        return jdbcUrl;
    }


    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }


    public int getValidateTimeout() {
        return validateTimeout;
    }


    public void setValidateTimeout(int validateTimeout) {
        this.validateTimeout = validateTimeout;
    }


    public String getTenantId() {
        return tenantId;
    }


    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }


    public String getMutateMaxSize() {
        return mutateMaxSize;
    }


    public void setMutateMaxSize(String mutateMaxSize) {
        this.mutateMaxSize = mutateMaxSize;
    }


    public String getHbaseConfFile() {
        return hbaseConfFile;
    }


    public void setHbaseConfFile(String hbaseConfFile) {
        this.hbaseConfFile = hbaseConfFile;
    }


    public String getMutateBatchSize() {
        return mutateBatchSize;
    }


    public void setMutateBatchSize(String mutateBatchSize) {
        this.mutateBatchSize = mutateBatchSize;
    }


}