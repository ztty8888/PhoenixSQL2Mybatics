package com.xghl.dtc.phoenix;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.xghl.dtc.zeusthunder.utils.SysLog;


/**
 * @author luodf
 * @description:Phoenix连接池实现
 * @since 2017/06/12
 */
public class PhoenixConnectionPoolImpl implements PhoenixConnectionPool, InitializingBean, DisposableBean {

//    public static final SysLogger SysLog = SysLoggerFactory.getSysLogger(PhoenixConnectionPoolImpl.class);

    private int maxActive = GenericObjectPool.DEFAULT_MAX_ACTIVE;
    private int maxIdle = GenericObjectPool.DEFAULT_MAX_IDLE;
    private int minIdle = GenericObjectPool.DEFAULT_MIN_IDLE;
    private long maxWait = GenericObjectPool.DEFAULT_MAX_WAIT;

    private boolean testOnBorrow = GenericObjectPool.DEFAULT_TEST_ON_BORROW;
    private boolean testOnReturn = GenericObjectPool.DEFAULT_TEST_ON_RETURN;
    private boolean testWhileIdle = GenericObjectPool.DEFAULT_TEST_WHILE_IDLE;

    private ObjectPool<PhoenixConnection> objectPool = null;
    private PhoenixPoolableObjectFactory poolFactory;

    @Override
    public PhoenixConnection getConnection() {
        try {
            PhoenixConnection conn = (PhoenixConnection) objectPool.borrowObject();
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Phoenix pool getConnection fail", e);
        }

    }


    @Override
    public void returnConnection(PhoenixConnection conn) {
        try {
            objectPool.returnObject(conn);
        } catch (Exception e) {
            throw new RuntimeException("Phoenix pool returnConnection fail", e);
        }
    }


    @SuppressWarnings("deprecation")
    @Override
    public void afterPropertiesSet() throws Exception {
        // 对象池
        objectPool = new GenericObjectPool<PhoenixConnection>();
        ((GenericObjectPool<PhoenixConnection>) objectPool).setMaxActive(maxActive);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setMaxIdle(maxIdle);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setMinIdle(minIdle);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setMaxWait(maxWait);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setTestOnBorrow(testOnBorrow);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setTestOnReturn(testOnReturn);
        ((GenericObjectPool<PhoenixConnection>) objectPool).setTestWhileIdle(testWhileIdle);
        ((GenericObjectPool<PhoenixConnection>) objectPool)
                .setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);

        objectPool.setFactory(poolFactory);
    }


    @Override
    public void destroy() {
        try {
            objectPool.close();
            SysLog.warn("Phoenix driver connectionPool closed because server is shutting down");
        } catch (Exception e) {
            SysLog.info("Phoenix driver connectionPool close error", e);
        }
    }


    public int getMaxActive() {
        return maxActive;
    }


    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }


    public int getMaxIdle() {
        return maxIdle;
    }


    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }


    public int getMinIdle() {
        return minIdle;
    }


    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }


    public long getMaxWait() {
        return maxWait;
    }


    public void setMaxWait(long maxWait) {
        this.maxWait = maxWait;
    }


    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }


    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }


    public boolean isTestOnReturn() {
        return testOnReturn;
    }


    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }


    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }


    public void setTestWhileIdle(boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
    }


    public ObjectPool<PhoenixConnection> getObjectPool() {
        return objectPool;
    }


    public void setObjectPool(ObjectPool<PhoenixConnection> objectPool) {
        this.objectPool = objectPool;
    }

    public PhoenixPoolableObjectFactory getPoolFactory() {
        return poolFactory;
    }


    public void setPoolFactory(PhoenixPoolableObjectFactory poolFactory) {
        this.poolFactory = poolFactory;
    }
}
