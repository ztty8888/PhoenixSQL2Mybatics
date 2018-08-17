package com.xghl.dtc.phoenix;

import org.apache.phoenix.jdbc.PhoenixConnection;

import com.xghl.dtc.zeusthunder.utils.SysLog;


/**
 * @author luodf
 * @description:Phoenix连接管理
 * @since 2017/06/12
 */
public class PhoenixConnectionManager {

//    public static final SysLogger SysLog = SysLoggerFactory.getSysLogger(PhoenixConnectionManager.class);

    public ThreadLocal<PhoenixConnection> socketThreadSafe = new ThreadLocal<PhoenixConnection>();

    public PhoenixConnectionPool connectionPoolBean;


    /**
     * 获得连接
     */
    public PhoenixConnection getConn() {
        PhoenixConnection conn = null;
        try {
            conn = connectionPoolBean.getConnection();
            socketThreadSafe.set(conn);
            return socketThreadSafe.get();
        } catch (Exception e) {
            SysLog.error("ConnectionManager get phoenix connection fail", e);
        }
        return conn;
    }

    /**
     * 释放连接
     */
    public void returnConnection(PhoenixConnection conn) {
        connectionPoolBean.returnConnection(conn);
        socketThreadSafe.remove();
    }

    public PhoenixConnectionPool getConnectionPoolBean() {
        return connectionPoolBean;
    }

    public void setConnectionPoolBean(PhoenixConnectionPool connectionPoolBean) {
        this.connectionPoolBean = connectionPoolBean;
    }

}