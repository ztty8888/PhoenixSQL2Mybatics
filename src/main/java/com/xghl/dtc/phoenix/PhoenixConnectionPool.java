package com.xghl.dtc.phoenix;

import org.apache.phoenix.jdbc.PhoenixConnection;


/**
 * @author luodf
 * @description:Phoenix连接池接口
 * @since 2017/06/12
 * 
 * 将Phoenix sql封装成类似mybatis。像只用mybatis一样使用Phoenix
 */
public interface PhoenixConnectionPool {
    public PhoenixConnection getConnection();

    public void returnConnection(PhoenixConnection conn);

    public void destroy();

}
