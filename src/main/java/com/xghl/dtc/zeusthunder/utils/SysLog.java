package com.xghl.dtc.zeusthunder.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xghl.dtc.zeusthunder.utils.IPUtils;

/**
 * @author luodf
 * @description: 系统log
 * @since 2017/09/15
 */
public class SysLog {
	private static Logger log = LoggerFactory.getLogger(SysLog.class);
    private static  String appName="";
    public static String machineIPAddr="127.0.0.1";
    public static String machineIPHOst="localhost";
    
    public static Logger  getLogger()
    {
    	return log;
    }
    
    public  static Boolean isErrorEnabled()
    {
    	return log.isErrorEnabled();
    }
    
    public static void setAppName(String appName)
    {
    	SysLog.appName=appName;
    	init();
    }
    
    public static void init()
    {
    	InetAddress	ipAdd=IPUtils.getLocalHostLANAddress();
    	if(ipAdd!=null)
    	{
    		try
    		{
    			machineIPAddr=ipAdd.getHostAddress();
        		machineIPHOst=ipAdd.getHostName();
    		}
    		catch(Exception e)
    		{
    			e.printStackTrace();
    		}
    	
    	}
    }
    
    
    
    /**
     * 打印调式
     * 
     * @param obj
     */
    public static void debug(String cumstoMsg) {
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
           
            
            location =machineIPAddr+"/"+machineIPHOst+"/"+appName+"/" + stacks[2].getClassName() + "/" + stacks[2].getMethodName()
                    + "/" + stacks[2].getLineNumber() + "/";
            
            if(cumstoMsg!=null)
            {
            	location=location+cumstoMsg+ " /";
            }
            log.debug(location +"#END#");
             
           
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 打印调式
     * 
     * @param obj
     */
    public static void debug(String cumstoMsg,Throwable obj) {
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
           
            
            location =machineIPAddr+"/"+machineIPHOst+"/"+appName+"/" + stacks[2].getClassName() + "/" + stacks[2].getMethodName()
                    + "/" + stacks[2].getLineNumber() + "/";
            
            if(cumstoMsg!=null)
            {
            	location=location+cumstoMsg+ " /";
            }
            
             if(obj!=null)
             {
                 StringWriter sw = new StringWriter();
                 obj.printStackTrace(new PrintWriter(sw, true));
                 sw.flush();
                 String str = sw.toString()+ "/";
                 sw.close();
                 log.debug(location + str+"#END#");
             }
             else
             {
            	 log.debug(location +"#END#");
             }
             
           
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 打印警告
     * 
     * @param obj
     */
    public static void warn(String cumstoMsg,Throwable obj) {
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
           
            
            location =machineIPAddr+"/"+machineIPHOst+"/"+appName+"/" + stacks[2].getClassName() + "/" + stacks[2].getMethodName()
                    + "/" + stacks[2].getLineNumber() + "/";
            
            if(cumstoMsg!=null)
            {
            	location=location+cumstoMsg+ "/";
            }
            
             if(obj!=null)
             {
                 StringWriter sw = new StringWriter();
                 obj.printStackTrace(new PrintWriter(sw, true));
                 sw.flush();
                 String str = sw.toString()+ "/";
                 sw.close();
                 log.warn(location + str+"#END#");
             }
             else
             {
            	 log.warn(location +"#END#");
             }
             
           
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
    /**
     * 打印警告
     * 
     * @param obj
     */
    public static void warn(String cumstoMsg) {
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
           
            
            location =machineIPAddr+"/"+machineIPHOst+"/"+appName+"/" + stacks[2].getClassName() + "/" + stacks[2].getMethodName()
                    + "/" + stacks[2].getLineNumber() + "/";
            
            if(cumstoMsg!=null)
            {
            	location=location+cumstoMsg+ "/";
            }
           
           log.warn(location +"#END#");
           
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    

    /**
     * 打印信息
     * 
     * @param obj
     */
    public static void info(String cumstoMsg,Throwable obj) {
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
            
            location =machineIPAddr+"/"+machineIPHOst+"/"+appName+"/" + stacks[2].getClassName() + "/" + stacks[2].getMethodName()
                    + "/" + stacks[2].getLineNumber() + "/";
            
            if(cumstoMsg!=null)
            {
            	location=location+cumstoMsg+ "/";
            }
            
             if(obj!=null)
             {
                 StringWriter sw = new StringWriter();
                 obj.printStackTrace(new PrintWriter(sw, true));
                 sw.flush();
                 String str = sw.toString()+ "/";
                 sw.close();
                 log.info(location + str+"#END#");
               
             }
             else
             {
                 log.info(location+"#END#");
             }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    /**
     * 打印信息
     * 
     * @param obj
     */
    public static void info(String cumstoMsg) {
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
            
            location =machineIPAddr+"/"+machineIPHOst+"/"+appName+"/" + stacks[2].getClassName() + "/" + stacks[2].getMethodName()
                    + "/" + stacks[2].getLineNumber() + "/";
            
            if(cumstoMsg!=null)
            {
            	location=location+cumstoMsg+ "/";
            }
            
             log.info(location+"#END#");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    

    /**
     * 打印错误
     * 
     * @param obj
     */
    public static void error(String cumstoMsg,Throwable obj) {
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
            
            location =machineIPAddr+"/"+machineIPHOst+"/"+appName+"/" + stacks[2].getClassName() + "/" + stacks[2].getMethodName()
                    + "/" + stacks[2].getLineNumber() + "/";
            
            if(cumstoMsg!=null)
            {
            	location=location+cumstoMsg+ "/";
            }
            
             if(obj!=null)
             {
                  StringWriter sw = new StringWriter();
                  obj.printStackTrace(new PrintWriter(sw, true));
                  sw.flush();
                  String str = sw.toString()+ "/";
                  sw.close();
                  log.error(location + str+"#END#");
             }
             else
             {
                 log.error(location+"#END#");
             }
           
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    

    /**
     * 打印错误
     * 
     * @param obj
     */
    public static void error(String cumstoMsg) {
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
            
            location =machineIPAddr+"/"+machineIPHOst+"/"+appName+"/" + stacks[2].getClassName() + "/" + stacks[2].getMethodName()
                    + "/" + stacks[2].getLineNumber() + "/";
            
            if(cumstoMsg!=null)
            {
            	location=location+cumstoMsg+ "/";
            }
            log.error(location+"#END#");
           
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
    /**
     * 获取调用此函数的代码的位置
     * @return 包名.类名.方法名(行数)
     */
    public static String getCodeLocation(){
        try{
            /*** 获取输出信息的代码的位置 ***/
            String location = "";
            StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
            location = stacks[2].getClassName() + "." + stacks[2].getMethodName()
                    + "(" + stacks[2].getLineNumber() + ")";
            return location;
        }catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}