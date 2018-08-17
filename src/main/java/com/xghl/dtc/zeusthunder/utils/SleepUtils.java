package com.xghl.dtc.zeusthunder.utils;

public class SleepUtils {
	public static void sleep(long millons){
		try {
			Thread.currentThread().sleep(millons);
		} catch (InterruptedException e) {
			SysLog.error("线程休息失败~~~");
		}
	}
}
