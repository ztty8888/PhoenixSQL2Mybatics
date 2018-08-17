package com.xghl.dtc.zeusthunder.utils;

import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * @author luodf
 * @description:对象池
 * @since 2017/10/20
 */
public class ObjectSimplePool {
	private static int maxThreadSize=60;
	private  static  ConcurrentLinkedQueue<SimpleDateFormat>  dateFormatQueue=new ConcurrentLinkedQueue<SimpleDateFormat> ();
	
	private static ConcurrentLinkedQueue<SeqComparator>  seqComparatorQueue=new ConcurrentLinkedQueue<SeqComparator> ();
	
	public static SeqComparator borrowSeqComparator()
	{
		SeqComparator  comparator =	seqComparatorQueue.poll();
		if(comparator==null)
		{
			comparator = new SeqComparator();
		}
		return comparator;
	}
	
	public static void returnSeqComparator (SeqComparator  comparator )
	{
		if(seqComparatorQueue.size()<maxThreadSize)
		{
			seqComparatorQueue.add(comparator);
		}
	}
	
	public static SimpleDateFormat borrowSimpleDateFormat()
	{
		SimpleDateFormat  dateFormat=	dateFormatQueue.poll();
		if(dateFormat==null)
		{
			dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		}
		return dateFormat;
	}
	
	public static void returnSimpleDateFormat(SimpleDateFormat  dateFormat)
	{
		if(dateFormatQueue.size()<maxThreadSize)
		{
			dateFormatQueue.add(dateFormat);
		}
	}
  
}
