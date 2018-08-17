package com.xghl.dtc.zeusthunder.utils;

import java.util.Comparator;

import com.xghl.dtc.zeuscommon.bean.ISeqBean;

public class SeqComparator  implements  Comparator<ISeqBean>
{
	@Override
	public int compare(ISeqBean o1, ISeqBean o2) {
		if(o1==null||o2==null)
		{
			return 0;
		}
		
		if(o1.getSeq()>o2.getSeq())
		{
			return 1;
		}
		if(o1.getSeq()<o2.getSeq())
		{
			return -1;
		}
		return 0;
	}
	
}
