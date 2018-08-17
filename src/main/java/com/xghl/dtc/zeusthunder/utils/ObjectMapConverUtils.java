package com.xghl.dtc.zeusthunder.utils;

import java.util.Map;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;

public class ObjectMapConverUtils {

//    public static final Logger LOG = LoggerFactory.getLogger(SqlDateConverter.class);

    static {
        ConvertUtils.register(new DateConverter(null), java.util.Date.class);
    }

    public static Object mapToObject(Map<String, Object> map, Class<?> beanClass) {
        if (map == null) {
			return null;
		}
        Object obj = null;
        try {
            obj = beanClass.newInstance();
            if (obj != null) {
//				org.apache.commons.beanutils.BeanUtils.populate(obj, map);
                if (map.containsKey("priority")) {
                    Object t1 = map.get("priority");
                    if (t1 == null) {
                        map.put("priority", 0);
                    }

                }
                if (map.containsKey("page")) {
                    Object t1 = map.get("page");
                    if (t1 == null) {
                        map.put("page", 1);
                    }
                }
                if (map.containsKey("tempStatus")) {
                    Object t1 = map.get("tempStatus");
                    if (t1 == null) {
                        map.put("tempStatus", 0);
                    }
                }
                org.apache.commons.beanutils.BeanUtils.copyProperties(obj, map);
            }
        } catch (Exception e) {
            e.printStackTrace();
            SysLog.error("ObjectMapConverUtils mapToObject error!", e);
        }

        return obj;
    }

    public static Map<?, ?> objectToMap(Object obj) {
        if (obj == null) {
            return null;
        }
        Map<?, ?> map = null;

        try {
            map = new org.apache.commons.beanutils.BeanMap(obj);
        } catch (Exception e) {
            e.printStackTrace();
            SysLog.error("ObjectMapConverUtils objectToMap error!", e);
        }
        return map;
    }

}
