package com.xghl.dtc.phoenix;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.xghl.dtc.zeusthunder.utils.SysLog;

/**
 * @author luodf
 * @description:Phoenix对数据库操作xml配置
 * @since 2017/06/12
 */
public class PhoenixSQLOperation {
//    public static final SysLogger SysLog = SysLoggerFactory.getSysLogger(PhoenixSQLOperation.class);
   
    public static final String SQLWHERE="where";
    
    public static final String SQLGROUP="group";
    public static final String SQLLIMIT="limit";
   
    public static final String SQLORDER="order";
    public static final String SQLID="id";
    
    /**
     * 区域名称
     */
    private String areaName;
    private String primeKey=SQLID;
    
    /**
     * 服务器是否启用使用areaName，对整个工程配置。
     */
    private Boolean useAreaName=false;
    
    /**
     * select语句里面是否使用id查询，如果使用id查询则不需要加上areaName
     */
    private Boolean selectUseId=false;
    
    /**
     * 表是否有areaName列
     */
    private Boolean tableHasAreaNameColumn=false;
    
    /**
     * 本sql语句是否使用areaName列查询。只有useAreaName=true &&  useAreaNameForSingleSQL=true情况下
     * 才会在sql 后面追加  areaName = ?
     */
    private Boolean useAreaNameForSingleSQL=false;

	private String key;
    private String operaType;
    private String resultClass;
    private String sql;
    private String parameters;
    private Boolean hasParameter = true;
    private Boolean batchQuery = false;
    private Map<String, String> resultClassMapping;
    private Map<String, PhoenixSQLPropertyInfo> resultClassHandledMapping;

    private List<String> parsedParameters;
    private volatile boolean inited = false;
    private volatile boolean cheked = false;

    private boolean isSQLOK = false;
    private Class resultClassCls;
    private static final Pattern EMPTYPATTERN = Pattern.compile("\r|\n|\t");
    
    private Boolean checkAreaNameOK=false;

    public void setBatchQuery(Boolean batchQuery) {
        this.batchQuery = batchQuery;
    }
    private String batchQueryIdSQL;

    private String batchQueryObjectSQL;

    public Boolean getCheckAreaNameOK() {
		return checkAreaNameOK;
	}

	public void setCheckAreaNameOK(Boolean checkAreaNameOK) {
		this.checkAreaNameOK = checkAreaNameOK;
	}

	public Boolean getTableHasAreaNameColumn() {
		return tableHasAreaNameColumn;
	}

	public void setTableHasAreaNameColumn(Boolean tableHasAreaNameColumn) {
		this.tableHasAreaNameColumn = tableHasAreaNameColumn;
	}
	
	public Boolean getUseAreaNameForSingleSQL() {
		return useAreaNameForSingleSQL;
	}

	public void setUseAreaNameForSingleSQL(Boolean useAreaNameForSingleSQL) {
		this.useAreaNameForSingleSQL = useAreaNameForSingleSQL;
	}
    
    public Boolean getSelectUseId() {
		return selectUseId;
	}

	public void setSelectUseId(Boolean selectUseId) {
		this.selectUseId = selectUseId;
	}

	
    public String getPrimeKey() {
		return primeKey;
	}

	public void setPrimeKey(String primeKey) {
		this.primeKey = primeKey;
	}
	
  
    public Boolean getUseAreaName() {
		return useAreaName;
	}

	public void setUseAreaName(Boolean useAreaName) {
		this.useAreaName = useAreaName;
	}

	public String getAreaName() {
  		return areaName;
  	}

  	public void setAreaName(String areaName) {
  		this.areaName = areaName;
  	}

    public Map<String, PhoenixSQLPropertyInfo> getResultClassHandledMapping() {
        return resultClassHandledMapping;
    }



    public void init() {
        resultClassHandledMapping = new HashMap<String, PhoenixSQLPropertyInfo>();
        if (resultClassMapping != null && !resultClassMapping.isEmpty()) {
            Set<Entry<String, String>> entrySet = resultClassMapping.entrySet();
            for (Entry<String, String> entry : entrySet) {
                PhoenixSQLPropertyInfo pInfo = new PhoenixSQLPropertyInfo();
                pInfo.setDbKey(entry.getKey());
                String value = entry.getValue();
                String[] clsVs = value.split(":");
                String beanKey = clsVs[0].trim();
                pInfo.setBeanKey(beanKey);
                resultClassHandledMapping.put(entry.getKey(), pInfo);
                Class beanClass = String.class;
                if (clsVs != null && clsVs.length == 2) {
                    String classStr = clsVs[1].trim().toLowerCase();
                    pInfo.setBeanClassKey(classStr);
                    switch (classStr) {
                        case "string":
                            beanClass = String.class;
                            break;
                        case "int":
                            beanClass = Integer.class;
                            break;

                        case "long":
                            beanClass = Long.class;
                            break;
                        case "double":
                            beanClass = Double.class;
                            break;
                        case "float":
                            beanClass = Float.class;
                            break;

                        case "java.util.date":
                            beanClass = java.util.Date.class;
                            break;
                        case "java.sql.date":
                            beanClass = java.sql.Date.class;
                            break;

                        case "short":
                            beanClass = Short.class;
                            break;
                        case "byte":
                            beanClass = Byte.class;
                            break;
                        case "boolean":
                            beanClass = Boolean.class;
                            break;

                        default:
                            beanClass = String.class;
                    }
                } else {
                    pInfo.setBeanClassKey("string");
                }
                pInfo.setBeanClass(beanClass);
            }
            
          
        }



    }

    public  void formatBatchQuery(){

        if(batchQuery){

        String batchQuerySql = sql;

        if(!sql.contains("limit")){
            SysLog.error(" limit is empty! \n\r  "+sql);
        }

        batchQuerySql.split("from");
        String[]  sqlStrs =   batchQuerySql.split("from");

        if(sqlStrs!=null){
            String batchQueryIdSQL  = "select "+ primeKey + "  from " + sqlStrs[1];
            String batchQueryObjectSQL  =   sqlStrs[0] + " from   #tableName#  where " + primeKey + "  in ( #queryArray# )";
            setBatchQueryIdSQL(batchQueryIdSQL);
            setBatchQueryObjectSQL(batchQueryObjectSQL);
        }else {
            SysLog.error(" sqlStrs is empty ! \n\r  "+sql);

        }
        }
    }
    public Map<String, String> getResultClassMapping() {
        return resultClassMapping;
    }

    public void setResultClassMapping(Map<String, String> resultClassMapping) {
        this.resultClassMapping = resultClassMapping;
    }

    @SuppressWarnings("rawtypes")
    public Class getResultClassCls() {
        return resultClassCls;
    }


    public void formatSQL() {
        if (StringUtils.isNotEmpty(sql)) {
            sql = sql.trim();
//			sql=sql.replaceAll("\t", "");
            Matcher m = EMPTYPATTERN.matcher(sql);
            if (m.find()) {
                sql = m.replaceAll(" ");
            }
            
            if(useAreaName&&useAreaNameForSingleSQL&&tableHasAreaNameColumn)
            {
            	   int index0 =sql.toLowerCase().indexOf("select");
                   if(index0>=0)
                   {
                	   SysLog.warn("before sql init : \n\r  "+sql);
                     List<SQLContition>  list=new ArrayList<SQLContition>();
                   	
                   	 int whereIndex =sql.toLowerCase().indexOf(SQLWHERE);
                   	 SQLContition  sc=new SQLContition(SQLWHERE,whereIndex,10,sql);
                   	 if(sc.isContain())
                   	 {
                   		 list.add(sc);
                   	 }
                   	 
                   	 
                   	 
//                   	 int idIndex =sql.toLowerCase().lastIndexOf(primeKey);

                	 
                   	   
                   
                   	 
                      int index3 =sql.toLowerCase().lastIndexOf(SQLGROUP);
                      SQLContition  sc2=new SQLContition(SQLGROUP,index3,30,sql);
                      if(sc2.isContain())
                   	 {
                   		 list.add(sc2);
                   	 }
                        
                     int index4 =sql.toLowerCase().lastIndexOf(SQLORDER);
                     SQLContition  sc3=new SQLContition(SQLORDER,index4,80,sql);
                     if(sc3.isContain())
                   	 {
                   		 list.add(sc3);
                   	 }
                     
                     
                	 int index2 =sql.toLowerCase().lastIndexOf(SQLLIMIT);
                   	 SQLContition  sc5=new SQLContition(SQLLIMIT,index2,100,sql);
                     if(sc5.isContain())
                   	 {
                   		 list.add(sc5);
                   	 }
                     
                 
                     if(list.isEmpty())
                      {
                       	    sql=sql +"  where areaName = ? ";
                        	parsedParameters.add("areaName");
                        	hasParameter = true;
                        	checkAreaNameOK=true;
                       }
                      else
                      {
                       	    
                           	 if(parsedParameters==null)
                         	{
                         		parsedParameters = new ArrayList<String>();
                         	}
                           	checkAreaNameOK=true;
                       	    if(list.size()==1)
                       	    {
                       	    	if(whereIndex>0)
                       	    	{
                       	    		if(selectUseId)
                           	    	{
                           	    		
                           	    	}
                       	    		else
                       	    		{
                       	    			sql=sql +"  and  areaName = ? ";
                           	    		parsedParameters.add("areaName");
                                    	hasParameter = true;
                       	    		}
                       	    		
                       	    	}
                       	    	else
                       	    	{
                       	    		SQLContition  sc4=list.get(0);
                       	    		String sqlPre=sql.substring(0, sc4.getLocation());
                       	    		String sqlSub=sql.substring(sc4.getLocation());
                       	    		sql=sqlPre+" where  areaName = ?  "+sqlSub;
                       	    		
                       	    		parsedParameters.add("areaName");
                                	hasParameter = true;
                       	    	}
                       	    }
                       	    else
                       	    {
                       	    	Collections.sort(list);
                       	    	if(whereIndex>0)
                       	    	{
                       	    		if(selectUseId)
                           	    	{
                           	    		
                           	    	}
                       	    		else
                       	    		{
                       	    			
                       	    			SQLContition  sc4=	list.get(1);
                           	    		String sqlPre=sql.substring(0, sc4.getLocation());
                           	    		String sqlSub=sql.substring(sc4.getLocation());
                           	    		sql=sqlPre+"  and areaName = ?  "+sqlSub;
                           	    		
                           	    		parsedParameters.add("areaName");
                                    	hasParameter = true;
                       	    		}
                       	    		
                       	    	}
                       	    	else
                       	    	{
                       	    		SQLContition  sc4=list.get(0);
                       	    		String sqlPre=sql.substring(0, sc4.getLocation());
                       	    		String sqlSub=sql.substring(sc4.getLocation());
                       	    		sql=sqlPre+"  where areaName = ?  "+sqlSub;
                       	    		
                       	    		parsedParameters.add("areaName");
                                	hasParameter = true;
                       	    	}
                       	    }
                       	    
                       	    list.clear();
                        }
                   	
                        SysLog.info("after sql init : \n\r "+sql);
                   }
            }
            else
            {
            	 SysLog.info("\n\r useAreaName: "+useAreaName+" #### useAreaNameForSingleSQL: "+useAreaNameForSingleSQL+" #### tableHasAreaNameColumn: "+tableHasAreaNameColumn);
            	 SysLog.info("after sql init : \n\r "+sql);
            }
        }

    }

    public Boolean getHasParameter() {
        return hasParameter;
    }

    public void setHasParameter(Boolean hasParameter) {
        this.hasParameter = hasParameter;
    }

    public boolean isOKSQL() {
        if (!cheked) {
            synchronized (this) {
                if (!cheked) {
                    cheked = true;
                    if (StringUtils.isEmpty(sql) || StringUtils.isEmpty(key) || StringUtils.isEmpty(operaType)) {
                        SysLog.error("sql  or key or operaType ie empty !");
                        isSQLOK = false;
                        return isSQLOK;
                    }

                    if (!OperaType.SELECT.getType().equals(operaType.toLowerCase())
                            && !OperaType.DELETE.getType().equals(operaType.toLowerCase())
                            && !OperaType.UPSERT.getType().equals(operaType.toLowerCase())) {
                        SysLog.error("operaType  must in SELECT, DELETE, UPSERT ");
                        isSQLOK = false;
                        return isSQLOK;
                    }

                    if (OperaType.SELECT.getType().equals(operaType.toLowerCase())
                            && StringUtils.isEmpty(resultClass)) {
                        SysLog.error("SELECT  must has resultClass ! resultClass is null");
                        isSQLOK = false;
                        return isSQLOK;
                    }

                    if (hasParameter && StringUtils.isEmpty(parameters)) {
                        SysLog.error("hasParameter = true,but  parameters is empty!");
                        isSQLOK = false;
                        return isSQLOK;
                    }

                    if (!StringUtils.isEmpty(resultClass)) {
                        try {
                            resultClassCls = Class.forName(resultClass.trim());
                        } catch (Exception e) {
                            SysLog.error("not find class for :  " + resultClass.trim(), e);
                            isSQLOK = false;
                            return isSQLOK;
                        }

                        if (resultClassMapping == null || resultClassMapping.isEmpty()) {
                            SysLog.error("resultClassMapping is empty !  ");
                            isSQLOK = false;
                            return isSQLOK;
                        }
                    }
                    isSQLOK = true;
                }
            }
        }

        return isSQLOK;

    }

    public List<String> getParsedParameters() {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    inited = true;
                    if (StringUtils.isNotEmpty(parameters)) {
                    	if(parsedParameters==null)
                    	{
                    		parsedParameters = new ArrayList<String>();
                    	}
                        
                        String paras[] = parameters.trim().split(",");
                        if (paras != null && paras.length > 0) {
                            for (String p : paras) {
                                if (StringUtils.isNotEmpty(p)) {
                                    parsedParameters.add(p.trim());
                                }
                            }
                        }
                    }

                }
            }
        }
        return parsedParameters;

    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getOperaType() {
        return operaType;
    }

    public void setOperaType(String operaType) {
        this.operaType = operaType;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getParameters() {
        return parameters;
    }

    public void setParameters(String parameters) {
        this.parameters = parameters;
    }

    public String getResultClass() {
        return resultClass;
    }

    public void setResultClass(String resultClass) {
        this.resultClass = resultClass;
    }

    public enum ResultContainerClass {
     	/**
    	 * 操作类型：查询
    	 */
        LIST("List"), 
        
     	/**
    	 * 操作类型：返回map
    	 */
        MAP("Map"), 
        
        /**
         * 普通类型
         */
        NORMAL("Normal");

        private String type;

        private ResultContainerClass(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }

        public String getType() {
            return type;
        }
    }

    public enum OperaType {
    	/**
    	 * 操作类型：查询
    	 */
        SELECT("select"),
        
    	/**
    	 * 操作类型：删除
    	 */
        DELETE("delete"),
        
        
    	/**
    	 * 操作类型：更新
    	 */
        UPSERT("upsert");

        private String type;

        private OperaType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }

        public String getType() {
            return type;
        }
    }

    public String getBatchQueryIdSQL() {
        return batchQueryIdSQL;
    }

    public void setBatchQueryIdSQL(String batchQueryIdSQL) {
        this.batchQueryIdSQL = batchQueryIdSQL;
    }

    public String getBatchQueryObjectSQL() {
        return batchQueryObjectSQL;
    }

    public void setBatchQueryObjectSQL(String batchQueryObjectSQL) {
        this.batchQueryObjectSQL = batchQueryObjectSQL;
    }
}
