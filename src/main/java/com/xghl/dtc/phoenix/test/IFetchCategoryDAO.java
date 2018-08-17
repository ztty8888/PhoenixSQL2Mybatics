package com.xghl.dtc.phoenix.test;

import com.xghl.dtc.zeuscommon.bean.FetchCategory;

import java.util.Date;
import java.util.List;

/**
 * @author luodf
 * @description:
 * @since 2017/08/01
 */
public interface IFetchCategoryDAO {
    public static final String SPRING_CONFIG_BEANNAME = "fetchCategoryDAO";

    public Long getFetchCategorysCountById(String id);

    FetchCategory getFetchCategoryByCode(String code);

    public void addFetchCategory(FetchCategory category);

    public List<FetchCategory> getFetchCategoryByStatus(int status, int limitCount);

    public void updateCategoryPageStatus(String id, int status);

    public List<FetchCategory> getFetchCategoryByStatusLE(int status, int limitCount);

    public void batchUpdateFetchCategoryStatus(List<FetchCategory> items);

    
    
    public List<FetchCategory> getAllFetchCategory();

    public List<FetchCategory> getFetchCategoryByScanTime(long scanTime);

    public void updateFetchCategoryByScanTime(List<FetchCategory> fetchCategories);

    public Long getFetchCategoryByScanTimeAndStatus(long scanTime, int status);


    public List<FetchCategory> getFetchCategoryByModified(int limitnumber);

    public void updateFetchCategoryStatusByModified(List<FetchCategory> fetchCategories);

}
  