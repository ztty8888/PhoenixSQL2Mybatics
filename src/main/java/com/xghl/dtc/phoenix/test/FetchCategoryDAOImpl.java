package com.xghl.dtc.phoenix.test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.xghl.dtc.phoenix.BasePhoenixDAO;
import com.xghl.dtc.zeuscommon.bean.FetchCategory;

/**
 * @author luodf
 * @description:
 * @since 2017/08/01
 */
public class FetchCategoryDAOImpl extends BasePhoenixDAO implements IFetchCategoryDAO {

    @Override
    public void addFetchCategory(FetchCategory category) {
        this.upSertDelObject(category, "addFetchCategory");
    }

    @Override
    public List<FetchCategory> getFetchCategoryByStatus(int status, int limitCount) {

        Map<String, Object> bean = objectPool.getMap();
        bean.put("status", status);
        bean.put("limitCount", limitCount);
        List<FetchCategory> items = this.queryMoreObject(bean, "getFetchCategoryByStatus");
        objectPool.returnMap(bean);
        return items;
    }

    @Override
    public Long getFetchCategorysCountById(String id) {

        Map<String, Object> bean = objectPool.getMap();
        bean.put("id", id);
        Long count = (Long) this.queryOneObject(bean, "getFetchCategorysCountById");
        objectPool.returnMap(bean);
        return count;
    }

    @Override
    public FetchCategory getFetchCategoryByCode(String code) {

        Map<String, Object> bean = objectPool.getMap();
        bean.put("code", code);
        FetchCategory fetchCategory = (FetchCategory) this.queryOneObject(bean, "getFetchCategoryByCode");
        objectPool.returnMap(bean);
        return fetchCategory;
    }

    @Override
    public void updateCategoryPageStatus(String id, int status) {

        Map<String, Object> bean = objectPool.getMap();
        bean.put("id", id);
        bean.put("status", status);
        bean.put("gmtModified", new Date());
        this.upSertDelObject(bean, "updateCategoryPageStatus");
        objectPool.returnMap(bean);
    }

    @Override
    public void batchUpdateFetchCategoryStatus(List<FetchCategory> items) {
        this.batchUpSertDelObject(items, "batchUpdateFetchCategoryStatus");

    }

    @Override
    public List<FetchCategory> getFetchCategoryByStatusLE(int status, int limitCount) {

        Map<String, Object> bean = objectPool.getMap();
        bean.put("status", status);
        bean.put("limitCount", limitCount);
        List<FetchCategory> items = this.queryMoreObject(bean, "getFetchCategoryByStatusLE");
        objectPool.returnMap(bean);
        return items;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<FetchCategory> getAllFetchCategory() {

        Map<String, Object> bean = objectPool.getMap();
        List<FetchCategory> items = this.queryMoreObject(bean, "getAllFetchCategory");
        objectPool.returnMap(bean);
        return items;
    }

    @Override
    public List<FetchCategory> getFetchCategoryByScanTime(long scanTime) {

        Map<String, Object> bean = objectPool.getMap();
        bean.put("scanTime", scanTime);
        List<FetchCategory> items = this.queryMoreObject(bean, "getFetchCategoryByScanTime");
        objectPool.returnMap(bean);
        return items;
    }

    @Override
    public void updateFetchCategoryByScanTime(List<FetchCategory> fetchCategories) {

        this.batchUpSertDelObject(fetchCategories, "updateFetchCategoryByScanTime");
    }

    @Override
    public Long getFetchCategoryByScanTimeAndStatus(long scanTime, int status) {

        Map<String, Object> bean = objectPool.getMap();
        bean.put("scanTime", scanTime);
        bean.put("status", status);
        Long count = (Long) this.queryOneObject(bean, "getFetchCategoryByScanTimeAndStatus");
        objectPool.returnMap(bean);
        return count;
    }

    @Override
    public List<FetchCategory> getFetchCategoryByModified(int limitnumber) {

        Map<String, Object> bean = objectPool.getMap();
        bean.put("limitnumber", limitnumber);
        List<FetchCategory> items = this.queryMoreObject(bean, "getFetchCategoryByModified");
        objectPool.returnMap(bean);
        return items;
    }

    @Override
    public void updateFetchCategoryStatusByModified(List<FetchCategory> fetchCategories) {
        this.batchUpSertDelObject(fetchCategories, "updateFetchCategoryStatusByModified");
    }


}
