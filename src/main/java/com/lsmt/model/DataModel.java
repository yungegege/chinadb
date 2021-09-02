package com.lsmt.model;

import lombok.Data;

/**
 * @author liyunfei
 * Created on 2021-08-23
 */
@Data
public class DataModel {

    private String key;
    private String value;

    /**
     * 是否删除
     */
    private int delete;

    public DataModel(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public DataModel(String key, int delete) {
        this.key = key;
        this.delete = delete;
    }
}
