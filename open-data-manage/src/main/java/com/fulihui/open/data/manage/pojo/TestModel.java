package com.fulihui.open.data.manage.pojo;

import java.io.Serializable;

/**
 * Created by wtjun on 2017/10/31.
 */

public class TestModel implements Serializable{
    private String rowkey;
    private  String family;
    private String qualifier;
    private  String value;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
