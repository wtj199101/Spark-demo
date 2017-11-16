package com.fulihui.open.data.manage.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Created by wtjun on 2017/10/31.
 */
@Repository
public class HbaseDao<T> {
    @Value("${hbase.zk.host}")
    private String zkHost;
    @Value("${hbase.zk.port}")
    private String zkPort;

    @Autowired
    private HbaseTemplate hbaseTemplate;

    public void createTable(String tableName,String famaily) {
        try {
        Configuration config = hbaseTemplate.getConfiguration();
            Connection conn = ConnectionFactory.createConnection(config);
            Admin admin = conn.getAdmin();
            if(!admin.tableExists(TableName.valueOf("user_pro:"+tableName))){
//                不存在则创建 默认namespace
                admin.createNamespace(NamespaceDescriptor.create("user_pro").build());
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_pro:"+tableName));
                HColumnDescriptor rowkey1 = new HColumnDescriptor(famaily);
                hTableDescriptor.addFamily(rowkey1);
                admin.createTable(hTableDescriptor);
          }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public  void Hput(String tableName,  String rowName,  String familyName,  String qualifier,  byte[] value){
        hbaseTemplate.put( tableName,   rowName,   familyName,   qualifier,  value);
    }

    public void  Hdelete(String tableName, String rowName, String familyName){
        hbaseTemplate.delete( tableName,  rowName,  familyName);
    }
    public  <T> T Hget(String tableName, String rowName, final RowMapper<T> mapper){
       return hbaseTemplate.get(tableName,rowName, mapper);
    }

}
