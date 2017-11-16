package com.fulihui.open.data.manage.service;

import com.fulihui.open.data.manage.dao.HbaseDao;
import com.fulihui.open.data.manage.pojo.TestModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;

/**
 * Created by wtjun on 2017/10/31.
 */
@Service
public class HbaseService {
    @Autowired
    private HbaseDao<TestModel> hbaseDao;


    public  void testInsert(String tableName,TestModel test){
        hbaseDao.createTable(tableName,test.getFamily());
//        hbaseDao.Hput("user_pro:"+tableName,test.getRowkey(),test.getFamily(),test.getQualifier(),Bytes.toBytes(test.getValue()));
    }


}
