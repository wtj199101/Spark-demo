package cn.spark.maindemo;

import java.io.IOException;  
import java.util.ArrayList;  
import java.util.List;  
   
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.KeyValue;  
import org.apache.hadoop.hbase.MasterNotRunningException;  
import org.apache.hadoop.hbase.ZooKeeperConnectionException;  
import org.apache.hadoop.hbase.client.Delete;  
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.util.Bytes;  
   
public class HBaseDemo {  
      
    private static Configuration conf = null;  
       
    /** 
     * 初始化配置 
     */  
    static {  
        Configuration HBASE_CONFIG = new Configuration();  
        //与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同   
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "hadoop01.dev.xjh.com,hadoop02.dev.xjh.com,hadoop03.dev.xjh.com");  
        //与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同  
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");  
        conf = HBaseConfiguration.create(HBASE_CONFIG);  
    }  
      
    /** 
     * 创建一张表 
     */  
    public static void creatTable(String tableName, String[] familys) throws Exception {  
        HBaseAdmin admin = new HBaseAdmin(conf);  
        if (admin.tableExists(tableName)) {  
            System.out.println("table already exists!");  
        } else {  
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);  
            for(int i=0; i<familys.length; i++){  
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));  
            }  
            admin.createTable(tableDesc);  
            System.out.println("create table " + tableName + " ok.");  
        }   
    }  
      
    /** 
     * 删除表 
     */  
    public static void deleteTable(String tableName) throws Exception {  
       try {  
           HBaseAdmin admin = new HBaseAdmin(conf);  
           admin.disableTable(tableName);  
           admin.deleteTable(tableName);  
           System.out.println("delete table " + tableName + " ok.");  
       } catch (MasterNotRunningException e) {  
           e.printStackTrace();  
       } catch (ZooKeeperConnectionException e) {  
           e.printStackTrace();  
       }  
    }  
       
    /** 
     * 插入一行记录 
     */  
    public static void addRecord (String tableName, String rowKey, String family, String qualifier, String value)  
            throws Exception{  
        try {  
            HTable table = new HTable(conf, tableName);  
            Put put = new Put(Bytes.toBytes(rowKey));  
            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));  
            table.put(put);  
            System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
   
    /** 
     * 删除一行记录 
     */  
    public static void delRecord (String tableName, String rowKey) throws IOException{  
        HTable table = new HTable(conf, tableName);  
        List list = new ArrayList();  
        Delete del = new Delete(rowKey.getBytes());  
        list.add(del);  
        table.delete(list);  
        System.out.println("del recored " + rowKey + " ok.");  
    }  
       
    /** 
     * 查找一行记录 
     */  
    public static void getOneRecord (String tableName, String rowKey) throws IOException{  
        HTable table = new HTable(conf, tableName);  
        Get get = new Get(rowKey.getBytes());  
        Result rs = table.get(get);  
        for(KeyValue kv : rs.raw()){  
            System.out.print(new String(kv.getRow()) + " " );  
            System.out.print(new String(kv.getFamily()) + ":" );  
            System.out.print(new String(kv.getQualifier()) + " " );  
            System.out.print(kv.getTimestamp() + " " );  
            System.out.println(new String(kv.getValue()));  
        }  
    }  
       
    /** 
     * 显示所有数据 
     */  
    public static void getAllRecord (String tableName) {  
        try{  
             HTable table = new HTable(conf, tableName);  
             Scan s = new Scan();  
             ResultScanner ss = table.getScanner(s);  
             for(Result r:ss){  
                 for(KeyValue kv : r.raw()){  
                    System.out.print(new String(kv.getRow()) + " ");  
                    System.out.print(new String(kv.getFamily()) + ":");  
                    System.out.print(new String(kv.getQualifier()) + " ");  
                    System.out.print(kv.getTimestamp() + " ");  
                    System.out.println(new String(kv.getValue()));  
                 }  
             }  
        } catch (IOException e){  
            e.printStackTrace();  
        }  
    }  
      
    public static void  main (String [] agrs) {  
        try {  
//        	checkHbaseAvailable();
            String tablename = "scores";
            String[] familys = {"grade", "course"};
            //HBaseDemo.creatTable(tablename, familys);

            //add record zkb
//            HBaseDemo.addRecord(tablename,"zkb","grade","","5");
//            HBaseDemo.addRecord(tablename,"zkb","course","","90");
//            HBaseDemo.addRecord(tablename,"zkb","course","math","97");
//            HBaseDemo.addRecord(tablename,"zkb","course","art","87");
            //add record  baoniu
//            HBaseDemo.addRecord(tablename,"baoniu","grade","","4");
//            HBaseDemo.addRecord(tablename,"baoniu","course","math","89");

            /*System.out.println("===========get one record========");
            HBaseDemo.getOneRecord(tablename, "zkb");
*/
//            System.out.println("===========show all record========");
            HBaseDemo.getAllRecord(tablename);
               
//            System.out.println("===========del one record========");
//            HBaseDemo.delRecord(tablename, "baoniu");
//            HBaseDemo.getAllRecord(tablename);

//            System.out.println("===========show all record========");
//            HBaseDemo.getAllRecord(tablename);
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    } 
    
    public static final String MASTER_IP = "hadoop01.dev.xjh.com";
    public static final String ZOOKEEPER_PORT = "2181";
    public static void checkHbaseAvailable(){
	    Configuration config = HBaseConfiguration.create();
	    config.set("hbase.zookeeper.quorum", MASTER_IP);
	    config.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
	
	    System.out.println("Running connecting test...");
	
	    try {
	        HBaseAdmin.checkHBaseAvailable(config);
	        System.out.println("HBase found!");
	        HTable table = new HTable(config, "testTable");
	        System.out.println("Table testTable obtained!");
	    } catch (MasterNotRunningException e) {
	        System.out.println("HBase connection failed!");
	        e.printStackTrace();
	    } catch (ZooKeeperConnectionException e) {
	        System.out.println("Zookeeper connection failed!");
	        e.printStackTrace();
	    } catch (Exception e) { 
	    	e.printStackTrace(); 
	    }
    }
}