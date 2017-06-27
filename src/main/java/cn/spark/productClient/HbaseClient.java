package cn.spark.productClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

public class HbaseClient {
    private void start() {
//        SparkConf sc=new SparkConf();
//        sc.setMaster("local[*]").setAppName("Spark shell").set("spark.driver.host","localhost").set("sp‌​ark.driver.port","7077");
//        JavaSparkContext jsc = new JavaSparkContext("spark://dev-hadoop01:7077","Spark shell");
//        JavaSparkContext jsc = new JavaSparkContext(sc);
        //使用HBaseConfiguration.create()生成Configuration
        // 必须在项目classpath下放上hadoop以及hbase的配置文件。
        //设置查询条件，这里值返回用户的等级
//        System.out.println("1212"+jsc);
//        jsc.stop();
//        if(true){
//            return;
//        }
//        Scan scan = new Scan();
////        scan.setStartRow(Bytes.toBytes("0"));
////        scan.setStopRow(Bytes.toBytes("2"));
//        scan.addFamily(Bytes.toBytes("title"));
//        scan.addColumn(Bytes.toBytes("title"),Bytes.toBytes(""));

        try {
            Configuration conf = HBaseConfiguration.create();
            //需要读取的hbase表名
            String tableName = "test";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            conf.set("hbase.master", "localhost:60010");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum","127.0.0.1");
//           conf.set(TableInputFormat.SCAN_COLUMNS,"title:");
            Connection conn = ConnectionFactory.createConnection(conf);
            Table table = conn.getTable(TableName.valueOf(tableName));
            System.out.println("33333333333333333333333"+table.getName());
            Result result1 = table.get(new Get("1".getBytes()));
                System.out.println(result1.getExists());   
            System.out.println(result1.getStats());
            System.out.println(result1.getValue("info".getBytes(),"url".getBytes()).toString());
            Put p=new Put("2".getBytes());
            p.addColumn("info".getBytes(),"title".getBytes(),"world".getBytes());
            table.put(p);
            System.out.println("1111111111111");
            if(true){
                return;
            }
            // 如果表不存在则创建表
//            HBaseAdmin admin = new HBaseAdmin(conf);
//            if (!admin.isTableAvailable(tableName)) {
//                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
//                admin.createTable(tableDesc);
//            }
            //高版本可以用如下方式：
//            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
//            String scanStr = Base64.encodeBytes(proto.toByteArray());
//            conf.set(TableInputFormat.SCAN, scanStr);
//            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc
//                    .newAPIHadoopRDD(conf, TableInputFormat.class,
//                            ImmutableBytesWritable.class, Result.class);
//            System.out.println("aaaaaaaaaaaaaa"+hBaseRDD.count());
//            Long count = hBaseRDD.count();
//            System.out.println("count: " + count);
//
//            List<Tuple2<ImmutableBytesWritable, Result>> tuples = hBaseRDD
//                    .take(count.intValue());
//            for (int i = 0, len = count.intValue(); i < len; i++) {
//                Result result = tuples.get(i)._2();
//                KeyValue[] kvs = result.raw();
//                for (KeyValue kv : kvs) {
//                    System.out.println("rowkey:" + new String(kv.getRow()) + " cf:"
//                            + new String(kv.getFamily()) + " column:"
//                            + new String(kv.getQualifier()) + " value:"
//                            + new String(kv.getValue()));
//                }
//            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


//    static String convertScanToString(Scan scan) throws Exception {
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        DataOutputStream dos = new DataOutputStream(out);
//        scan.
//        return Base64.encodeBytes(out.toByteArray());
//    }
    /**
     * spark如果计算没写在main里面,实现的类必须继承Serializable接口，<br>
     * </>否则会报 Task not serializable: java.io.NotSerializableException 异常
     */
    public static void main(String[] args) throws InterruptedException {

        new HbaseClient().start();

        System.exit(0);
}
}