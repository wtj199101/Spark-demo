package cn.spark.maindemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class HbaseClient {
    /**
     * spark如果计算没写在main里面,实现的类必须继承Serializable接口，<br>
     * </>否则会报 Task not serializable: java.io.NotSerializableException 异常
     */
    public static void main(String[] args) throws Exception {
//        JavaSparkContext jsc = new JavaSparkContext("spark://dev-hadoop01:7077","Spark shell");
        SparkConf sc=new SparkConf();
        sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext jsc = new JavaSparkContext("local[*]","Spark shell");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("name"),Bytes.toBytes("title"));

        //需要读取的hbase表名
        String tableName="test";
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.master", "localhost:60010");
        conf.set("hbase.zookeeper.quorum", "hadoop01.dev.xjh.com,hadoop02.dev.xjh.com,hadoop03.dev.xjh.com");
//        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String scanStr = Base64.encodeBytes(proto.toByteArray());
        conf.set(TableInputFormat.SCAN, scanStr);
        conf.set(TableInputFormat.INPUT_TABLE,tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc
                .newAPIHadoopRDD(conf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);
        System.out.println("aaaaaaaaaaaaaa"+hBaseRDD.count());
        Long count = hBaseRDD.count();

        JavaPairRDD<String, String> javaPairRDD = hBaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                System.out.println("gggggggggggggg");
                byte[] o = immutableBytesWritableResultTuple2._2().getValue(
                        Bytes.toBytes("name"), Bytes.toBytes("title"));
                if (o != null) {
                    System.out.println(Bytes.toString(o));
                    return new Tuple2<String, String>(Bytes.toString(o), "1");
                }
                return null;
            }
        });
        //#############################数据处理##############################
        JavaPairRDD<String, String> result = javaPairRDD.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + ":" + v2;
            }
        });
        List<Tuple2<String, String>> output = result.collect();
                for (Tuple2 tuple : output) {
            System.out.println(tuple._1 + ": " + tuple._2);
        }








//        List<Tuple2<ImmutableBytesWritable, Result>> tuples = hBaseRDD.take(count.intValue());

//        for (int i = 0, len = count.intValue(); i < len; i++) {
//            Result result = tuples.get(i)._2();
//            KeyValue[] kvs = result.raw();
//            for (KeyValue kv : kvs) {
//                System.out.println("rowkey:" + new String(kv.getRow()) + " cf:"
//                        + new String(kv.getFamily()) + " column:"
//                        + new String(kv.getQualifier()) + " value:"
//                        + new String(kv.getValue()));
//            }
//        }
}

public void test() throws  Exception{
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "hadoop01.dev.xjh.com,hadoop02.dev.xjh.com,hadoop03.dev.xjh.com");
        String tableName = "test";
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
//            conf.set(TableInputFormat.SCAN_COLUMNS,"title:");
        Configuration conf2 = HBaseConfiguration.create(conf);

        HTable table = new HTable(conf2, tableName);
        Get get = new Get("1".getBytes());
        Result rs = table.get(get);
        for (KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) + " ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + " ");
            System.out.print(kv.getTimestamp() + " ");
            System.out.println(new String(kv.getValue()));
        }
    //            Connection conn = ConnectionFactory.createConnection(conf);
//            Table table = conn.getTable(TableName.valueOf(tableName));
//            System.out.println("33333333333333333333333");
//            Put p=new Put("1".getBytes());
//            p.addColumn("name".getBytes(),"title".getBytes(),"world".getBytes());
//            table.put(p);
//            System.out.println("1111111111111");

    // 如果表不存在则创建表
//            HBaseAdmin admin = new HBaseAdmin(conf);
//            if (!admin.isTableAvailable(tableName)) {
//                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
//                admin.createTable(tableDesc);
//            }
    }
}
