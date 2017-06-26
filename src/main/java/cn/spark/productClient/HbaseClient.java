package cn.spark.productClient;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.List;

public class HbaseClient {
    private void start() {
        JavaSparkContext sc = new JavaSparkContext("local[*]","Spark shell");


        //使用HBaseConfiguration.create()生成Configuration
        // 必须在项目classpath下放上hadoop以及hbase的配置文件。

        //设置查询条件，这里值返回用户的等级
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("0"));
        scan.setStopRow(Bytes.toBytes("1"));
        scan.addFamily(Bytes.toBytes("info"));
        scan.addColumn(Bytes.toBytes("info"),Bytes.toBytes("url"));

        try {
            Configuration conf = HBaseConfiguration.create();
            //需要读取的hbase表名
            String tableName = "test";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            conf.set(TableInputFormat.SCAN, scan.toJSON());
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum", "127.0.0.1");
//            conf.set();

            //高版本可以用如下方式：
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String scanStr = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, scanStr);

            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc
                    .newAPIHadoopRDD(conf, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class);
            System.out.println(hBaseRDD.count());
            Long count = hBaseRDD.count();
            System.out.println("count: " + count);

            List<Tuple2<ImmutableBytesWritable, Result>> tuples = hBaseRDD
                    .take(count.intValue());
            for (int i = 0, len = count.intValue(); i < len; i++) {
                Result result = tuples.get(i)._2();
                KeyValue[] kvs = result.raw();
                for (KeyValue kv : kvs) {
                    System.out.println("rowkey:" + new String(kv.getRow()) + " cf:"
                            + new String(kv.getFamily()) + " column:"
                            + new String(kv.getQualifier()) + " value:"
                            + new String(kv.getValue()));
                }
            }
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