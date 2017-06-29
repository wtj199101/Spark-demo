package cn.spark.maindemo;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;


public class SparkMain {
    //################# kafka ##########################
    private static final String CHECK_POINT_DIR="/checkpoint";
    private static final String KAFKA_TOPIC="test";
    private static final String HBASE_Table_Name="test";
    private static final String KAFKA_SERVERS="192.168.12.102:9092";
    private static final String KAFKA_GROUP_ID="test-group";
    //    private static final String KAFKA_OFFERT_RESET="latest";
    private static final String KAFKA_OFFERT_RESET="largest";//kafka 0.8 中是 largest and smallest
    private static final Boolean KAFKA_AUTO_COMMIT=false;
    //################# spark ##########################
    private static final String SPARK_APP_NAME="Spark shell";
    //    private static final String SPARK_MASTER="spark://dev-hadoop01:7077";
    private static final String SPARK_MASTER="local[*]";//本地
    private static final String SPARK_SERIALIZER="org.apache.spark.serializer.KryoSerializer";
    private static final Integer DUR_TIME=2000;
    //################# HBASE ##########################
    private static final String HBASE_ZOOKEEPER_QUORUN="hadoop01.dev.xjh.com,hadoop02.dev.xjh.com,hadoop03.dev.xjh.com";
    private static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT="2181";
    private static final String HBASE_MASTER="localhost:60010";//待使用

    public static void main(String[] args) throws  Exception {
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(CHECK_POINT_DIR, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                return createContext(HBASE_Table_Name);
            }
        });
        jsc.start();
        jsc.awaitTermination();

    }
    public  static JavaStreamingContext createContext(String tableName){
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", KAFKA_SERVERS);
        kafkaParams.put("group.id", KAFKA_GROUP_ID);
        kafkaParams.put("auto.offset.reset", KAFKA_OFFERT_RESET);
        Set<String> topics = new HashSet<String>(Arrays.asList(KAFKA_TOPIC));

        SparkConf sc=new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER);
        sc.set("spark.serializer", SPARK_SERIALIZER);
//        sc.setJars(new String[]{"E:\\apache-maven-3.2.3\\repository\\org\\apache\\spark\\spark-streaming-kafka_2.11\\1.6.3\\spark-streaming-kafka_2.11-1.6.3-sources.jar"});
        //设置每2秒读取一次kafka
        final JavaStreamingContext jssc=new JavaStreamingContext(sc,new Duration(DUR_TIME));

        final Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUN);
        conf.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
        final JobConf jc=new JobConf(conf);
        jc.setOutputFormat(TableOutputFormat.class);
        jc.set(TableOutputFormat.OUTPUT_TABLE,tableName);
        //#################从kafka中取得数据并封装###################################
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams, topics
        );
        JavaPairDStream<ImmutableBytesWritable, Put> jpds = directStream.mapToPair(new PairFunction<Tuple2<String, String>, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                //TODO 这里是封装数据,根据需求改变
                System.out.println("aaaaaaaaaaaaaaaa"+stringStringTuple2.toString());
                return convertToPut("1","title","", stringStringTuple2._2());
            }
        });
        //#############################保存数据进入hbase ##################
        jpds.foreachRDD(new VoidFunction2<JavaPairRDD<ImmutableBytesWritable, Put>, Time>() {
            @Override
            public void call(JavaPairRDD<ImmutableBytesWritable, Put> v1, Time v2) throws Exception {
                System.out.println("aaaaaaaaaaaaaaaaa"+v1.count()+",time="+v2.toString());
                if(v1.count()>0){
                    v1.saveAsHadoopDataset(jc);
                }
            }
        });
        return jssc;
    }

    /**
     *生成要插入hbase的数据格式
     * @param row
     * @param column
     * @param qualifier
     * @param value
     * @return
     */
    protected  static Tuple2<ImmutableBytesWritable, Put> convertToPut(String row,String column,String qualifier,String value){
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(column),Bytes.toBytes(qualifier),Bytes.toBytes(value));
        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }

}
