package cn.spark.productClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/06/23.
 */
public class SparkReadUtils {
    //################# kafka ##########################
    private static final String CHECK_POINT_DIR="/checkpoint";
    private static final String KAFKA_TOPIC="test";
    private static final String HBASE_Table_Name="test";
    private static final String KAFKA_SERVERS="192.168.12.102:9092";
    private static final String KAFKA_GROUP_ID="test-group";
    private static final String KAFKA_OFFERT_RESET="latest";
    private static final Boolean KAFKA_AUTO_COMMIT=false;
    //################# spark ##########################
    private static final String SPARK_APP_NAME="Spark shell";
    private static final String SPARK_MASTER="spark://dev-hadoop01:7077";
//    private static final String SPARK_MASTER="local[*]";//本地
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
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", KAFKA_SERVERS);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", KAFKA_GROUP_ID);
        kafkaParams.put("auto.offset.reset", KAFKA_OFFERT_RESET);
        kafkaParams.put("enable.auto.commit", KAFKA_AUTO_COMMIT);
        Collection<String> topics = Arrays.asList(KAFKA_TOPIC);

        SparkConf sc=new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER);
        sc.set("spark.serializer", SPARK_SERIALIZER);
        //设置每2秒读取一次kafka
        final JavaStreamingContext jssc=new JavaStreamingContext(sc,new Duration(DUR_TIME));

        final Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUN);
        conf.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
        final JobConf jc=new JobConf(conf);
        jc.setOutputFormat(TableOutputFormat.class);
        jc.set(TableOutputFormat.OUTPUT_TABLE,tableName);
//#################从kafka中取得数据并封装###################################
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        JavaPairDStream<ImmutableBytesWritable, Put> jpds = stream.mapToPair(
            new PairFunction<ConsumerRecord<String, String>, ImmutableBytesWritable, Put>() {
                @Override
                public Tuple2<ImmutableBytesWritable, Put> call(ConsumerRecord<String, String> record) throws Exception {
                    //TODO 这里是封装数据,根据需求改变
                    return convertToPut("1","title","", record.value());
                }
            });
        //#############################保存数据进入hbase ##################
        jpds.foreachRDD(new VoidFunction2<JavaPairRDD<ImmutableBytesWritable, Put>, Time>() {
        @Override
        public void call(JavaPairRDD<ImmutableBytesWritable, Put> v1, Time v2) throws Exception {
            System.out.println("aaaaaaaaaaaaaaaaa"+v1.count());
            if(v1.count()>0){
                v1.saveAsHadoopDataset(jc);
            }
        }
    });
        jpds.print();
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
//    public static void main(String[] args)  throws  Exception{
//        Map<String, Object> kafkaParams = new HashMap<String, Object>();
//        kafkaParams.put("bootstrap.servers", "192.168.12.102:9092");
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
//        kafkaParams.put("group.id", "test-group");
//        kafkaParams.put("auto.offset.reset", "latest");
//        kafkaParams.put("enable.auto.commit", false);
//        Collection<String> topics = Arrays.asList("test");
//        SparkConf sc=new SparkConf().setAppName("Spark shell").setMaster("local[*]");
//        JavaStreamingContext jssc=new JavaStreamingContext(sc,new Duration(10000));
//        final JavaInputDStream<ConsumerRecord<String, String>> stream =
//                KafkaUtils.createDirectStream(
//                        jssc,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//                );
//        JavaPairDStream<String, String> jpds = stream.mapToPair(
//                new PairFunction<ConsumerRecord<String, String>, String, String>() {
//                    @Override
//                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
//                        System.out.println("topci="+record.topic()+";key="+record.key()+";value="+record.value());
//                        return new Tuple2<String, String>(record.key(), record.value());
//                    }
//                });
//        System.out.println("#############begin");
//        jpds.count().print();
//        System.out.println("#############end");
//        jssc.start();
//        jssc.awaitTermination();
//    }
    //            JavaPairDStream<String, String> jpds = stream.mapToPair(
//                new PairFunction<ConsumerRecord<String, String>, String, String>() {
//                        @Override
//                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
//                        System.out.println("topci="+record.topic()+";key="+record.key()+";value="+record.value());
//                return new Tuple2<String, String>(record.key(), record.value());
//                    }
//                });
}
