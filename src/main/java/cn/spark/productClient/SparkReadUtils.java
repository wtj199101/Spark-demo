package cn.spark.productClient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
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
    private static final String CHECK_POINT_DIR="/checkpoint";


    public static void main(String[] args) throws  Exception {
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(CHECK_POINT_DIR, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                return createContext();
            }
        });
        jsc.start();
        jsc.awaitTermination();

    }
public  static JavaStreamingContext createContext(){
            Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "192.168.12.102:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("test");
        SparkConf sc=new SparkConf().setAppName("Spark shell").setMaster("local[*]");
        //设置每2秒读取一次kafka
    JavaStreamingContext jssc=new JavaStreamingContext(sc,new Duration(2000));

            final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
            JavaPairDStream<String, String> jpds = stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                        System.out.println("topci="+record.topic()+";key="+record.key()+";value="+record.value());
                        return new Tuple2<String, String>(record.key(), record.value());
                    }
                });
      jpds.count().print();
        return jssc;
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
}
