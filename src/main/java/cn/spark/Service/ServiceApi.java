package cn.spark.Service;
//import kafka.serializer.StringDecoder;
//import org.apache.spark.SparkConf;
//import com.alibaba.fastjson.JSON;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.SequenceFileOutputFormat;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import scala.Tuple2;
//
//import java.net.URLDecoder;
//import java.util.*;
//
//public class ServiceApi {
//    static final String ZK_QUORUM = "localhost:9092";
//    static final String GROUP = "test-consumer-group";
//    static final String TOPICSS = "user_trace";
//    static final String NUM_THREAD = "64";
//    public static void main(String[] args) throws  Exception {
//        SparkConf sparkConf = new SparkConf().setAppName("main.java.computingCenter");
//        // Create the context with 2 seconds batch size
//        //每两秒读取一次kafka
//        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
//        int numThreads = Integer.parseInt(NUM_THREAD);
//        Map<String, Integer> topicMap = new HashMap<String, Integer>();
//        Set<String> topicSet = new HashSet<String>();
//        topicSet.add("test_topic");
//        HashMap<String, String> kafkaParam = new HashMap<String, String>();
//        kafkaParam.put("metadata.broker.list", "test1:9092,test2:9092");
//        JavaPairReceiverInputDStream<String, String> messages =KafkaUtils.createDirectStream(
//                jssc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParam,
//                topicSet
//        );
//
//        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//            public String call(Tuple2<String, String> tuple2) {
//                return tuple2._2();
//            }
//        });
//        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            public Iterable<String> call(String lines) {
//                //kafka数据格式："{\"Topic\":\"user_trace\",\"PartitionKey\":\"0\",\"TimeStamp\":1471524044018,\"Data\":\"0=163670589171371918%3A196846178238302087\",\"LogId\":\"0\",\"ContentType\":\"application/x-www-form-urlencoded\"}";
//                List<String> arr = new ArrayList<String>();
//                for (String s : lines.split(" ")) {
//                    Map j = JSON.parseObject(s);
//                    String s1 = "";
//                    String s2 = "";
//                    try {
//                        s1 = URLDecoder.decode(j.get("Data").toString(), "UTF-8");
//                        s2 = s1.split("=")[1];
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    arr.add(s2);
//                }
//                return arr;
//            }
//        });
//
//        JavaPairDStream<String, String> goodsSimilarityLists = words.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                //过滤非法的数据
//                if (s.split(":").length == 2) {
//                    return true;
//                }
//                return false;
//            }
//        }).mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {
//            //此处分partition对每个pair进行处理
//            @Override
//            public Iterable<Tuple2<String, String>> call(Iterator<String> s) throws Exception {
//                ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
//                while (s.hasNext()) {
//                    String x = s.next();
//                    String userId = x.split(":")[0];
//                    String goodsId = x.split(":")[1];
//                    System.out.println(x);
//                    LinkedHashMap<Long, Double> recommendMap = null;
//                    try {
//                        //此service从redis读数据,进行实时兴趣度计算,推荐结果写入redis,供api层使用
////                        CalculateInterestService calculateInterestService = new CalculateInterestService();
//                        try {
////                            recommendMap = calculateInterestService.calculateInterest(userId, goodsId);
//                            recommendMap =null;
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//
//                        String text = "";
//                        int count = 0;
//                        for (Map.Entry<Long, Double> entry : recommendMap.entrySet()) {
//                            text = text + entry.getKey();
//                            if (count == recommendMap.size() - 1) {
//                                break;
//                            }
//                            count = count + 1;
//                            text = text + "{/c}";
//                        }
//
//                        text = System.currentTimeMillis() + ":" + text;
//                        result.add(new Tuple2<String, String>(userId, text));
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//
//                return result;
//            }
//        });
//
//        goodsSimilarityLists.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
//            @Override
//            public Void call(JavaPairRDD<String, String> rdd) throws Exception {
//                //打印rdd，调试方便
//                System.out.println(rdd.collect());
//                return null;
//            }
//        });
//
//        JavaPairDStream<Text, Text> goodsSimilarityListsText = goodsSimilarityLists.mapToPair(new PairFunction<Tuple2<String, String>, Text, Text>(){
//            @Override
//            public Tuple2<Text, Text> call(Tuple2<String, String> ori) throws Exception {
//                //此处要将tuple2转化为org.apache.hadoop.io.Text格式，使用saveAsHadoopFiles方法写入hdfs
//                return new Tuple2(new Text(ori._1), new Text(ori._2));
//            }
//        });
//
//        //写入hdfs
//        goodsSimilarityListsText.saveAsHadoopFiles("/user/hadoop/recommend_list/rl", "123", Text.class, Text.class, SequenceFileOutputFormat.class);
//        jssc.start();
//        jssc.awaitTermination();
//
//    }
//}
