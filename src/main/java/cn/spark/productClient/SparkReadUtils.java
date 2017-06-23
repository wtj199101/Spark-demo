package cn.spark.productClient;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by Administrator on 2017/06/23.
 */
public class SparkReadUtils {
    public static void main(String[] args) {
        SparkConf sc=new SparkConf().setAppName("SparkReadUtils").setMaster("local[*]");
//        JavaStreamingContext jssc=new JavaStreamingContext(sc,new )
    }
}
