package cn.spark.maindemo;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 引入kafka-clients jars  后直接可以生产消息
 */
public class ProductorClient {
    public static void main(String[] args) {
        Properties props=new Properties();
        props.put("bootstrap.servers", "192.168.12.102:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
            producer.send(new ProducerRecord<String, String>("test", "1",SendMessage()));
        producer.close();
    }

    public static String SendMessage(){
        String serverName ="root"; //request.getServerName();
        Integer serverPort = 8080;//request.getServerPort();
        String serverPath = "/";//request.getContextPath();

        String localName = "root"; //request.getLocalName();
        String localAddr ="root"; // request.getLocalAddr();
        Integer localPort = 1;//request.getLocalPort();

        String remoteHost = "root"; //request.getRemoteHost(); // 客户端主机名
        String remoteAddr = "root"; //request.getRemoteAddr(); // 客户端ip地址
        Integer remotePort =1;// request.getRemotePort(); // 客户端端口号

        String protocol = "root"; //request.getProtocol();
        String schema = "root"; //request.getScheme();

        String method ="root"; // request.getMethod();
//        Map<String, String[]> parameterMap = request.getParameterMap();
        String queryString ="root"; // request.getQueryString();

        String requestURI = "root"; //request.getRequestURI();
        String requestURL ="root"; // request.getRequestURL().toString();

        // 构造 map
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("requestHeaderMap","root"); // requestHeaderMap);
        map.put("responseHeaderMap", "root"); //responseHeaderMap);
        map.put("serverName", serverName);
        map.put("serverPort", serverPort);
        map.put("serverPath", serverPath);
        map.put("localName", localName);
        map.put("localAddr", localAddr);
        map.put("localPort", localPort);
        map.put("remoteHost", remoteHost);
        map.put("remoteAddr", remoteAddr);
        map.put("remotePort", remotePort);
        map.put("protocol", protocol);
        map.put("schema", schema);
        map.put("method", method);
        map.put("parameterMap", "12");//parameterMap);
        map.put("queryString", queryString);
        map.put("requestURI", requestURI);
        map.put("requestURL", requestURL);
        map.put("visitDate", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
        return JSONObject.toJSONStringWithDateFormat(map,"yyyy-MM-dd HH:mm:ss.SSS");
    }
}
