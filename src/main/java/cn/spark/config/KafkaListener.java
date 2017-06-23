package cn.spark.config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.Optional;

public class KafkaListener {

    @org.springframework.kafka.annotation.KafkaListener(topics = {"test"})
    public void KafkaListener(ConsumerRecord<?, ?> record) {

        Optional<?> messages = Optional.ofNullable(record.value());

        if (messages.isPresent()) {
            Object msg = messages.get();
            System.out.println("  this is the testTopic send message: " + msg);
        }
    }
}
