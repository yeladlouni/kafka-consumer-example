import jdk.jfr.Timespan;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class BasicConsumer {
    public static void main(String[] args) {
        System.out.println("======Starting Basic Consumer=====");

        Properties settings = new Properties();
        settings.put("group.id", "group1");
        settings.put("bootstrap.servers", "localhost:9092");
        settings.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        settings.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        settings.put("auto.commit.interval.ms", 5000);
        settings.put("auto.offset.reset", "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(settings);

        ArrayList topics = new ArrayList();
        topics.add("hello_world_topic");

        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records) {
                System.out.println(record.topic() + ":" + record.partition() + ":" + record.offset() + ":" + record.key() + ":" + record.value());
            }
        }
    }
}
