package io.github.aneudeveloper.func.engine.function;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaComsumerTestHelper {
    public List<ConsumerRecord<String, String>> getMessages(String topic, String consumerGroup) {
        // 1. Consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        List<ConsumerRecord<String, String>> returnVal = new ArrayList<>();
        // for (ConsumerRecords<String, String> records = consumer
        // .poll(Duration.ofMillis(1000)); records != null
        // && !records.isEmpty(); records = consumer.poll(Duration.ofMillis(1000))) {

        for (int i = 0; i < 10; i++) {
            ConsumerRecords<String, String> records = consumer
                    .poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                returnVal.add(record);
            }
        }
        // }

        consumer.close();

        return returnVal;
    }

    public ConsumerRecord<String, String> getFunc(List<ConsumerRecord<String, String>> messages, String funcName) {
        for (ConsumerRecord<String, String> message : messages) {

            Headers headers = message.headers();
            for (Header header : headers) {
                String key = header.key();
                String value = header.value() != null
                        ? new String(header.value())
                        : null;

                if (FuncEvent.FUNCTION.equals(key) && funcName.equals(value)) {
                    return message;
                }
            }

        }
        return null;
    }

     public ConsumerRecord<String, String> getFuncById(List<ConsumerRecord<String, String>> messages, String id) {
        for (ConsumerRecord<String, String> message : messages) {

            Headers headers = message.headers();
            for (Header header : headers) {
                String key = header.key();
                String value = header.value() != null
                        ? new String(header.value())
                        : null;

                if (FuncEvent.ID.equals(key) && id.equals(value)) {
                    return message;
                }
            }

        }
        return null;
    }

}
