package consumers;

import com.example.Employee;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by user102265 on 17.01.19.
 */
public class AvroConsumer implements Runnable {
    private final KafkaConsumer<Integer, Employee> consumer;

    public AvroConsumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8087");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<Integer, Employee> records = consumer.poll(100);
                records.forEach(record -> {

                    Employee employeeRecord = record.value();

                    System.out.printf("%s %d %d %s \n", record.topic(),
                            record.partition(), record.offset(), employeeRecord);

                });
            }
        } finally {
            consumer.close();
        }
    }
}
