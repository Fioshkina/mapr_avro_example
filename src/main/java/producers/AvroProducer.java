package producers;

import com.example.Employee;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by user102265 on 17.01.19.
 */
public class AvroProducer implements Runnable {
    private final KafkaProducer<Integer, Employee> producer;
    private final String topic;
    private final int n;

    public AvroProducer(String topic, int n) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        properties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8087");
        producer = new KafkaProducer<>(properties);
        this.topic = topic;
        this.n = n;
    }

    @Override
    public void run() {
        Employee employee;

        for (int i = 0; i < n; i++) {
            List<String> emails = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                emails.add("john" + j + ".doe" + i + "@mail.com");
            }

            employee = Employee.newBuilder()
                    .setFirstName("John" + i)
                    .setLastName("Doe")
                    .setAge(i + 5)
                    .setEmails(emails)
                    .setPhoneNumber("+1-202-555-" + i + i + i + i)
                    .build();

            ProducerRecord<Integer, Employee> record = new ProducerRecord(topic, i, employee);
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("Success!" );
                    System.out.println(recordMetadata.toString());
                } else {
                    e.printStackTrace();
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
