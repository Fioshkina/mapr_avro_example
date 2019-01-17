import consumers.AvroConsumer;
import producers.AvroProducer;

/**
 * Created by user102265 on 17.01.19.
 */
public class Main {
    public static void main(String[] args) {
        String topic = "/sample-stream:avro_example";

        AvroProducer producer = new AvroProducer(topic, 10);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        AvroConsumer consumer = new AvroConsumer(topic);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {

        }
    }
}
