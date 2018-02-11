package blackhole.base.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Should be a thread safe.
 * @author madassar
 *
 */
public class KafkaProducerBase {

	private Producer<String, String> producer;

	protected KafkaProducerBase(String bootstrapServers) {
		this(buildProps(bootstrapServers));
	}

	protected KafkaProducerBase(Properties props) {
		producer = new KafkaProducer<>(props);
	}

	public Producer getProducer() {
		return producer;
	}

	public void destroy() {
		this.producer.close();
	}

	public static void main(String args[]) {
		KafkaProducerBase kpb = new KafkaProducerBase("127.0.0.1:2181");
		Producer<String, String> producer = kpb.getProducer();
		for (int i = 0; i < 100; i++)
			producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
		kpb.destroy();
	}

	private static Properties buildProps(String bootstrapServers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:2181");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}
}
