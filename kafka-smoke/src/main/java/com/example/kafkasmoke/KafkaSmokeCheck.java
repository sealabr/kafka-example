package com.example.kafkasmoke;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaSmokeCheck {

	private static final String BOOTSTRAP = "localhost:9092";
	private static final String TOPIC = "smoke.check";

	private KafkaSmokeCheck() {
	}

	public static void main(String[] args) throws Exception {
		String payload = "smoke-" + UUID.randomUUID();

		System.out.println("Kafka smoke check");
		System.out.println("  bootstrap: " + BOOTSTRAP);
		System.out.println("  topic:     " + TOPIC);
		System.out.println("  payload:   " + payload);

		ensureTopic();
		produce(payload);
		String received = consume(payload);

		if (payload.equals(received)) {
			System.out.println("OK — Kafka is working (produce + consume).");
			return;
		}

		System.err.println("FAIL — expected [" + payload + "] but got [" + received + "]");
		System.exit(1);
	}

	private static void ensureTopic() throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);

		try (AdminClient admin = AdminClient.create(props)) {
			if (admin.listTopics().names().get(15, TimeUnit.SECONDS).contains(TOPIC)) {
				System.out.println("Topic already exists.");
				return;
			}
			admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)))
					.all()
					.get(15, TimeUnit.SECONDS);
			System.out.println("Topic ready.");
		}
	}

	private static void produce(String payload) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.ACKS_CONFIG, "all");

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			producer.send(new ProducerRecord<>(TOPIC, "smoke-key", payload)).get(15, TimeUnit.SECONDS);
			System.out.println("Produced.");
		}
	}

	private static String consume(String expected) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-smoke-" + UUID.randomUUID());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(List.of(TOPIC));

			long deadline = System.currentTimeMillis() + 20_000;
			while (System.currentTimeMillis() < deadline) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
				for (ConsumerRecord<String, String> record : records) {
					if (expected.equals(record.value())) {
						System.out.println("Consumed from partition " + record.partition()
								+ " offset " + record.offset());
						return record.value();
					}
				}
			}
		}

		return null;
	}
}
