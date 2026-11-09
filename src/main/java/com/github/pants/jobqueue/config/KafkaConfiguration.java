package com.github.pants.jobqueue.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Kafka infrastructure configuration.
 *
 * <p>The producer is configured with idempotence and all acknowledgements. Consumers
 * disable auto-commit and use manual acknowledgements, so offsets are committed only
 * after the application has made a durable state transition.</p>
 */
@Configuration
public class KafkaConfiguration {

    /**
     * Creates Kafka admin configuration used by Spring to create the queue topics.
     *
     * @param properties application queue properties.
     * @return Kafka admin bean.
     */
    @Bean
    public KafkaAdmin kafkaAdmin(final JobQueueProperties properties) {
        final Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        return new KafkaAdmin(config);
    }

    /**
     * Creates all application topics on startup when broker auto-creation is disabled.
     *
     * @param properties application queue properties.
     * @return topic definitions grouped in one bean.
     */
    @Bean
    public KafkaAdmin.NewTopics jobQueueTopics(final JobQueueProperties properties) {
        final int partitions = properties.getKafka().getTopicPartitions();
        final short replicas = properties.getKafka().getTopicReplicas();
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(properties.getTopics().getHigh()).partitions(partitions).replicas(replicas).build(),
                TopicBuilder.name(properties.getTopics().getLow()).partitions(partitions).replicas(replicas).build(),
                TopicBuilder.name(properties.getTopics().getRetry5s()).partitions(partitions).replicas(replicas).build(),
                TopicBuilder.name(properties.getTopics().getRetry30s()).partitions(partitions).replicas(replicas).build(),
                TopicBuilder.name(properties.getTopics().getRetry2m()).partitions(partitions).replicas(replicas).build(),
                TopicBuilder.name(properties.getTopics().getDlq()).partitions(partitions).replicas(replicas).build());
    }

    /**
     * Producer factory with Kafka idempotent producer settings enabled.
     *
     * @param properties application queue properties.
     * @return producer factory for string keys and JSON values.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory(final JobQueueProperties properties) {
        final Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * Kafka template used by the transactional outbox publisher.
     *
     * @param producerFactory producer factory.
     * @return string Kafka template.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(final ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Consumer factory shared by all listener containers.
     *
     * @param properties application queue properties.
     * @return consumer factory for string keys and JSON values.
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory(final JobQueueProperties properties) {
        final Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getKafka().getConsumerGroupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getKafka().getMaxPollRecords());
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * Listener container factory for high-priority jobs.
     *
     * @param consumerFactory consumer factory.
     * @param properties application queue properties.
     * @return listener container factory.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> highPriorityKafkaListenerContainerFactory(
            final ConsumerFactory<String, String> consumerFactory,
            final JobQueueProperties properties) {
        return listenerContainerFactory(consumerFactory, properties.getWorker().getHighConcurrency());
    }

    /**
     * Listener container factory for low-priority jobs.
     *
     * @param consumerFactory consumer factory.
     * @param properties application queue properties.
     * @return listener container factory.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> lowPriorityKafkaListenerContainerFactory(
            final ConsumerFactory<String, String> consumerFactory,
            final JobQueueProperties properties) {
        return listenerContainerFactory(consumerFactory, properties.getWorker().getLowConcurrency());
    }

    /**
     * Listener container factory for retry topics.
     *
     * @param consumerFactory consumer factory.
     * @param properties application queue properties.
     * @return listener container factory.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> retryKafkaListenerContainerFactory(
            final ConsumerFactory<String, String> consumerFactory,
            final JobQueueProperties properties) {
        return listenerContainerFactory(consumerFactory, properties.getWorker().getRetryConcurrency());
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory(
            final ConsumerFactory<String, String> consumerFactory,
            final int concurrency) {
        final ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // These factories are built by hand, so Boot's spring.threads.virtual.enabled does not reach
        // them. Concurrency still bounds the consumer count; each consumer now runs on a virtual thread.
        final SimpleAsyncTaskExecutor consumerExecutor = new SimpleAsyncTaskExecutor("kafka-consumer-");
        consumerExecutor.setVirtualThreads(true);
        factory.getContainerProperties().setListenerTaskExecutor(consumerExecutor);
        return factory;
    }
}
