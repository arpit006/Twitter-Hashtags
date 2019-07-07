package com.kafka.twitterkafka.kafka.bean;

import com.kafka.twitterkafka.kafka.config.ProducerConfiguration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Component
public class KafkaBean {

    @Autowired
    private ProducerConfiguration producerConfiguration;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerConfiguration.getBOOTSTRAP_SERVER());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerConfiguration.getKEY_SERIALIZER());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerConfiguration.getVALUE_SERIALIZER());
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, producerConfiguration.getRETRY_BACK_TIME());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerConfiguration.getCLIENT_ID());
        //safe-producer.  set retries to Integer.MAX_VALUE
        properties.put(ProducerConfig.RETRIES_CONFIG, producerConfiguration.getRETRIES());
        properties.put(ProducerConfig.ACKS_CONFIG, producerConfiguration.getACKS_CONFIG());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producerConfiguration.getENABLE_IDEMPOTENCE());
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, producerConfiguration.getMAX_REQUEST_PER_CONNECTION());

        //kafka high throughput producer TODO: Test high Producer Throughput at the cost of latency and CPU usage
        /*properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));*/
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
