package com.immerok.cookbook.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Iterator;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.SendValues;
import net.mguenther.kafka.junit.TopicConfig;

/** A slim wrapper around <a href="https://mguenther.github.io/kafka-junit/">kafka-junit</a>. */
public class CookbookKafkaCluster extends EmbeddedKafkaCluster {

    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder().build().registerModule(new JavaTimeModule());

    public CookbookKafkaCluster() {
        super(EmbeddedKafkaClusterConfig.defaultClusterConfig());

        this.start();
    }

    /**
     * Creates a topic with the given name and asynchronously writes all data from the given
     * iterator to that topic.
     *
     * @param topic topic name
     * @param topicData topic data to write
     * @param <EVENT> event data type
     */
    public <EVENT> void createTopic(String topic, Iterator<EVENT> topicData) {
        // eagerly create topic to prevent cases where the job fails because by the time it started
        // no value was written yet.
        createTopic(TopicConfig.withName(topic).build());
        new Thread(
                        () -> {
                            while (topicData.hasNext()) {
                                sendEventAsJSON(topic, topicData.next());
                            }
                        },
                        "Generator")
                .start();
    }

    /**
     * Sends one JSON-encoded event to the topic and sleeps for 100ms.
     *
     * @param event An event to send to the topic.
     */
    private <EVENT> void sendEventAsJSON(String topic, EVENT event) {
        try {
            final SendValues<String> sendRequest =
                    SendValues.to(topic, OBJECT_MAPPER.writeValueAsString(event)).build();
            this.send(sendRequest);
            Thread.sleep(1000);
        } catch (InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
