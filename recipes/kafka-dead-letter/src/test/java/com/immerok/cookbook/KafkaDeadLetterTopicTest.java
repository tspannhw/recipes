package com.immerok.cookbook;

import static com.immerok.cookbook.KafkaDeadLetterTopic.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventSupplier;
import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import com.immerok.cookbook.utils.DataStreamCollectUtil;
import com.immerok.cookbook.utils.DataStreamCollector;
import java.util.stream.Stream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KafkaDeadLetterTopicTest {
    @RegisterExtension
    static final FlinkMiniClusterExtension FLINK = new FlinkMiniClusterExtension();

    /**
     * Runs the production job against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(TOPIC, Stream.generate(new EventSupplier()));

            KafkaDeadLetterTopic.runJob();
        }
    }

    @Test
    void JobProducesAtLeastOneResult() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopic(TOPIC, Stream.generate(new EventSupplier()).limit(100));

            KafkaSource<String> source =
                    KafkaSource.<String>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            // set an upper bound so that the job (and this test) will end
                            .setBounded(OffsetsInitializer.latest())
                            .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build();

            final DataStreamCollectUtil dataStreamCollector = new DataStreamCollectUtil();

            final DataStreamCollector<Event> testSink = new DataStreamCollector<>();
            final DataStreamCollector<String> testErrorSink = new DataStreamCollector<>();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            KafkaDeadLetterTopic.defineWorkflow(
                    env,
                    source,
                    workflow -> dataStreamCollector.collectAsync(workflow, testSink),
                    errors -> dataStreamCollector.collectAsync(errors, testErrorSink));
            dataStreamCollector.startCollect(env.executeAsync());

            assertThat(testSink.getOutput()).toIterable().isNotEmpty();
            assertThat(testErrorSink.getOutput()).toIterable().isNotEmpty();
        }
    }

    /** Verify that Flink recognizes the Event type as a POJO that it can serialize efficiently. */
    @Test
    void EventsAreAPOJOs() {
        TypeSerializer<Event> eventSerializer =
                TypeInformation.of(Event.class).createSerializer(new ExecutionConfig());

        assertThat(eventSerializer).isInstanceOf(PojoSerializer.class);
    }
}
