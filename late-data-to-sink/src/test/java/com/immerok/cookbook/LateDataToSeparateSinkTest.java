package com.immerok.cookbook;

import static com.immerok.cookbook.LateDataToSeparateSink.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventDeserializationSchema;
import com.immerok.cookbook.events.EventSupplier;
import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import com.immerok.cookbook.utils.DataStreamCollectUtil;
import com.immerok.cookbook.utils.DataStreamCollector;
import java.util.stream.Stream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FlinkMiniClusterExtension.class)
class LateDataToSeparateSinkTest {

    /**
     * Runs the production job against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(TOPIC, Stream.generate(new EventSupplier(LATE_ELEMENT_DATA)));

            LateDataToSeparateSink.runJob();
        }
    }

    private static final String LATE_ELEMENT_DATA = "Eagles";

    @Test
    void JobProducesAtLeastOneResult() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {

            kafka.createTopicAsync(TOPIC, Stream.generate(new EventSupplier(LATE_ELEMENT_DATA)));

            KafkaSource<Event> source =
                    KafkaSource.<Event>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setValueOnlyDeserializer(new EventDeserializationSchema())
                            .build();

            final DataStreamCollectUtil dataStreamCollector = new DataStreamCollectUtil();

            final DataStreamCollector<Event> mainTestSink = new DataStreamCollector<>();
            final DataStreamCollector<Event> lateTestSink = new DataStreamCollector<>();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            LateDataToSeparateSink.defineWorkflow(
                    env,
                    source,
                    mainWorkflowOutput ->
                            dataStreamCollector.collectAsync(mainWorkflowOutput, mainTestSink),
                    lateWorkflowInput ->
                            dataStreamCollector.collectAsync(lateWorkflowInput, lateTestSink));
            final JobClient jobClient = env.executeAsync();

            dataStreamCollector.startCollect(jobClient);

            try (final CloseableIterator<Event> mainEvents = mainTestSink.getOutput();
                    final CloseableIterator<Event> lateEvents = lateTestSink.getOutput()) {

                // verify that all late elements ended up in the lateElements stream
                for (int x = 0; x < 100; x++) {
                    assertThat(mainEvents.hasNext()).isTrue();
                    assertThat(mainEvents.next().data).doesNotContain(LATE_ELEMENT_DATA);
                }
                for (int x = 0; x < 10; x++) {
                    assertThat(lateEvents.hasNext()).isTrue();
                    assertThat(lateEvents.next().data).isEqualTo(LATE_ELEMENT_DATA);
                }
            }
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
