package com.immerok.cookbook;

import static com.immerok.cookbook.SplitStream.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventSupplier;
import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import com.immerok.cookbook.utils.DataStreamCollectUtil;
import com.immerok.cookbook.utils.DataStreamCollector;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FlinkMiniClusterExtension.class)
class SplitStreamTest {

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

            SplitStream.runJob();
        }
    }

    @Test
    void JobProducesAtLeastOneResult() throws Exception {
        final DataStreamCollectUtil dataStreamCollector = new DataStreamCollectUtil();

        final DataStreamCollector<Event> criticalEventSink = new DataStreamCollector<>();
        final DataStreamCollector<Event> majorEventSink = new DataStreamCollector<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Event> source =
                env.fromElements(
                        new Event(1, "some critical event", Event.Priority.CRITICAL),
                        new Event(2, "some major event", Event.Priority.MAJOR),
                        new Event(3, "some minor event", Event.Priority.MINOR));

        SplitStream.defineWorkflow(
                source,
                workflow -> dataStreamCollector.collectAsync(workflow, criticalEventSink),
                workflow -> dataStreamCollector.collectAsync(workflow, majorEventSink));
        dataStreamCollector.startCollect(env.executeAsync());

        try (CloseableIterator<Event> criticalEvents = criticalEventSink.getOutput();
                CloseableIterator<Event> majorEvents = majorEventSink.getOutput()) {
            assertNotEmptyAndMatchPriority(criticalEvents, Event.Priority.CRITICAL);
            assertNotEmptyAndMatchPriority(majorEvents, Event.Priority.MAJOR);
        }
    }

    private static void assertNotEmptyAndMatchPriority(
            Iterator<Event> events, Event.Priority priority) {
        assertThat(events)
                .toIterable()
                .isNotEmpty()
                .allSatisfy(event -> assertThat(event.priority).isEqualTo(priority));
    }

    /** Verify that Flink recognizes the Event type as a POJO that it can serialize efficiently. */
    @Test
    void EventsAreAPOJOs() {
        TypeSerializer<Event> eventSerializer =
                TypeInformation.of(Event.class).createSerializer(new ExecutionConfig());

        assertThat(eventSerializer).isInstanceOf(PojoSerializer.class);
    }
}
