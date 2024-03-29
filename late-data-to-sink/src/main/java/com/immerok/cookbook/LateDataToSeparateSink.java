package com.immerok.cookbook;

import com.immerok.cookbook.events.Event;
import com.immerok.cookbook.events.EventDeserializationSchema;
import com.immerok.cookbook.sinks.PrintSink;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LateDataToSeparateSink {

    static final String TOPIC = "input";

    public static void main(String[] args) throws Exception {
        runJob();
    }

    static void runJob() throws Exception {
        KafkaSource<Event> source =
                KafkaSource.<Event>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new EventDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineWorkflow(
                env,
                source,
                workflow -> workflow.sinkTo(new PrintSink<>()),
                workflow -> workflow.sinkTo(new PrintSink<>("lateData")));
        env.execute();
    }

    static void defineWorkflow(
            StreamExecutionEnvironment env,
            Source<Event, ?, ?> source,
            Consumer<DataStream<Event>> mainSinkApplier,
            Consumer<DataStream<Event>> lateSinkApplier) {
        final OutputTag<Event> lateDataOutputTag = new OutputTag<>("lateData") {};

        final SingleOutputStreamOperator<Event> events =
                env.fromSource(
                                source,
                                WatermarkStrategy.<Event>forMonotonousTimestamps()
                                        // extract timestamp from the record
                                        // required so that we can introduce late data
                                        .withTimestampAssigner(
                                                (element, recordTimestamp) ->
                                                        element.timestamp.toEpochMilli()),
                                "Kafka")
                        .setParallelism(1)
                        .process(new LateDataSplittingProcessFunction(lateDataOutputTag));

        final DataStream<Event> lateEvents = events.getSideOutput(lateDataOutputTag);

        // additional workflow steps go here

        mainSinkApplier.accept(events);
        lateSinkApplier.accept(lateEvents);
    }

    private static class LateDataSplittingProcessFunction extends ProcessFunction<Event, Event> {

        private final OutputTag<Event> lateDataOutputTag;

        private LateDataSplittingProcessFunction(OutputTag<Event> lateDataOutputTag) {
            this.lateDataOutputTag = lateDataOutputTag;
        }

        @Override
        public void processElement(
                Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) {
            final long currentWatermark = ctx.timerService().currentWatermark();

            long eventTimestamp = ctx.timestamp();

            if (eventTimestamp < currentWatermark) {
                ctx.output(lateDataOutputTag, value);
            } else {
                out.collect(value);
            }
        }
    }
}
