package com.immerok.cookbook;

import static com.immerok.cookbook.TableDeduplicatedJoin.CUSTOMER_TOPIC;
import static com.immerok.cookbook.TableDeduplicatedJoin.TRANSACTION_TOPIC;
import static com.immerok.cookbook.records.CustomerSupplier.TOTAL_CUSTOMERS;

import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.records.CustomerSupplier;
import com.immerok.cookbook.records.DuplicatingTransactionSupplier;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(FlinkMiniClusterExtension.class)
class CompiledPlanTest {

    /**
     * Set an explicit path here if you want the plan file to be available after running these tests
     */
    private static Path planLocation;

    @BeforeAll
    public static void setPlanLocation() {
        if (planLocation == null) {
            try {
                planLocation = Files.createTempFile("plan", ".json");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Creates a compiled JSON plan file for a streaming Table application. Note that the Kafka
     * cluster and relevant topics don't need to exist when this code is run -- Kafka is only
     * necessary later when the plan is executed.
     */
    @Test
    @Order(1)
    public void compileAndWritePlan() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().set("table.plan.force-recompile", "true");

        String customersDDL =
                String.join(
                        "\n",
                        "CREATE TABLE Customers (",
                        "  c_id BIGINT,",
                        "  c_name STRING",
                        ") WITH (",
                        "  'connector' = 'kafka',",
                        "  'topic' = 'customers',",
                        "  'properties.bootstrap.servers' = 'localhost:9092',",
                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'format' = 'json',",
                        "  'json.timestamp-format.standard' = 'ISO-8601'",
                        ")");

        String transactionsDDL =
                String.join(
                        "\n",
                        "CREATE TABLE Transactions (",
                        "  t_time TIMESTAMP_LTZ(3),",
                        "  t_id BIGINT,",
                        "  t_customer_id BIGINT,",
                        "  t_amount DECIMAL",
                        ") WITH (",
                        "  'connector' = 'kafka',",
                        "  'topic' = 'transactions',",
                        "  'properties.bootstrap.servers' = 'localhost:9092',",
                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'format' = 'json',",
                        "  'json.timestamp-format.standard' = 'ISO-8601'",
                        ")");

        String printSinkDDL =
                String.join(
                        "\n",
                        "CREATE TABLE print_sink (",
                        "  t_id BIGINT,",
                        "  c_name STRING,",
                        "  t_amount DECIMAL(5, 2)",
                        ") WITH (",
                        "  'connector' = 'print'",
                        ")");

        tableEnv.executeSql(customersDDL);
        tableEnv.executeSql(transactionsDDL);
        tableEnv.executeSql(printSinkDDL);

        String streamingQueryWithInsert =
                String.join(
                        "\n",
                        "INSERT INTO print_sink",
                        "SELECT t_id, c_name, CAST(t_amount AS DECIMAL(5, 2))",
                        "FROM Customers",
                        "JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id");

        tableEnv.compilePlanSql(streamingQueryWithInsert).writeToFile(planLocation);
    }

    /**
     * Loads and executes the compiled json plan written out by compileAndWritePlan(), which needs
     * to be run first.
     */
    @Test
    @Order(2)
    public void loadAndExecutePlan() throws ExecutionException, InterruptedException {

        // Compiled plans can only be executed in streaming mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        final int numberOfDuplicatedTransactions = 10;

        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {

            // Create and serve bounded streams of Customer and Transaction data
            kafka.createTopic(
                    CUSTOMER_TOPIC, Stream.generate(new CustomerSupplier()).limit(TOTAL_CUSTOMERS));
            kafka.createTopic(
                    TRANSACTION_TOPIC,
                    Stream.generate(new DuplicatingTransactionSupplier())
                            .limit(numberOfDuplicatedTransactions));

            // Start executing the job (asynchronously)
            TableResult execution = tableEnv.executePlan(PlanReference.fromFile(planLocation));
            final JobClient jobClient = execution.getJobClient().get();

            // Wait and watch as the results are printed out
            try {
                Thread.sleep(1_000 * 5);
            } finally {
                jobClient.cancel();
            }
        }
    }
}
