/*
 * Copyright 2017-2020 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.flink.integration.operation.handler;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.flink.operation.FlinkTest;
import uk.gov.gchq.gaffer.flink.operation.TestFileSink;
import uk.gov.gchq.gaffer.flink.operation.handler.AddElementsFromKafkaHandler;
import uk.gov.gchq.gaffer.generator.TestBytesGeneratorImpl;
import uk.gov.gchq.gaffer.generator.TestGeneratorImpl;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromKafka;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class AddElementsFromKafkaHandlerIT extends FlinkTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromKafkaHandlerIT.class);
    private static final long TEST_TIMEOUT_MS = 10000L;
    private static final long WAIT_MS = 1000L;
    private static final String GROUP_ID = "groupId";
    private static final String TOPIC = UUID.randomUUID().toString();

    private final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

    private KafkaProducer<Integer, String> producer;
    private KafkaServer kafkaServer;
    private TestingServer zkServer;
    private String bootstrapServers;
    private TestFileSink testFileSink;

    @BeforeEach
    public void before() throws Exception {
        bootstrapServers = "localhost:" + getOpenPort();

        // Create zookeeper server
        zkServer = new TestingServer(-1, createTemporaryDirectory("zkTmpDir"));
        zkServer.start();

        testFileSink = createTestFileSink();

        // Create kafka server
        kafkaServer = TestUtils.createServer(new KafkaConfig(serverProperties()), new MockTime());

        MapStore.resetStaticMap();
    }

    @AfterEach
    public void cleanUp() throws IOException {
        if (null != producer) {
            producer.close();
        }

        if (null != kafkaServer) {
            kafkaServer.shutdown();
        }

        if (null != zkServer) {
            zkServer.close();
        }

        unregisterMBeans();
    }

    @Test()
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    public void shouldAddElementsWithStringConsumer() throws Exception {
        shouldAddElements(String.class, TestGeneratorImpl.class, StringSerializer.class);
    }

    @Test()
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    public void shouldAddElementsWithByteArrayConsumer() throws Exception {
        shouldAddElements(byte[].class, TestBytesGeneratorImpl.class, ByteArraySerializer.class);
    }

    protected <T> void shouldAddElements(
            final Class<T> consumeAs,
            final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> elementGenerator,
            final Class<? extends Serializer> valueSerialiser) throws Exception {
        // Given
        final Graph graph = createGraph();
        final boolean validate = true;
        final boolean skipInvalid = false;

        final AddElementsFromKafka op = new AddElementsFromKafka.Builder()
                .generator(consumeAs, elementGenerator)
                .parallelism(1)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .topic(TOPIC)
                .bootstrapServers(bootstrapServers)
                .groupId(GROUP_ID)
                .build();

        // When
        new Thread(() -> {
            try {
                graph.execute(op, new User());
            } catch (final OperationException e) {
                throw new RuntimeException(e);
            }
        }).start();

        waitForOperationToStart();

        producer = new KafkaProducer<>(producerProps(valueSerialiser));
        for (final String dataValue : DATA_VALUES) {
            producer.send(new ProducerRecord(TOPIC, convert(consumeAs, dataValue))).get();
        }

        waitForElements(consumeAs, elementGenerator);

        // Then
        verifyElements(consumeAs, testFileSink, elementGenerator);
    }

    private <T> T convert(final Class<T> valueClass, final String value) {
        return (valueClass.equals(String.class)) ? (T) value : (T) value.getBytes();
    }

    private Properties producerProps(final Class<? extends Serializer> valueSerialiser) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", valueSerialiser);
        return props;
    }

    private Properties serverProperties() throws IOException {
        final Properties props = new Properties();
        props.put("zookeeper.connect", zkServer.getConnectString());
        props.put("broker.id", "0");
        props.setProperty("listeners", "PLAINTEXT://" + bootstrapServers);
        props.put(KafkaConfig.LogDirProp(), createTemporaryDirectory("kafkaLogDir").getPath());
        return props;
    }

    private static int getOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForOperationToStart() throws Exception {
        while (!consumerConnected()) {
            LOGGER.info("Waiting for Operation to start, sleeping for {} ms", WAIT_MS);
            Thread.sleep(WAIT_MS);
        }
    }

    private <T> void waitForElements(
            final Class<T> consumeAs,
            final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> elementGenerator) throws Exception {
        while (!waitForElements(consumeAs, testFileSink, elementGenerator)) {
            LOGGER.info("Waiting for Elements to be stored, sleeping for {} ms", WAIT_MS);
            Thread.sleep(WAIT_MS);
        }
    }

    @Override
    public Store createStore() {
        final Store store = Store.createStore("graphId", SCHEMA, MapStoreProperties.loadStoreProperties("store.properties"));
        store.addOperationHandler(AddElementsFromKafka.class, new AddElementsFromKafkaHandler(createJmxEnabledExecutionEnvironment(), testFileSink));
        return store;
    }

    private StreamExecutionEnvironment createJmxEnabledExecutionEnvironment() {
        final Configuration configuration = new Configuration();
        configuration.setString("metrics.reporters", "jmx");
        configuration.setString("metrics.reporter.jmx.class", JMXReporter.class.getName());
        return StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
    }

    private void unregisterMBeans() {
        for (ObjectName name : platformMBeanServer.queryNames(null, null)) {
            try {
                platformMBeanServer.unregisterMBean(name);
            } catch (Exception e) {
                /* intentional */
            }
        }
    }

    private boolean consumerConnected() throws Exception {
        return !platformMBeanServer.queryNames(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,*,topic=".concat(TOPIC)), null).isEmpty();
    }
}
