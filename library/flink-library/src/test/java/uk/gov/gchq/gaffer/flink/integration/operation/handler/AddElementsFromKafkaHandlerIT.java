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
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.flink.operation.FlinkTest;
import uk.gov.gchq.gaffer.generator.TestBytesGeneratorImpl;
import uk.gov.gchq.gaffer.generator.TestGeneratorImpl;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromKafka;
import uk.gov.gchq.gaffer.operation.impl.add.ByteArrayEndOfElementsIndicator;
import uk.gov.gchq.gaffer.operation.impl.add.EndOfElementsIndicator;
import uk.gov.gchq.gaffer.operation.impl.add.StringEndOfElementsIndicator;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

import static uk.gov.gchq.gaffer.flink.operation.handler.util.FlinkConstants.START_FROM_EARLIEST;
import static uk.gov.gchq.gaffer.flink.operation.handler.util.FlinkConstants.SYNCHRONOUS_SINK;

public class AddElementsFromKafkaHandlerIT extends FlinkTest {
    private static final String TOPIC = UUID.randomUUID().toString();
    private static final String GROUP = "groupId";

    @Rule
    public final TemporaryFolder zookeeperFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    @Rule
    public final TemporaryFolder kafkaLogDir = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    @Rule
    public final RetryRule rule = new RetryRule();

    private KafkaProducer<Integer, String> producer;
    private KafkaServer kafkaServer;
    private TestingServer zkServer;
    private String bootstrapServers;

    @Before
    public void before() throws Exception {
        bootstrapServers = "localhost:" + getOpenPort();

        // Create zookeeper server
        zkServer = new TestingServer(-1, createZookeeperTmpDir());
        zkServer.start();

        // Create kafka server
        kafkaServer = TestUtils.createServer(new KafkaConfig(serverProperties()), new MockTime());

        MapStore.resetStaticMap();
    }

    @After
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
    }

    @Test
    public void shouldAddElementsWithStringConsumer() throws Exception {
        shouldAddElements(String.class, TestGeneratorImpl.class, StringEndOfElementsIndicator.class, StringSerializer.class);
    }

    @Test
    public void shouldAddElementsWithByteArrayConsumer() throws Exception {
        shouldAddElements(byte[].class, TestBytesGeneratorImpl.class, ByteArrayEndOfElementsIndicator.class, ByteArraySerializer.class);
    }

    protected <T> void shouldAddElements(
            final Class<T> consumeAs,
            final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> elementGenerator,
            final Class<? extends EndOfElementsIndicator<T>> endOfElementsIndicator,
            final Class<? extends Serializer> producerValueSerialiser) throws Exception {
        // Given
        final Graph graph = createGraph();
        final boolean validate = true;
        final boolean skipInvalid = false;

        final AddElementsFromKafka op = new AddElementsFromKafka.Builder()
                .generator(consumeAs, elementGenerator, endOfElementsIndicator)
                .parallelism(1)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .topic(TOPIC)
                .bootstrapServers(bootstrapServers)
                .groupId(GROUP)
                .option(SYNCHRONOUS_SINK, Boolean.TRUE.toString())
                .option(START_FROM_EARLIEST, Boolean.TRUE.toString())
                .build();

        // When
        final Thread graphOperationThread = new Thread(() -> {
            try {
                graph.execute(op, new User());
            } catch (final OperationException e) {
                throw new RuntimeException(e);
            }
        });

        graphOperationThread.start();

        producer = new KafkaProducer<>(producerProps(producerValueSerialiser));
        for (final String dataValue : DATA_VALUES) {
            final ProducerRecord record = createProducerRecord(consumeAs, dataValue);
            producer.send(record).get();
        }
        producer.send(new ProducerRecord(TOPIC, endOfElementsIndicator.newInstance().get())).get();

        /* Wait for consumer to complete processing */
        graphOperationThread.join();

        verifyElements(graph);
    }

    private <T> ProducerRecord<Integer, T> createProducerRecord(final Class<T> consumeAs, final String dataValue) {
        return String.class == consumeAs
                ? new ProducerRecord(TOPIC, dataValue)
                : new ProducerRecord(TOPIC, dataValue.getBytes());
    }

    private File createZookeeperTmpDir() throws IOException {
        zookeeperFolder.delete();
        zookeeperFolder.create();
        return zookeeperFolder.newFolder("zkTmpDir");
    }

    private Properties producerProps(final Class<? extends Serializer> valueSerialiserClass) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", valueSerialiserClass.getName());
        return props;
    }

    private Properties serverProperties() throws IOException {
        kafkaLogDir.delete();
        kafkaLogDir.create();
        Properties props = new Properties();
        props.put("zookeeper.connect", zkServer.getConnectString());
        props.put("broker.id", "0");
        props.setProperty("listeners", "PLAINTEXT://" + bootstrapServers);
        props.put(KafkaConfig.LogDirProp(), kafkaLogDir.newFolder().getPath());
        return props;
    }

    private static int getOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
