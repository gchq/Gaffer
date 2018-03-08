/*
 * Copyright 2017-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class AddElementsFromKafkaHandlerIT extends FlinkTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
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
        shouldAddElements(String.class, TestGeneratorImpl.class);
    }

    @Test
    public void shouldAddElementsWithByteArrayConsumer() throws Exception {
        shouldAddElements(byte[].class, TestBytesGeneratorImpl.class);
    }

    protected <T> void shouldAddElements(final Class<T> consumeAs, final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> elementGenerator) throws Exception {
        // Given
        final Graph graph = createGraph();
        final boolean validate = true;
        final boolean skipInvalid = false;
        final String topic = UUID.randomUUID().toString();

        final AddElementsFromKafka op = new AddElementsFromKafka.Builder()
                .generator(consumeAs, elementGenerator)
                .parallelism(1)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .topic(topic)
                .bootstrapServers(bootstrapServers)
                .groupId("groupId")
                .build();

        // When
        new Thread(() -> {
            try {
                Thread.sleep(10000);
                graph.execute(op, new User());
            } catch (final OperationException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(20000);
                // Create kafka producer and add some data
                producer = new KafkaProducer<>(producerProps());
                for (final String dataValue : DATA_VALUES) {
                    producer.send(new ProducerRecord<>(topic, dataValue)).get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).start();

        // Then
        Thread.sleep(20000);
        try {
            verifyElements(graph);
        } catch (final AssertionError e) {
            Thread.sleep(60000);
            verifyElements(graph);
        }
    }

    private File createZookeeperTmpDir() throws IOException {
        testFolder.delete();
        testFolder.create();
        return testFolder.newFolder("zkTmpDir");
    }

    private Properties producerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private Properties serverProperties() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkServer.getConnectString());
        props.put("broker.id", "0");
        props.setProperty("listeners", "PLAINTEXT://" + bootstrapServers);
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
