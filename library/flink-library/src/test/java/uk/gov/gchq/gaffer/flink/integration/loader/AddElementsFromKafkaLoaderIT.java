/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.flink.integration.loader;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.integration.impl.loader.ParameterizedLoaderIT;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.SchemaLoader;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromKafka;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.TestSchema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class AddElementsFromKafkaLoaderIT extends ParameterizedLoaderIT<AddElementsFromKafka> {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private KafkaProducer<Integer, String> producer;
    private KafkaServer kafkaServer;
    private TestingServer zkServer;
    private String bootstrapServers;

    public AddElementsFromKafkaLoaderIT(final TestSchema schema, final SchemaLoader loader, final Map<String, User> userMap) {
        super(schema, loader, userMap);
        StoreProperties props = getStoreProperties();
        props.addOperationDeclarationPaths("../../library/flink-library/src/main/resources/FlinkOperationDeclarations.json");
        setStoreProperties(props);
    }

    @Override
    protected void addElements(final Iterable<? extends Element> input) throws OperationException {
        try {
            configure();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        add(input);
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

    private void configure() throws Exception {

        bootstrapServers = "localhost:" + getOpenPort();

        // Create zookeeper server
        zkServer = new TestingServer(-1, createZookeeperTmpDir());
        zkServer.start();

        // Create kafka server
        kafkaServer = TestUtils.createServer(new KafkaConfig(serverProperties()), new MockTime());
    }

    protected void add(final Iterable<? extends Element> input) {
        final AddElementsFromKafka op = new AddElementsFromKafka.Builder()
                .generator(String.class, GeneratorImpl.class)
                .parallelism(1)
                .validate(false)
                .skipInvalidElements(false)
                .topic(UUID.randomUUID().toString())
                .bootstrapServers(bootstrapServers)
                .groupId("groupId")
                .build();

        new Thread(() -> {
            try {
                Thread.sleep(10000);
                graph.execute(op, getUser());
            } catch (final OperationException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(20000);
                // Create kafka producer and add some data
                producer = new KafkaProducer<>(producerProps());

                final List<String> vertices = new ArrayList<>();
                for (final Element element : input) {
                    if (element instanceof Entity) {
                        vertices.add(((Entity) element).getVertex().toString());
                    }
                }

                for (final String dataValue : vertices) {
                    producer.send(new ProducerRecord<>(op.getTopic(), dataValue)).get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).start();

        // Wait....
        try {
            Thread.sleep(30000);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
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