/*
 * Copyright 2016-17 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.SparkConstants;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.function.Concat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GetRDDOfAllElementsHandlerTest {
    private final String USER_NAME = "user";
    private final String PASSWORD = "password";
    private final User USER = new User();
    private final String GRAPH_ID = "graphId";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void testGetAllElementsInRDD() throws OperationException, IOException, InterruptedException,
            AccumuloSecurityException, AccumuloException, TableNotFoundException {
        testGetAllElementsInRDD(getGraphForMockAccumulo(), getOperation());
        testGetAllElementsInRDD(getGraphForDirectRDD(), getOperationWithDirectRDDOption());
    }

    @Test
    public void testGetAllElementsInRDDWithView() throws OperationException, IOException, InterruptedException,
            AccumuloSecurityException, AccumuloException, TableNotFoundException {
        testGetAllElementsInRDDWithView(getGraphForMockAccumulo(), getOperation());
        testGetAllElementsInRDDWithView(getGraphForDirectRDD(), getOperationWithDirectRDDOption());
    }

    private void testGetAllElementsInRDD(final Graph graph, final GetRDDOfAllElements getRDD) throws OperationException,
            IOException, InterruptedException, AccumuloSecurityException, AccumuloException {
        final Set<Element> expectedElements = new HashSet<>(getElements());
        final RDD<Element> rdd = graph.execute(getRDD, USER);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>();
        final Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        assertEquals(expectedElements, results);

        getRDD.getSparkSession().stop();
    }

    private void testGetAllElementsInRDDWithView(final Graph graph, final GetRDDOfAllElements getRDD) throws OperationException,
            IOException, InterruptedException, AccumuloSecurityException, AccumuloException {
        final Set<Element> expectedElements = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(TestGroups.EDGE))
                .map(e -> (Edge) e)
                .map(e -> {
                    e.putProperty("newProperty", e.getSource().toString() + "," + e.getProperty(TestPropertyNames.COUNT));
                    return e;
                })
                .forEach(expectedElements::add);
        getRDD.setView(new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transientProperty("newProperty", String.class)
                        .transformer(new ElementTransformer.Builder()
                                .select(IdentifierType.SOURCE.name(), TestPropertyNames.COUNT)
                                .execute(new Concat())
                                .project("newProperty")
                                .build())
                        .build())
                .build());
        final RDD<Element> rdd = graph.execute(getRDD, USER);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>();
        final Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        assertEquals(expectedElements, results);

        getRDD.getSparkSession().stop();
    }

    private Graph getGraphForMockAccumulo() throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .addSchema(getClass().getResourceAsStream("/schema/elements.json"))
                .addSchema(getClass().getResourceAsStream("/schema/types.json"))
                .addSchema(getClass().getResourceAsStream("/schema/serialisation.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();
        final User user = new User();
        graph.execute(new AddElements.Builder().input(getElements()).build(), user);
        return graph;
    }

    private Graph getGraphForDirectRDD() throws InterruptedException, AccumuloException, AccumuloSecurityException,
            IOException, OperationException, TableNotFoundException {
        final MiniAccumuloCluster cluster = setUpMiniAccumulo();
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .addSchema(getClass().getResourceAsStream("/schema/elements.json"))
                .addSchema(getClass().getResourceAsStream("/schema/types.json"))
                .addSchema(getClass().getResourceAsStream("/schema/serialisation.json"))
                .storeProperties(getAccumuloProperties(cluster))
                .build();
        final User user = new User();
        graph.execute(new AddElements.Builder().input(getElements()).build(), user);
        cluster.getConnector("root", PASSWORD).tableOperations().compact(GRAPH_ID, new CompactionConfig());
        Thread.sleep(1000L);
        return graph;
    }

    private List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .build();

            final Edge edge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 2)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 4)
                    .build();

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);
        }
        return elements;
    }

    private GetRDDOfAllElements getOperation() throws IOException {
        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        configuration.write(new DataOutputStream(baos));
        final String configurationString = new String(baos.toByteArray(), CommonConstants.UTF_8);

        // Check get correct elements
        final GetRDDOfAllElements rddQuery = new GetRDDOfAllElements.Builder()
                .sparkSession(getSparkSession())
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        return rddQuery;
    }

    private GetRDDOfAllElements getOperationWithDirectRDDOption() throws IOException {
        final GetRDDOfAllElements op = getOperation();
        op.addOption(AbstractGetRDDHandler.USE_RFILE_READER_RDD, "true");
        return op;
    }

    private MiniAccumuloCluster setUpMiniAccumulo() throws IOException, InterruptedException, AccumuloSecurityException,
            AccumuloException {
        //  - Create MiniAccumuloCluster
        final MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempFolder.newFolder(), PASSWORD);
        final MiniAccumuloCluster cluster = new MiniAccumuloCluster(miniAccumuloConfig);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    cluster.stop();
                } catch (final IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        cluster.start();
        //  - Create user USER_NAME with permissions to create a table
        cluster.getConnector("root", PASSWORD).securityOperations().createLocalUser(USER_NAME, new PasswordToken(PASSWORD));
        cluster.getConnector("root", PASSWORD).securityOperations().grantSystemPermission(USER_NAME, SystemPermission.CREATE_TABLE);
        return cluster;
    }

    private AccumuloProperties getAccumuloProperties(final MiniAccumuloCluster cluster) {
        final AccumuloProperties properties = new AccumuloProperties();
        properties.setStoreClass(AccumuloStore.class);
        properties.setInstance(cluster.getInstanceName());
        properties.setZookeepers(cluster.getZooKeepers());
        properties.setUser(USER_NAME);
        properties.setPassword(PASSWORD);
        properties.setOperationDeclarationPaths("sparkAccumuloOperationsDeclarations.json");
        return properties;
    }

    private SparkSession getSparkSession() {
        final SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("testGetAllElementsInRDD-" + System.currentTimeMillis())
                .config(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER)
                .config(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR)
                .config(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, true)
                .getOrCreate();
        return sparkSession;
    }
}
