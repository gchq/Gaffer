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

import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.rdd.RDD;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
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
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.MiniAccumuloClusterProvider;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.SparkSessionProvider;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.function.Concat;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GetRDDOfAllElementsHandlerTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private final User USER = new User();
    private final User USER_WITH_PUBLIC = new User("user1", Sets.newHashSet("public"));
    private final User USER_WITH_PUBLIC_AND_PRIVATE = new User("user2", Sets.newHashSet("public", "private"));
    private final String GRAPH_ID = "graphId";
    private Entity entityRetainedAfterValidation;

    @Test
    public void testGetAllElementsInRDD() throws OperationException, IOException, InterruptedException,
            AccumuloSecurityException, AccumuloException, TableNotFoundException {
        testGetAllElementsInRDD(getGraphForMockAccumulo(), getOperation());
        testGetAllElementsInRDD(getGraphForDirectRDD("testGetAllElementsInRDD"),
                getOperationWithDirectRDDOption());
    }

    @Test
    public void testGetAllElementsInRDDWithView() throws OperationException, IOException, InterruptedException,
            AccumuloSecurityException, AccumuloException, TableNotFoundException {
        testGetAllElementsInRDDWithView(getGraphForMockAccumulo(), getOperation());
        testGetAllElementsInRDDWithView(getGraphForDirectRDD("testGetAllElementsInRDDWithView"),
                getOperationWithDirectRDDOption());
    }

    @Test
    public void testGetAllElementsInRDDWithVisibilityFilteringApplied() throws OperationException, IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        testGetAllElementsInRDDWithVisibilityFilteringApplied(getGraphForMockAccumuloWithVisibility(),
                getOperation());
        testGetAllElementsInRDDWithVisibilityFilteringApplied(
                getGraphForDirectRDDWithVisibility("testGetAllElementsInRDDWithVisibilityFilteringApplied"),
                getOperationWithDirectRDDOption());
    }

    @Test
    public void testGetAllElementsInRDDWithValidationApplied() throws InterruptedException, IOException,
            OperationException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
        testGetAllElementsInRDDWithValidationApplied(getGraphForMockAccumuloForValidationChecking(),
                getOperation());
        testGetAllElementsInRDDWithValidationApplied(
                getGraphForDirectRDDForValidationChecking("testGetAllElementsInRDDWithValidationApplied"),
                getOperationWithDirectRDDOption());
    }

    @Test
    public void testGetAllElementsInRDDWithIngestAggregationApplied() throws OperationException, IOException,
            InterruptedException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
        testGetAllElementsInRDDWithIngestAggregationApplied(getGraphForMockAccumuloForIngestAggregation(),
                getOperation());
        testGetAllElementsInRDDWithIngestAggregationApplied(
                getGraphForDirectRDDForIngestAggregation("testGetAllElementsInRDDWithIngestAggregationApplied"),
                getOperationWithDirectRDDOption());
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
    }

    private void testGetAllElementsInRDDWithVisibilityFilteringApplied(final Graph graph,
                                                                       final GetRDDOfAllElements getRDD)
            throws OperationException, IOException, InterruptedException, AccumuloSecurityException, AccumuloException {
        final Set<Element> expectedElements = new HashSet<>();

        // Test with user with public visibility
        getElementsWithVisibilities()
                .stream()
                .filter(e -> e.getProperty(TestPropertyNames.VISIBILITY).equals("public"))
                .forEach(expectedElements::add);
        RDD<Element> rdd = graph.execute(getRDD, USER_WITH_PUBLIC);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>();
        Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        assertEquals(expectedElements, results);

        // Test with user with public and private visibility
        getElementsWithVisibilities().forEach(expectedElements::add);
        rdd = graph.execute(getRDD, USER_WITH_PUBLIC_AND_PRIVATE);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        assertEquals(expectedElements, results);

        // Test with user with no visibilities
        rdd = graph.execute(getRDD, USER);
        if (rdd == null) {
            fail("No RDD returned");
        }
        results.clear();
        returnedElements = (Element[]) rdd.collect();
        assertEquals(0, returnedElements.length);
    }

    private void testGetAllElementsInRDDWithValidationApplied(final Graph graph, final GetRDDOfAllElements getRDD)
            throws InterruptedException, IOException, OperationException {
        // Sleep for 1 second to give chance for Entity A to age off
        Thread.sleep(1000L);

        final RDD<Element> rdd = graph.execute(getRDD, USER);
        if (rdd == null) {
            fail("No RDD returned");
        }

        // Should get Entity B but not Entity A
        final Element[] returnedElements = (Element[]) rdd.collect();
        assertEquals(1, returnedElements.length);
        assertEquals(entityRetainedAfterValidation, returnedElements[0]);
    }

    private void testGetAllElementsInRDDWithIngestAggregationApplied(final Graph graph, final GetRDDOfAllElements getRDD)
            throws OperationException {
        final RDD<Element> rdd = graph.execute(getRDD, USER);
        if (rdd == null) {
            fail("No RDD returned");
        }

        // Should get aggregated data
        final Element[] returnedElements = (Element[]) rdd.collect();

        assertEquals(1, returnedElements.length);
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("A")
                .property("count", 2)
                .build();
        assertEquals(entity1, returnedElements[0]);
    }

    private Graph _getGraphForMockAccumulo(final Schema schema, final List<Element> elements) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .addSchema(schema)
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        return graph;
    }

    private Graph getGraphForMockAccumulo() throws OperationException {
        return _getGraphForMockAccumulo(getSchema(), getElements());
    }

    private Graph getGraphForMockAccumuloWithVisibility() throws OperationException {
        return _getGraphForMockAccumulo(getSchemaForVisibility(), getElementsWithVisibilities());
    }

    private Graph getGraphForMockAccumuloForValidationChecking() throws OperationException {
        return _getGraphForMockAccumulo(getSchemaForValidationChecking(), getElementsForValidationChecking());
    }

    private Graph getGraphForMockAccumuloForIngestAggregation() throws OperationException {
        final Graph graph = _getGraphForMockAccumulo(getSchemaForIngestAggregationChecking(),
                getElementsForIngestAggregationChecking());
        // Add data twice so that can check data is aggregated
        graph.execute(new AddElements.Builder().input(getElementsForIngestAggregationChecking()).build(), USER);
        return graph;
    }

    private Graph _getGraphForDirectRDD(final String tableName, final Schema schema, final List<Element> elements)
            throws InterruptedException, AccumuloException, AccumuloSecurityException, IOException, OperationException,
            TableNotFoundException {
        final MiniAccumuloCluster cluster = MiniAccumuloClusterProvider.getMiniAccumuloCluster();
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(tableName)
                        .build())
                .addSchema(schema)
                .storeProperties(MiniAccumuloClusterProvider.getAccumuloProperties())
                .build();
        if (null != elements) {
            graph.execute(new AddElements.Builder().input(elements).build(), USER);
            cluster.getConnector(MiniAccumuloClusterProvider.ROOT, MiniAccumuloClusterProvider.PASSWORD)
                    .tableOperations()
                    .compact(tableName, new CompactionConfig());
            Thread.sleep(1000L);
        }
        return graph;
    }

    private Graph getGraphForDirectRDD(final String tableName) throws InterruptedException, AccumuloException, AccumuloSecurityException,
            IOException, OperationException, TableNotFoundException {
        return _getGraphForDirectRDD(tableName, getSchema(), getElements());
    }

    private Graph getGraphForDirectRDDWithVisibility(final String tableName) throws InterruptedException, AccumuloException,
            AccumuloSecurityException, IOException, OperationException, TableNotFoundException {
        return _getGraphForDirectRDD(tableName, getSchemaForVisibility(), getElementsWithVisibilities());
    }

    private Graph getGraphForDirectRDDForValidationChecking(final String tableName) throws InterruptedException,
            AccumuloException, AccumuloSecurityException, IOException, OperationException, TableNotFoundException {
        final MiniAccumuloCluster cluster = MiniAccumuloClusterProvider.getMiniAccumuloCluster();
        final Graph graph = _getGraphForDirectRDD(tableName, getSchemaForValidationChecking(), null);
        graph.execute(new AddElements.Builder().input(getElementsForValidationChecking()).build(), USER);
        cluster.getConnector(MiniAccumuloClusterProvider.ROOT, MiniAccumuloClusterProvider.PASSWORD)
                .tableOperations()
                .compact(tableName, new CompactionConfig());
        Thread.sleep(1000L);
        return graph;
    }

    private Graph getGraphForDirectRDDForIngestAggregation(final String tableName) throws InterruptedException,
            AccumuloException, AccumuloSecurityException, IOException, OperationException, TableNotFoundException {
        final Graph graph = _getGraphForDirectRDD(tableName, getSchemaForIngestAggregationChecking(), null);
        final MiniAccumuloCluster cluster = MiniAccumuloClusterProvider.getMiniAccumuloCluster();
        // Write 2 files and import them to the table - writing 2 files with the same data allows us to test whether
        // data from multiple Rfiles is combined, i.e. whether the ingest aggregation is applied at query time when
        // using the RFileReaderRDD
        for (int i = 0; i < 2; i++) {
            final String dir = tempFolder.newFolder().getAbsolutePath();
            final String file = dir + File.separator + "file" + i+ ".rf";
            final String failure = tempFolder.newFolder().getAbsolutePath();
            writeFile(graph.getSchema(), file);
            cluster.getConnector(MiniAccumuloClusterProvider.USER, MiniAccumuloClusterProvider.PASSWORD)
                    .tableOperations()
                    .importDirectory(tableName, dir, failure, false);
        }
        return graph;
    }

    private void writeFile(final Schema schema, final String file) throws IOException {
        final Configuration conf = new Configuration();
        final CachableBlockFile.Writer blockFileWriter = new CachableBlockFile.Writer(
                FileSystem.get(conf),
                new Path(file),
                Compression.COMPRESSION_NONE,
                conf,
                AccumuloConfiguration.getDefaultConfiguration());
        final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(schema);
        final Entity entity = (Entity) getElementsForIngestAggregationChecking().get(0);
        final Key key = converter.getKeyFromEntity((Entity) getElementsForIngestAggregationChecking().get(0));
        final Value value = converter.getValueFromProperties(entity.getGroup(), entity.getProperties());
        final RFile.Writer writer = new RFile.Writer(blockFileWriter, 1000);
        writer.startDefaultLocalityGroup();
        writer.append(key, value);
        writer.close();
    }

    private Schema getSchema() {
        final Schema schema = new Schema.Builder()
                .json(getClass().getResourceAsStream("/schema/elements.json"),
                        getClass().getResourceAsStream("/schema/types.json"),
                        getClass().getResourceAsStream("/schema/serialisation.json"))
                .build();
        return schema;
    }

    private Schema getSchemaForVisibility() {
        final Schema schema = new Schema.Builder()
                .json(getClass().getResourceAsStream("/schema/elementsWithVisibility.json"),
                        getClass().getResourceAsStream("/schema/types.json"),
                        getClass().getResourceAsStream("/schema/serialisation.json"))
                .build();
        return schema;
    }

    private Schema getSchemaForValidationChecking() {
        final Schema schema = new Schema.Builder()
                .json(getClass().getResourceAsStream("/schema/elementsForValidationChecking.json"),
                        getClass().getResourceAsStream("/schema/typesForValidationChecking.json"),
                        getClass().getResourceAsStream("/schema/serialisation.json"))
                .build();
        return schema;
    }

    private Schema getSchemaForIngestAggregationChecking() {
        final Schema schema = new Schema.Builder()
                .json(getClass().getResourceAsStream("/schema/elementsForAggregationChecking.json"),
                        getClass().getResourceAsStream("/schema/types.json"),
                        getClass().getResourceAsStream("/schema/serialisation.json"))
                .build();
        return schema;
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

    private List<Element> getElementsWithVisibilities() {
        return getElements().stream()
                .map(e -> {
                    if (e.getGroup().equals(TestGroups.ENTITY)) {
                        e.putProperty(TestPropertyNames.VISIBILITY, "public");
                    } else if (((Edge) e).getDestination().equals("B")) {
                        e.putProperty(TestPropertyNames.VISIBILITY, "private");
                    } else {
                        e.putProperty(TestPropertyNames.VISIBILITY, "public");
                    }
                    return e;
                })
                .collect(Collectors.toList());
    }

    private List<Element> getElementsForValidationChecking() {
        final List<Element> elements = new ArrayList<>();
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("A")
                .property("timestamp", System.currentTimeMillis())
                .build();
        final Entity entity2 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("B")
                .property("timestamp", System.currentTimeMillis() + 1000000L)
                .build();
        entityRetainedAfterValidation = entity2;
        elements.add(entity1);
        elements.add(entity2);
        return elements;
    }

    private List<Element> getElementsForIngestAggregationChecking() {
        final List<Element> elements = new ArrayList<>();
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("A")
                .property("count", 1)
                .build();
        elements.add(entity1);
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
                .sparkSession(SparkSessionProvider.getSparkSession())
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        return rddQuery;
    }

    private GetRDDOfAllElements getOperationWithDirectRDDOption() throws IOException {
        final GetRDDOfAllElements op = getOperation();
        op.addOption(AbstractGetRDDHandler.USE_RFILE_READER_RDD, "true");
        return op;
    }
}
