/*
 * Copyright 2017-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.integration.operation.handler.scalardd;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.rdd.RDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public final class GetRDDOfAllElementsHandlerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetRDDOfAllElementsHandlerIT.class);
    private static final User USER = new User();
    private static final User USER_WITH_PUBLIC = new User("user1", Sets.newHashSet("public"));
    private static final User USER_WITH_PUBLIC_AND_PRIVATE = new User("user2", Sets.newHashSet("public", "private"));
    private static final String GRAPH_ID = "graphId";

    private static final AccumuloProperties PROPERTIES_A = AccumuloProperties.loadStoreProperties(GetRDDOfAllElementsHandlerIT.class.getResourceAsStream("/store.properties"));
    private static final AccumuloProperties PROPERTIES_B = AccumuloProperties.loadStoreProperties(GetRDDOfAllElementsHandlerIT.class.getResourceAsStream("/store.properties"));

    private Entity entityRetainedAfterValidation;

    private GetRDDOfAllElementsHandlerIT() {

    }

    @TempDir
    public static File tempFolder;

    private enum KeyPackage {
        BYTE_ENTITY, CLASSIC
    }

    @Test
    public void checkHadoopConfIsPassedThrough() throws OperationException, IOException {
        final Graph graph1 = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream("/schema/elements.json"))
                .addSchema(getClass().getResourceAsStream("/schema/types.json"))
                .addSchema(getClass().getResourceAsStream("/schema/serialisation.json"))
                .storeProperties(PROPERTIES_A)
                .build();
        final Configuration conf = new Configuration();
        conf.set("AN_OPTION", "A_VALUE");
        final String encodedConf = AbstractGetRDDHandler.convertConfigurationToString(conf);
        final GetRDDOfAllElements rddQuery = new GetRDDOfAllElements.Builder()
                .option(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, encodedConf)
                .build();

        final RDD<Element> rdd = graph1.execute(rddQuery, new User());

        assertEquals(encodedConf, rddQuery.getOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY));
        assertEquals("A_VALUE", rdd.sparkContext().hadoopConfiguration().get("AN_OPTION"));
    }

    @ParameterizedTest
    @EnumSource
    public void testGetAllElementsInRDD(KeyPackage keyPackage) throws OperationException, IOException, InterruptedException,
            AccumuloSecurityException, AccumuloException, StoreException, TableNotFoundException {
        testGetAllElementsInRDD(getGraphForMockAccumulo(keyPackage), getOperation());
        testGetAllElementsInRDD(getGraphForMockAccumulo(keyPackage), getOperationWithBatchScannerEnabled());
        testGetAllElementsInRDD(
                getGraphForDirectRDD(keyPackage, "testGetAllElementsInRDD_" + keyPackage.name()),
                getOperationWithDirectRDDOption());
    }

    @ParameterizedTest
    @EnumSource
    public void testGetAllElementsInRDDWithView(KeyPackage keyPackage) throws OperationException, IOException, InterruptedException,
            AccumuloSecurityException, AccumuloException, StoreException, TableNotFoundException {
        testGetAllElementsInRDDWithView(getGraphForMockAccumulo(keyPackage), getOperation());
        testGetAllElementsInRDDWithView(getGraphForMockAccumulo(keyPackage), getOperationWithBatchScannerEnabled());
        testGetAllElementsInRDDWithView(
                getGraphForDirectRDD(keyPackage, "testGetAllElementsInRDDWithView_" + keyPackage.name()),
                getOperationWithDirectRDDOption());
    }

    @ParameterizedTest
    @EnumSource
    public void testGetAllElementsInRDDWithVisibilityFilteringApplied(KeyPackage keyPackage) throws OperationException, IOException,
            InterruptedException, AccumuloSecurityException, StoreException, AccumuloException, TableNotFoundException {
        testGetAllElementsInRDDWithVisibilityFilteringApplied(
                getGraphForMockAccumuloWithVisibility(keyPackage),
                getOperation());
        testGetAllElementsInRDDWithVisibilityFilteringApplied(
                getGraphForMockAccumuloWithVisibility(keyPackage),
                getOperationWithBatchScannerEnabled());
        testGetAllElementsInRDDWithVisibilityFilteringApplied(
                getGraphForDirectRDDWithVisibility(keyPackage, "testGetAllElementsInRDDWithVisibilityFilteringApplied_" + keyPackage.name()),
                getOperationWithDirectRDDOption());
    }

    @ParameterizedTest
    @EnumSource
    public void testGetAllElementsInRDDWithValidationApplied(KeyPackage keyPackage) throws InterruptedException, IOException,
            OperationException, AccumuloSecurityException, StoreException, TableNotFoundException, AccumuloException {
        testGetAllElementsInRDDWithValidationApplied(
                getGraphForAccumuloForValidationChecking(keyPackage),
                getOperation());
        testGetAllElementsInRDDWithValidationApplied(
                getGraphForAccumuloForValidationChecking(keyPackage),
                getOperationWithBatchScannerEnabled());
        testGetAllElementsInRDDWithValidationApplied(
                getGraphForDirectRDDForValidationChecking(keyPackage, "testGetAllElementsInRDDWithValidationApplied_" + keyPackage.name()),
                getOperationWithDirectRDDOption());
    }

    @ParameterizedTest
    @EnumSource
    public void testGetAllElementsInRDDWithIngestAggregationApplied(KeyPackage keyPackage) throws OperationException, IOException,
            InterruptedException, AccumuloSecurityException, StoreException, TableNotFoundException, AccumuloException {
        testGetAllElementsInRDDWithIngestAggregationApplied(
                getGraphForMockAccumuloForIngestAggregation(keyPackage),
                getOperation());
        testGetAllElementsInRDDWithIngestAggregationApplied(
                getGraphForMockAccumuloForIngestAggregation(keyPackage),
                getOperationWithBatchScannerEnabled());
        testGetAllElementsInRDDWithIngestAggregationApplied(
                getGraphForDirectRDDForIngestAggregation(keyPackage, "testGetAllElementsInRDDWithIngestAggregationApplied_" + keyPackage.name()),
                getOperationWithDirectRDDOption());
    }


    private void testGetAllElementsInRDD(final Graph graph, final GetRDDOfAllElements getRDD) throws OperationException {
        final Set<Element> expectedElements = new HashSet<>(getElements());
        final RDD<Element> rdd = graph.execute(getRDD, USER);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Element[] returnedElements = (Element[]) rdd.collect();
        // Check the number of elements returned is correct to ensure edges
        // aren't returned twice
        assertThat(returnedElements).hasSize(30);
        final Set<Element> results = new HashSet<>(Arrays.asList(returnedElements));
        assertEquals(expectedElements, results);
    }

    private void testGetAllElementsInRDDWithView(final Graph graph, final GetRDDOfAllElements getRDD) throws OperationException {
        final Set<Element> expectedElements = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(TestGroups.EDGE))
                .map(e -> (Edge) e)
                .map(e -> {
                    e.putProperty("newProperty", e.getSource().toString() + "," + e.getProperty(TestPropertyNames.COUNT));
                    return e;
                })
                .filter(e -> e.getProperty("newProperty").equals("0,2"))
                .forEach(expectedElements::add);
        getRDD.setView(new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transientProperty("newProperty", String.class)
                        .transformer(new ElementTransformer.Builder()
                                .select(IdentifierType.SOURCE.name(), TestPropertyNames.COUNT)
                                .execute(new Concat())
                                .project("newProperty")
                                .build())
                        .postTransformFilter(new ElementFilter.Builder()
                                .select("newProperty")
                                .execute(new IsEqual("0,2"))
                                .build())
                        .build())
                .build());
        final RDD<Element> rdd = graph.execute(getRDD, USER);
        if (rdd == null) {
            fail("No RDD returned");
        }

        final Element[] returnedElements = (Element[]) rdd.collect();
        final Set<Element> results = new HashSet<>(Arrays.asList(returnedElements));
        assertEquals(expectedElements, results);
    }

    private void testGetAllElementsInRDDWithVisibilityFilteringApplied(final Graph graph, final GetRDDOfAllElements getRDD)
            throws OperationException {
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
        assertThat(returnedElements).isEmpty();
    }

    private void testGetAllElementsInRDDWithValidationApplied(final Graph graph, final GetRDDOfAllElements getRDD)
            throws InterruptedException, OperationException {
        // Sleep for 1 second to give chance for Entity A to age off
        Thread.sleep(1000L);

        final RDD<Element> rdd = graph.execute(getRDD, USER);
        if (rdd == null) {
            fail("No RDD returned");
        }

        // Should get Entity B but not Entity A
        final Element[] returnedElements = (Element[]) rdd.collect();
        assertThat(returnedElements).hasSize(1);
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

        assertThat(returnedElements).hasSize(1);
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("A")
                .property("count", 2)
                .build();
        assertEquals(entity1, returnedElements[0]);
    }

    private StoreProperties getAccumuloProperties(final KeyPackage keyPackage) {
        final AccumuloProperties storeProperties = PROPERTIES_A.clone();
        switch (keyPackage) {
            case BYTE_ENTITY:
                storeProperties.setKeyPackageClass(ByteEntityKeyPackage.class.getName());
                break;
            case CLASSIC:
                storeProperties.setKeyPackageClass(ClassicKeyPackage.class.getName());
        }
        return storeProperties;
    }

    private Graph _getGraphForAccumulo(final Schema schema,
                                       final List<Element> elements,
                                       final KeyPackage keyPackage) throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .build())
                .addSchema(schema)
                .storeProperties(getAccumuloProperties(keyPackage))
                .build();
        graph.execute(new AddElements.Builder()
                .input(elements)
                .validate(false)
                .build(), USER);
        return graph;
    }

    private Graph getGraphForMockAccumulo(KeyPackage keyPackage) throws OperationException {
        return _getGraphForAccumulo(getSchema(), getElements(), keyPackage);
    }

    private Graph getGraphForMockAccumuloWithVisibility(KeyPackage keyPackage) throws OperationException {
        return _getGraphForAccumulo(getSchemaForVisibility(), getElementsWithVisibilities(), keyPackage);
    }

    private Graph getGraphForAccumuloForValidationChecking(KeyPackage keyPackage) throws OperationException {
        return _getGraphForAccumulo(getSchemaForValidationChecking(),
                getElementsForValidationChecking(), keyPackage);
    }

    private Graph getGraphForMockAccumuloForIngestAggregation(KeyPackage keyPackage) throws OperationException {
        final Graph graph = _getGraphForAccumulo(
                getSchemaForIngestAggregationChecking(),
                getElementsForIngestAggregationChecking(), keyPackage);
        // Add data twice so that can check data is aggregated
        graph.execute(new AddElements.Builder()
                .input(getElementsForIngestAggregationChecking())
                .validate(false)
                .build(), USER);
        return graph;
    }

    private Graph _getGraphForDirectRDD(final KeyPackage keyPackage,
                                        final String tableName,
                                        final Schema schema,
                                        final List<Element> elements)
            throws InterruptedException, AccumuloException, AccumuloSecurityException,
                   IOException, OperationException, TableNotFoundException, StoreException {
        AccumuloStore accumuloStore = new AccumuloStore();
        accumuloStore.initialise(tableName, schema, PROPERTIES_B);
        updateAccumuloPropertiesWithKeyPackage(keyPackage);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(tableName)
                        .build())
                .addSchema(schema)
                .storeProperties(PROPERTIES_B)
                .build();
        if (null != elements) {
            graph.execute(new AddElements.Builder()
                    .input(elements)
                    .validate(false)
                    .build(), USER);
            accumuloStore.getConnection()
                    .tableOperations()
                    .compact(tableName, new CompactionConfig());
            Thread.sleep(1000L);
        }
        return graph;
    }

    private void updateAccumuloPropertiesWithKeyPackage(final KeyPackage keyPackage)
            throws InterruptedException, AccumuloSecurityException, AccumuloException, IOException {
        switch (keyPackage) {
            case BYTE_ENTITY:
                PROPERTIES_B.setKeyPackageClass(ByteEntityKeyPackage.class.getName());
                break;
            case CLASSIC:
                PROPERTIES_B.setKeyPackageClass(ClassicKeyPackage.class.getName());
        }
    }

    private Graph getGraphForDirectRDD(final KeyPackage keyPackage,
                                       final String tableName)
            throws InterruptedException, AccumuloException, AccumuloSecurityException,
            IOException, OperationException, StoreException, TableNotFoundException {
        return _getGraphForDirectRDD(keyPackage, tableName, getSchema(), getElements());
    }

    private Graph getGraphForDirectRDDWithVisibility(final KeyPackage keyPackage,
                                                     final String tableName)
            throws InterruptedException, AccumuloException, AccumuloSecurityException,
            IOException, OperationException, StoreException, TableNotFoundException {
        return _getGraphForDirectRDD(keyPackage, tableName, getSchemaForVisibility(),
                getElementsWithVisibilities());
    }

    private Graph getGraphForDirectRDDForValidationChecking(final KeyPackage keyPackage,
                                                            final String tableName)
            throws InterruptedException, AccumuloException, AccumuloSecurityException,
            IOException, OperationException, StoreException, TableNotFoundException {
        Schema schema = getSchemaForValidationChecking();
        final Graph graph = _getGraphForDirectRDD(keyPackage, tableName, schema, null);
        graph.execute(new AddElements.Builder()
                .input(getElementsForValidationChecking())
                .validate(false)
                .build(), USER);
        AccumuloStore accumuloStore = new AccumuloStore();
        accumuloStore.initialise(tableName, schema, PROPERTIES_B);
        accumuloStore.getConnection()
                .tableOperations()
                .compact(tableName, new CompactionConfig());
        Thread.sleep(1000L);
        return graph;
    }

    private Graph getGraphForDirectRDDForIngestAggregation(final KeyPackage keyPackage,
                                                           final String tableName)
            throws InterruptedException, AccumuloException, AccumuloSecurityException,
            IOException, OperationException, StoreException, TableNotFoundException {
        Schema schema = getSchemaForIngestAggregationChecking();
        final Graph graph = _getGraphForDirectRDD(keyPackage, tableName, schema, null);
        AccumuloStore accumuloStore = new AccumuloStore();
        accumuloStore.initialise(tableName, schema, PROPERTIES_B);

        // Write 2 files and import them to the table - writing 2 files with the same data allows us to test whether
        // data from multiple Rfiles is combined, i.e. whether the ingest aggregation is applied at query time when
        // using the RFileReaderRDD
        for (int i = 0; i < 2; i++) {
            final String dir = tempFolder.getAbsolutePath() + File.separator + "files";
            File filesDir = new File(dir);
            final String file = dir + File.separator + "file" + i + ".rf";
            final String failure = tempFolder.getAbsolutePath() + File.separator + "failures";
            File failuresDir = new File(failure);
            try {
                filesDir.mkdir();
                failuresDir.mkdir();
            } catch (Exception e) {
                LOGGER.error("Failed to create directory: " + e.getMessage());
            }

            writeFile(keyPackage, graph.getSchema(), file);
            accumuloStore.getConnection()
                    .tableOperations()
                    .importDirectory(tableName, dir, failure, false);
        }
        return graph;
    }

    private void writeFile(final KeyPackage keyPackage, final Schema schema, final String file)
            throws IllegalArgumentException, IOException {
        final Configuration conf = new Configuration();
        final CachableBlockFile.Writer blockFileWriter = new CachableBlockFile.Writer(
                FileSystem.get(conf),
                new Path(file),
                Compression.COMPRESSION_NONE,
                null,
                conf,
                AccumuloConfiguration.getDefaultConfiguration());
        final AccumuloElementConverter converter;
        switch (keyPackage) {
            case BYTE_ENTITY:
                converter = new ByteEntityAccumuloElementConverter(schema);
                break;
            case CLASSIC:
                converter = new ClassicAccumuloElementConverter(schema);
                break;
            default:
                throw new IllegalArgumentException("Unknown keypackage");
        }
        final Entity entity = (Entity) getElementsForIngestAggregationChecking().get(0);
        final Key key = converter.getKeyFromEntity((Entity) getElementsForIngestAggregationChecking().get(0));
        final Value value = converter.getValueFromProperties(entity.getGroup(), entity.getProperties());
        final RFile.Writer writer = new RFile.Writer(blockFileWriter, 1000);
        writer.startDefaultLocalityGroup();
        writer.append(key, value);
        writer.close();
    }

    private Schema getSchema() {
        return new Schema.Builder()
                .json(getClass().getResourceAsStream("/schema/elements.json"),
                        getClass().getResourceAsStream("/schema/types.json"),
                        getClass().getResourceAsStream("/schema/serialisation.json"))
                .build();
    }

    private Schema getSchemaForVisibility() {
        return new Schema.Builder()
                .json(getClass().getResourceAsStream("/schema/elementsWithVisibility.json"),
                        getClass().getResourceAsStream("/schema/types.json"),
                        getClass().getResourceAsStream("/schema/serialisation.json"))
                .build();
    }

    private Schema getSchemaForValidationChecking() {
        return new Schema.Builder()
                .json(getClass().getResourceAsStream("/schema/elementsForValidationChecking.json"),
                        getClass().getResourceAsStream("/schema/typesForValidationChecking.json"),
                        getClass().getResourceAsStream("/schema/serialisation.json"))
                .build();
    }

    private Schema getSchemaForIngestAggregationChecking() {
        return new Schema.Builder()
                .json(getClass().getResourceAsStream("/schema/elementsForAggregationChecking.json"),
                        getClass().getResourceAsStream("/schema/types.json"),
                        getClass().getResourceAsStream("/schema/serialisation.json"))
                .build();
    }

    private List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
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
        final String configurationString = AbstractGetRDDHandler
                .convertConfigurationToString(configuration);

        // Check get correct elements
        final GetRDDOfAllElements rddQuery = new GetRDDOfAllElements.Builder()
                .build();
        rddQuery.addOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        return rddQuery;
    }

    private GetRDDOfAllElements getOperationWithDirectRDDOption() throws IOException {
        final GetRDDOfAllElements op = getOperation();
        op.addOption(AbstractGetRDDHandler.USE_RFILE_READER_RDD, "true");
        return op;
    }

    private GetRDDOfAllElements getOperationWithBatchScannerEnabled() throws IOException {
        final GetRDDOfAllElements op = getOperation();
        op.addOption(AbstractGetRDDHandler.USE_BATCH_SCANNER_RDD, "true");
        return op;
    }

}
