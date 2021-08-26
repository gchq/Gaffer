/*
 * Copyright 2016-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.integration;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View.Builder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Logic/config for setting up and running store integration tests.
 * All tests will be skipped if the storeProperties variable has not been set
 * prior to running the tests.
 */
public abstract class AbstractStoreIT {
    protected static final int DUPLICATES = 2;

    protected static final long AGE_OFF_TIME = 10L * 1000; // 10 seconds;

    // Identifier prefixes
    public static final String SOURCE = "1-Source";
    public static final String DEST = "2-Dest";
    public static final String SOURCE_DIR = "1-SourceDir";
    public static final String DEST_DIR = "2-DestDir";
    public static final String A = "A";
    public static final String B = "B";
    public static final String C = "C";
    public static final String D = "D";
    public static final String[] VERTEX_PREFIXES = new String[]{A, B, C, D};

    // Identifiers
    public static final String SOURCE_1 = SOURCE + 1;
    public static final String DEST_1 = DEST + 1;

    public static final String SOURCE_2 = SOURCE + 2;
    public static final String DEST_2 = DEST + 2;

    public static final String SOURCE_3 = SOURCE + 3;
    public static final String DEST_3 = DEST + 3;

    public static final String SOURCE_DIR_0 = SOURCE_DIR + 0;
    public static final String DEST_DIR_0 = DEST_DIR + 0;

    public static final String SOURCE_DIR_1 = SOURCE_DIR + 1;
    public static final String DEST_DIR_1 = DEST_DIR + 1;

    public static final String SOURCE_DIR_2 = SOURCE_DIR + 2;
    public static final String DEST_DIR_2 = DEST_DIR + 2;

    public static final String SOURCE_DIR_3 = SOURCE_DIR + 3;
    public static final String DEST_DIR_3 = DEST_DIR + 3;

    protected static Schema storeSchema = new Schema();
    protected static StoreProperties storeProperties;
    protected static String singleTestMethod;
    protected Map<EntityId, Entity> entities;
    protected List<Entity> duplicateEntities;

    protected Map<EdgeId, Edge> edges;
    private List<Edge> duplicateEdges;

    protected final Map<String, User> userMap = new HashMap<>();
    protected static Graph graph;
    protected User user = new User();

    @Rule
    public TestName name = new TestName();
    private static Map<? extends Class<? extends AbstractStoreIT>, String> skippedTests;
    private static Map<? extends Class<? extends AbstractStoreIT>, Map<String, String>> skipTestMethods;
    protected String originalMethodName;
    private Method method;

    public static void setStoreProperties(final StoreProperties storeProperties) {
        AbstractStoreIT.storeProperties = storeProperties;
    }

    public static StoreProperties getStoreProperties() {
        return storeProperties.clone();
    }

    public static Schema getStoreSchema() {
        return storeSchema.clone();
    }

    public static void setStoreSchema(final Schema storeSchema) {
        AbstractStoreIT.storeSchema = storeSchema;
    }

    public static void setSkipTests(final Map<? extends Class<? extends AbstractStoreIT>, String> skippedTests) {
        AbstractStoreIT.skippedTests = skippedTests;
    }

    public static void setSingleTestMethod(final String singleTestMethod) {
        AbstractStoreIT.singleTestMethod = singleTestMethod;
    }

    public static void setSkipTestMethods(final Map<? extends Class<? extends AbstractStoreIT>, Map<String, String>> skipTestMethods) {
        AbstractStoreIT.skipTestMethods = skipTestMethods;
    }

    /**
     * Setup the Parameterised Graph for each type of Store.
     * Excludes tests where the graph's Store doesn't implement the required StoreTraits.
     * Do not override, use the _setup method if more is necessary
     *
     * @throws Exception should never be thrown
     */
    @Before
    public void setup() throws Exception {
        initialise();
        validateTest();
        createGraph();
        _setup();
        validateTraits();
    }

    protected void _setup() throws Exception {
        // Override if required;
    }

    @AfterClass
    public static void tearDown() {
        graph = null;
    }

    protected void initialise() throws Exception {
        entities = createEntities();
        duplicateEntities = duplicate(entities.values());

        edges = createEdges();
        duplicateEdges = duplicate(edges.values());

        originalMethodName = name.getMethodName().endsWith("]")
                ? name.getMethodName().substring(0, name.getMethodName().indexOf("["))
                : name.getMethodName();

        method = this.getClass().getMethod(originalMethodName);
    }

    protected void validateTest() throws Exception {
        assumeThat(null != storeProperties).as("Skipping test as no store properties have been defined.").isTrue();
        assumeThat(null == singleTestMethod || singleTestMethod.equals(originalMethodName)).as("Skipping test as only " + singleTestMethod + " is being run.").isTrue();
        assumeThat(!skippedTests.containsKey(getClass())).as("Skipping test. Justification: " + skippedTests.get(getClass())).isTrue();

        final Map<String, String> skippedMethods = skipTestMethods.get(getClass());
        if (null != skippedMethods && !skippedMethods.isEmpty()) {
            assumeThat(!skippedMethods.containsKey(originalMethodName)).as("Skipping test. Justification: " + skippedMethods.get(method.getName())).isTrue();
        }
    }

    protected void validateTraits() {
        final Collection<StoreTrait> requiredTraits = new ArrayList<>();
        for (final Annotation annotation : method.getDeclaredAnnotations()) {
            if (annotation.annotationType().equals(TraitRequirement.class)) {
                final TraitRequirement traitRequirement = (TraitRequirement) annotation;
                requiredTraits.addAll(Arrays.asList(traitRequirement.value()));
            }
        }

        for (final StoreTrait requiredTrait : requiredTraits) {
            assumeThat(graph.hasTrait(requiredTrait)).as("Skipping test as the store does not implement all required traits.").isTrue();
        }
    }

    protected void applyVisibilityUser() {
        if (!userMap.isEmpty()) {
            if (null != method.getDeclaredAnnotations()) {
                for (final Annotation annotation : method.getDeclaredAnnotations()) {
                    if (annotation.annotationType().equals(VisibilityUser.class)) {
                        final VisibilityUser userAnnotation = (VisibilityUser) annotation;

                        final User user = userMap.get(userAnnotation.value());

                        if (null != user) {
                            this.user = user;
                        }
                    }
                }
            }
        }
    }

    protected void createGraph() {
        graph = getGraphBuilder()
                .build();

        applyVisibilityUser();
    }

    public void createGraph(final Schema schema) {
        graph = new Graph.Builder()
                .config(createGraphConfig())
                .storeProperties(getStoreProperties())
                .addSchema(schema)
                .build();

        applyVisibilityUser();
    }

    public void createGraph(final GraphConfig config) {
        graph = new Graph.Builder()
                .config(config)
                .storeProperties(getStoreProperties())
                .addSchema(createSchema())
                .addSchema(getStoreSchema())
                .build();

        applyVisibilityUser();
    }

    public void createGraph(final Schema schema, final StoreProperties properties) {
        graph = new Graph.Builder()
                .config(createGraphConfig())
                .storeProperties(properties)
                .addSchema(schema)
                .build();

        applyVisibilityUser();
    }

    public void createGraph(final StoreProperties properties) {
        graph = new Graph.Builder()
                .config(createGraphConfig())
                .storeProperties(properties)
                .addSchema(createSchema())
                .addSchema(getStoreSchema())
                .build();

        applyVisibilityUser();
    }

    protected Graph.Builder getGraphBuilder() {
        return new Graph.Builder()
                .config(createGraphConfig())
                .storeProperties(getStoreProperties())
                .addSchema(createSchema())
                .addSchema(getStoreSchema());
    }

    protected void addStoreProperties(final StoreProperties storeProperties) {
        graph = getGraphBuilder().addStoreProperties(storeProperties).build();
    }

    protected void addGraphConfig(final GraphConfig graphConfig) {
        graph = getGraphBuilder().config(graphConfig).build();
    }

    protected Schema createSchema() {
        return createDefaultSchema();
    }

    protected GraphConfig createGraphConfig() {
        return createDefaultGraphConfig();
    }

    public static GraphConfig createDefaultGraphConfig() {
        return new GraphConfig.Builder()
                .graphId("integrationTestGraph")
                .build();
    }

    public static Schema createDefaultSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_EITHER, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .type(TestTypes.PROP_SET_STRING, new TypeDefinition.Builder()
                        .clazz(TreeSet.class)
                        .aggregateFunction(new CollectionConcat<>())
                        .build())
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Sum())
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.TIMESTAMP_2, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .validateFunctions(new AgeOff(AGE_OFF_TIME))
                        .build())
                .type(TestTypes.PROP_INTEGER_2, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .validateFunctions(new IsLessThan(10))
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.SET, TestTypes.PROP_SET_STRING)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP_2)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER_2)
                        .build())
                .build();
    }

    public void addDefaultElements() throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(getEntities().values())
                .build(), getUser());

        graph.execute(new AddElements.Builder()
                .input(getEdges().values())
                .build(), getUser());
    }

    public Map<EntityId, Entity> getEntities() {
        return entities;
    }

    public List<Entity> getIngestSummarisedEntities() {
        final Schema schema = null != graph ? graph.getSchema() : getStoreSchema();
        return (List) Lists.newArrayList((Iterable) AggregatorUtil.ingestAggregate(jsonClone(duplicateEntities), schema));
    }

    public List<Entity> getQuerySummarisedEntities() {
        final Schema schema = null != graph ? graph.getSchema() : getStoreSchema();
        final View view = new Builder()
                .entities(schema.getEntityGroups())
                .edges(schema.getEdgeGroups())
                .build();
        return getQuerySummarisedEntities(view);
    }

    public List<Entity> getQuerySummarisedEntities(final View view) {
        final Schema schema = null != graph ? graph.getSchema() : getStoreSchema();
        final List<Entity> ingestSummarisedEntities = getIngestSummarisedEntities();
        return (List) Lists.newArrayList((Iterable) AggregatorUtil.queryAggregate(ingestSummarisedEntities, schema, view));
    }

    public Map<EdgeId, Edge> getEdges() {
        return edges;
    }

    public List<Edge> getIngestSummarisedEdges() {
        final Schema schema = null != graph ? graph.getSchema() : getStoreSchema();
        return (List) Lists.newArrayList((Iterable) AggregatorUtil.ingestAggregate(jsonClone(duplicateEdges), schema));
    }

    public List<Edge> getQuerySummarisedEdges() {
        final Schema schema = null != graph ? graph.getSchema() : getStoreSchema();
        final View view = new Builder()
                .entities(schema.getEntityGroups())
                .edges(schema.getEdgeGroups())
                .build();
        return getQuerySummarisedEdges(view);
    }

    public List<Edge> getQuerySummarisedEdges(final View view) {
        final Schema schema = null != graph ? graph.getSchema() : getStoreSchema();
        final List<Edge> ingestSummarisedEdges = getIngestSummarisedEdges();
        return (List) Lists.newArrayList((Iterable) AggregatorUtil.queryAggregate(ingestSummarisedEdges, schema, view));
    }

    public Entity getEntity(final Object vertex) {
        return entities.get(new EntitySeed(vertex));
    }

    public Edge getEdge(final Object source, final Object dest, final boolean isDirected) {
        return edges.get(new EdgeSeed(source, dest, isDirected));
    }

    protected Map<EdgeId, Edge> createEdges() {
        return createDefaultEdges();
    }

    protected Iterable<Edge> getDuplicateEdges() {
        return jsonClone(duplicateEdges);
    }

    public static Map<EdgeId, Edge> createDefaultEdges() {
        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Edge edge = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(false)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edge, edges);

                final Edge edgeDir = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_PREFIXES[0] + i)
                        .dest(VERTEX_PREFIXES[j] + i)
                        .directed(true)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build();
                addToMap(edgeDir, edges);
            }

            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE + i)
                    .dest(DEST + i)
                    .directed(false)
                    .property(TestPropertyNames.INT, 1)
                    .property(TestPropertyNames.COUNT, 1L)
                    .build();
            addToMap(edge, edges);

            final Edge edgeDir = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(SOURCE_DIR + i)
                    .dest(DEST_DIR + i)
                    .directed(true)
                    .build();
            edgeDir.putProperty(TestPropertyNames.INT, 1);
            edgeDir.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(edgeDir, edges);
        }

        return edges;
    }

    protected Map<EntityId, Entity> createEntities() {
        return createDefaultEntities();
    }

    protected Iterable<Entity> getDuplicateEntities() {
        return jsonClone(duplicateEntities);
    }

    private <T> List<T> duplicate(final Iterable<T> items) {
        final List<T> duplicates = new ArrayList<>();
        for (int i = 0; i < DUPLICATES; i++) {
            Iterables.addAll(duplicates, items);
        }
        return duplicates;
    }

    public static Map<EntityId, Entity> createDefaultEntities() {
        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Entity entity = new Entity(TestGroups.ENTITY, VERTEX_PREFIXES[j] + i);
                entity.putProperty(TestPropertyNames.COUNT, 1L);
                entity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
                addToMap(entity, entities);
            }

            final Entity secondEntity = new Entity(TestGroups.ENTITY, SOURCE + i);
            secondEntity.putProperty(TestPropertyNames.COUNT, 1L);
            secondEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
            addToMap(secondEntity, entities);

            final Entity thirdEntity = new Entity(TestGroups.ENTITY, DEST + i);
            thirdEntity.putProperty(TestPropertyNames.COUNT, 1L);
            thirdEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
            addToMap(thirdEntity, entities);

            final Entity fourthEntity = new Entity(TestGroups.ENTITY, SOURCE_DIR + i);
            fourthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            fourthEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
            addToMap(fourthEntity, entities);

            final Entity fifthEntity = new Entity(TestGroups.ENTITY, DEST_DIR + i);
            fifthEntity.putProperty(TestPropertyNames.COUNT, 1L);
            fifthEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
            addToMap(fifthEntity, entities);
        }

        return entities;
    }

    protected static void addToMap(final Edge element, final Map<EdgeId, Edge> edges) {
        edges.put(ElementSeed.createSeed(element), element);
    }

    protected static void addToMap(final Entity element, final Map<EntityId, Entity> entities) {
        entities.put(ElementSeed.createSeed(element), element);
    }

    protected <T> List<T> jsonClone(final Iterable<T> items) {
        return Streams.toStream(items).map(this::jsonClone).collect(Collectors.toList());
    }

    protected <T> T jsonClone(final T item) {
        try {
            return (T) JSONSerialiser.deserialise(JSONSerialiser.serialise(item), item.getClass());
        } catch (final SerialisationException e) {
            throw new RuntimeException("Unable to clone item: " + item);
        }
    }

    public User getUser() {
        return user;
    }

    public void execute(final Operation op) throws OperationException {
        graph.execute(op, user);
    }

    public <T> T execute(final Output<T> op) throws OperationException {
        return graph.execute(op, user);
    }
}
