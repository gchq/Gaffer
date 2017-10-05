/*
/*
 * Copyright 2016 Crown Copyright
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assume.assumeTrue;

/**
 * Logic/config for setting up and running store integration tests.
 * All tests will be skipped if the storeProperties variable has not been set
 * prior to running the tests.
 */
public abstract class AbstractStoreIT {
    protected static final long AGE_OFF_TIME = 10L * 1000; // 4 seconds;

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

    protected static Graph graph;
    private static Schema storeSchema = new Schema();
    private static StoreProperties storeProperties;
    private static String singleTestMethod;

    private final Map<EntityId, Entity> entities = createEntities();
    private final Map<EdgeId, Edge> edges = createEdges();

    @Rule
    public TestName name = new TestName();
    private static Map<? extends Class<? extends AbstractStoreIT>, String> skippedTests;


    public static void setStoreProperties(final StoreProperties storeProperties) {
        AbstractStoreIT.storeProperties = storeProperties;
    }

    public static StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public static Schema getStoreSchema() {
        return storeSchema;
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

    /**
     * Setup the Parameterised Graph for each type of Store.
     * Excludes tests where the graph's Store doesn't implement the required StoreTraits.
     *
     * @throws Exception should never be thrown
     */
    @Before
    public void setup() throws Exception {
        assumeTrue("Skipping test as no store properties have been defined.", null != storeProperties);

        final String originalMethodName = name.getMethodName().endsWith("]")
                ? name.getMethodName().substring(0, name.getMethodName().indexOf("["))
                : name.getMethodName();

        assumeTrue("Skipping test as only " + singleTestMethod + " is being run.", null == singleTestMethod || singleTestMethod.equals(originalMethodName));

        final Method testMethod = this.getClass().getMethod(originalMethodName);
        final Collection<StoreTrait> requiredTraits = new ArrayList<>();

        for (final Annotation annotation : testMethod.getDeclaredAnnotations()) {
            if (annotation.annotationType().equals(TraitRequirement.class)) {
                final TraitRequirement traitRequirement = (TraitRequirement) annotation;
                requiredTraits.addAll(Arrays.asList(traitRequirement.value()));
            }
        }
        assumeTrue("Skipping test. Justification: " + skippedTests.get(getClass()), !skippedTests.containsKey(getClass()));

        graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("integrationTestGraph")
                        .build())
                .storeProperties(storeProperties)
                .addSchema(createSchema())
                .addSchema(storeSchema)
                .build();

        for (final StoreTrait requiredTrait : requiredTraits) {
            assumeTrue("Skipping test as the store does not implement all required traits.", graph.hasTrait(requiredTrait));
        }
    }

    protected Schema createSchema() {
        return createDefaultSchema();
    }

    public static Schema createDefaultSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_EITHER, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
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
                        .property(TestPropertyNames.STRING, TestTypes.PROP_STRING)
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

    @After
    public void tearDown() {
        graph = null;
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

    public Map<EdgeId, Edge> getEdges() {
        return edges;
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

    public static Map<EntityId, Entity> createDefaultEntities() {
        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            for (int j = 0; j < VERTEX_PREFIXES.length; j++) {
                final Entity entity = new Entity(TestGroups.ENTITY, VERTEX_PREFIXES[j] + i);
                entity.putProperty(TestPropertyNames.STRING, "3");
                addToMap(entity, entities);
            }

            final Entity secondEntity = new Entity(TestGroups.ENTITY, SOURCE + i);
            secondEntity.putProperty(TestPropertyNames.STRING, "3");
            addToMap(secondEntity, entities);

            final Entity thirdEntity = new Entity(TestGroups.ENTITY, DEST + i);
            thirdEntity.putProperty(TestPropertyNames.STRING, "3");
            addToMap(thirdEntity, entities);

            final Entity fourthEntity = new Entity(TestGroups.ENTITY, SOURCE_DIR + i);
            fourthEntity.putProperty(TestPropertyNames.STRING, "3");
            addToMap(fourthEntity, entities);

            final Entity fifthEntity = new Entity(TestGroups.ENTITY, DEST_DIR + i);
            fifthEntity.putProperty(TestPropertyNames.STRING, "3");
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

    protected User getUser() {
        return new User();
    }
}
