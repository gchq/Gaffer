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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;


public class FederatedStoreSchemaTest {
    private static final String STRING = "string";
    private static final Schema STRING_SCHEMA = new Schema.Builder()
            .type(STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();
    public User testUser;
    public Context testContext;
    public static final String TEST_FED_STORE = "testFedStore";

    private FederatedStore fStore;
    private static final FederatedStoreProperties FEDERATED_PROPERTIES = new FederatedStoreProperties();

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "properties/accumuloStore.properties"));

    @BeforeEach
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();

        fStore = new FederatedStore();
        fStore.initialise(TEST_FED_STORE, null, FEDERATED_PROPERTIES);

        testUser = testUser();
        testContext = new Context(testUser);
    }

    @Test
    public void shouldBeAbleToAddGraphsWithSchemaCollisions() throws Exception {
        // Given
        addGroupCollisionGraphs();

        final Schema aSchema = new Schema.Builder()
                .edge("e1", getProp("prop1"))
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        fStore.execute(new AddGraph.Builder()
                .graphId("c")
                .schema(aSchema)
                .storeProperties(PROPERTIES)
                .build(), testContext);
        // When
        Collection<String> graphIds = fStore.getAllGraphIds(testUser);

        // Then
        HashSet<String> expected = new HashSet<>();
        expected.addAll(Arrays.asList("a", "b", "c"));

        assertTrue(expected.equals(graphIds));
    }

    @Test
    public void shouldGetCorrectDefaultViewForAChosenGraphOperation() throws
            Exception {
        // Given
        addGroupCollisionGraphs();

        // When
        final CloseableIterable<? extends Element> a = fStore.execute(new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        //No view so makes default view, should get only view compatible with graph "a"
                        .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "a")
                        .build())
                .build(), testContext);

        // Then
        assertNotNull(a);
        assertFalse(a.iterator().hasNext());
    }

    @Test
    public void shouldBeAbleToGetElementsWithOverlappingSchemas() throws
            OperationException {
        // Given
        addOverlappingPropertiesGraphs();

        fStore.execute(new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("e1")
                        .source("source1")
                        .dest("dest1")
                        .property("prop1", "value1")
                        .build())
                .build(), testContext);

        fStore.execute(new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("e1")
                        .source("source1")
                        .dest("dest1")
                        .property("prop1", "value1")
                        .property("prop2", "value2")
                        .build())
                .build(), testContext);

        // When
        final CloseableIterable<? extends Element> a = fStore.execute(new GetElements.Builder()
                .input(new EntitySeed("source1"))
                .view(new View.Builder()
                        .edge("e1", new ViewElementDefinition.Builder()
                                .properties("prop2")
                                .build())
                        .build())
                .build(), testContext);

        assertNotNull(a);
        final ArrayList<Element> results = new ArrayList<>();
        final CloseableIterator<? extends Element> aIterator = a.iterator();
        while (aIterator.hasNext()) {
            Element result = aIterator.next();
            results.add(result);
        }

        // Then
        assertEquals(results.size(), 2);
    }

    @Test
    public void shouldBeAbleToGetSchemaWithOverlappingSchemas() throws
            OperationException {
        // Given
        addOverlappingPropertiesGraphs();

        // When
        final Schema s = fStore.execute(new GetSchema.Builder()
                .build(), testContext);

        // Then
        assertEquals(s.validate(), new ValidationResult());
    }


    private SchemaEdgeDefinition getProp(final String propName) {
        return new SchemaEdgeDefinition.Builder()
                .source(STRING)
                .destination(STRING)
                .directed(DIRECTED_EITHER)
                .property(propName, STRING)
                .build();
    }

    private void addGroupCollisionGraphs() throws OperationException {
        final Schema aSchema = new Schema.Builder()
                .edge("e1", getProp("prop1"))
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        fStore.execute(OperationChain.wrap(
                new AddGraph.Builder()
                        .graphId("a")
                        .schema(aSchema)
                        .storeProperties(PROPERTIES)
                        .build()), testContext);

        final Schema bSchema = new Schema.Builder()
                .edge("e1", getProp("prop2"))
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        fStore.execute(new AddGraph.Builder()
                .graphId("b")
                .schema(bSchema)
                .storeProperties(PROPERTIES)
                .build(), testContext);
    }

    private void addOverlappingPropertiesGraphs() throws OperationException {
        final Schema aSchema = new Schema.Builder()
                .edge("e1", new SchemaEdgeDefinition.Builder()
                        .source(STRING)
                        .destination(STRING)
                        .directed(DIRECTED_EITHER)
                        .property("prop1", STRING)
                        .build())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        final Schema bSchema = new Schema.Builder()
                .edge("e1", new SchemaEdgeDefinition.Builder()
                        .source(STRING)
                        .destination(STRING)
                        .directed(DIRECTED_EITHER)
                        .property("prop1", STRING)
                        .property("prop2", STRING)
                        .build())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        fStore.execute(new AddGraph.Builder()
                .graphId("a")
                .schema(aSchema)
                .storeProperties(PROPERTIES)
                .build(), testContext);

        fStore.execute(new AddGraph.Builder()
                .graphId("b")
                .schema(bSchema)
                .storeProperties(PROPERTIES)
                .build(), testContext);
    }
}
