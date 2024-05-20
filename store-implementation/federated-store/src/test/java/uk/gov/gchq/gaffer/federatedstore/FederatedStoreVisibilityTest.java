/*
 * Copyright 2022-2024 Crown Copyright
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.ConcatenateMergeFunction;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.BASIC_VERTEX;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.INTEGER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.store.TestTypes.INTEGER_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.STRING_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.VISIBILITY;
import static uk.gov.gchq.gaffer.store.TestTypes.VISIBILITY_2;

public class FederatedStoreVisibilityTest {
    private static final AccumuloProperties ACCUMULO_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    private static final String PUBLIC = "public";
    private static final String PRIVATE = "private";
    private static final User USER = new User.Builder().dataAuth(PUBLIC).build();
    private Graph federatedGraph;

    @AfterAll
    public static void tearDownCache() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        FederatedStoreProperties federatedStoreProperties = getFederatedStorePropertiesWithHashMapCache();

        federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .addStoreProperties(federatedStoreProperties)
                .build();
    }

    @Test
    public void shouldGetDataWhenVisibilityPropertyNamesAreSame() throws Exception {
        // Given
        addGraphs(VISIBILITY, VISIBILITY);
        addElements(GRAPH_ID_A, VISIBILITY, PUBLIC);
        addElements(GRAPH_ID_B, VISIBILITY, PUBLIC);

        // When
        Iterable<? extends Element> aggregatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .mergeFunction(new ConcatenateMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(2), "property is aggregated");

        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(2)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1");

        assertThat(noAuthResults).isEmpty();
    }

    @Test
    public void shouldGetDataWhenVisibilityPropertyNamesAreDifferent() throws Exception {
        // Given
        addGraphs(VISIBILITY, VISIBILITY_2);
        addElements(GRAPH_ID_A, VISIBILITY, PUBLIC);
        addElements(GRAPH_ID_B, VISIBILITY_2, PUBLIC);

        // When
        Iterable<? extends Element> aggregatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .mergeFunction(new ConcatenateMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(2), "property is aggregated");

        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(2)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1");

        assertThat(noAuthResults).isEmpty();
    }

    @Test
    public void shouldGetDataWhenVisibilityPropertyNamesAndAuthsAreDifferent() throws Exception {
        // Given
        addGraphs(VISIBILITY, VISIBILITY_2);
        addElements(GRAPH_ID_A, VISIBILITY, PUBLIC);
        addElements(GRAPH_ID_B, VISIBILITY_2, PRIVATE);

        // When
        Iterable<? extends Element> aggregatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .mergeFunction(new ConcatenateMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(1), "property is only visible once");

        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(1)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1");

        assertThat(noAuthResults).isEmpty();
    }

    @Test
    public void shouldMergeVisibilityPropertyWhenVisibilityPropertyNamesAreDifferentAndGroupNameIsSame() throws Exception {
        // Given
        addGraphs(VISIBILITY, VISIBILITY_2);
        addElements(GRAPH_ID_A, VISIBILITY, PUBLIC);
        addElements(GRAPH_ID_B, VISIBILITY_2, PUBLIC);

        // When
        Iterable<? extends Element> aggregatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .mergeFunction(new ConcatenateMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(2), "property is aggregated")
                .matches(e -> e.getProperty(VISIBILITY).equals(PUBLIC), "visibility1 is present")
                .matches(e -> e.getProperty(VISIBILITY_2).equals(PUBLIC), "visibility2 is present");
        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(2)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1")
                .anyMatch(e -> PUBLIC.equals(e.getProperty(VISIBILITY)))
                .anyMatch(e -> PUBLIC.equals(e.getProperty(VISIBILITY_2)));
        assertThat(noAuthResults).isEmpty();
    }

    @Test
    public void shouldAddElementToBothGraphsWhenVisibilityPropertyNamesAreDifferent() throws Exception {
        // Given
        addGraphs(VISIBILITY, VISIBILITY_2);
        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group(GROUP_BASIC_ENTITY)
                                .vertex(BASIC_VERTEX)
                                .property(PROPERTY_1, 1)
                                .property(VISIBILITY, PUBLIC)
                                .property(VISIBILITY_2, PUBLIC)
                                .build())
                        .build())
                .build(), USER);

        // When
        Iterable<? extends Element> aggregatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .mergeFunction(new ConcatenateMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = federatedGraph.execute(
                new FederatedOperation.Builder()
                        .<Void, Iterable<? extends Element>>op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(2), "property is aggregated")
                .matches(e -> e.getProperty(VISIBILITY).equals(PUBLIC), "has visibility1 property")
                .matches(e -> e.getProperty(VISIBILITY_2).equals(PUBLIC), "has visibility2 property");
        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(2)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1")
                .anyMatch(e -> PUBLIC.equals(e.getProperty(VISIBILITY)))
                .anyMatch(e -> PUBLIC.equals(e.getProperty(VISIBILITY_2)));
        assertThat(noAuthResults).isEmpty();
    }

    private void addGraphs(final String graphAVisibilityPropertyName, final String graphBVisibilityPropertyName) throws OperationException {
        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_A)
                        .storeProperties(ACCUMULO_PROPERTIES)
                        .schema(getSchema(graphAVisibilityPropertyName))
                        .build(), USER);

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_B)
                        .storeProperties(ACCUMULO_PROPERTIES)
                        .schema(getSchema(graphBVisibilityPropertyName))
                        .build(), USER);
    }

    private void addElements(final String graphId, final String visibilityPropertyName, final String visibilityValue) throws OperationException {
        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group(GROUP_BASIC_ENTITY)
                                .vertex(BASIC_VERTEX)
                                .property(PROPERTY_1, 1)
                                .property(visibilityPropertyName, visibilityValue)
                                .build())
                        .build())
                .graphIdsCSV(graphId)
                .build(), USER);
    }

    private Schema getSchema(final String visibilityPropertyName) {
        return new Schema.Builder()
                .entity(GROUP_BASIC_ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(STRING)
                        .property(PROPERTY_1, INTEGER)
                        .property(visibilityPropertyName, VISIBILITY)
                        .build())
                .type(STRING, STRING_TYPE)
                .type(VISIBILITY, STRING_TYPE)
                .type(INTEGER, INTEGER_TYPE)
                .visibilityProperty(visibilityPropertyName)
                .build();
    }
}
