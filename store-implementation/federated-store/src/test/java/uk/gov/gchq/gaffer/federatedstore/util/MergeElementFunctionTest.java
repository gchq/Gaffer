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

package uk.gov.gchq.gaffer.federatedstore.util;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.retriever.impl.AccumuloAllElementsRetriever;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.edgeBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.SCHEMA;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.USER;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.VIEW;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

class MergeElementFunctionTest {

    public static final Schema EDGE_SCHEMA = loadSchemaFromJson(SCHEMA_EDGE_BASIC_JSON);
    public static final AccumuloProperties ACCUMULO_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);

    @Test
    public void shouldGetFunctioningIteratorOfAccumuloElementRetriever() throws Exception {
        //given
        final AccumuloStore accumuloStore = getTestStore("shouldGetFunctioningIteratorOfAccumuloElementRetriever");
        addEdgeBasic(accumuloStore);

        //when
        final AccumuloAllElementsRetriever elements = new AccumuloAllElementsRetriever(accumuloStore, new GetAllElements.Builder().view(getViewForEdgeBasic()).build(), blankUser());

        //then
        assertThat(elements)
                .isExactlyInstanceOf(AccumuloAllElementsRetriever.class)
                .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
                .containsExactly(edgeBasic());
    }

    private static View getViewForEdgeBasic() {
        return new View.Builder().edge(edgeBasic().getGroup()).build();
    }

    private static void addEdgeBasic(final AccumuloStore accumuloStore) throws OperationException {
        accumuloStore.execute(new AddElements.Builder().input(edgeBasic()).build(), contextBlankUser());
    }

    @Test
    public void shouldAggregateEdgesFromMultipleRetrievers() throws Exception {
        //given
        final AccumuloStore accumuloStore = getTestStore("shouldAggregateEdgesFromMultipleRetrievers");
        addEdgeBasic(accumuloStore);
        AccumuloAllElementsRetriever[] retrievers = getRetrievers(accumuloStore);
        final MergeElementFunction function = new MergeElementFunction().createFunctionWithContext(
                makeContext(
                        new View.Builder().edge(GROUP_BASIC_EDGE).build(),
                        EDGE_SCHEMA.clone()));

        //when
        Iterable<Object> iterable = null;
        for (AccumuloAllElementsRetriever elements : retrievers) {
            iterable = function.apply(elements, iterable);
        }

        //then
        final Edge edge5 = edgeBasic();
        //With aggregated property value of 5
        edge5.putProperty(PROPERTY_1, 5);

        assertThat(iterable)
                .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
                .containsExactly(edge5);
    }

    private static FederatedStore getFederatedStore() throws StoreException {
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, new Schema(), new FederatedStoreProperties());
        return federatedStore;
    }

    @Test
    public void shouldApplyViewToAggregatedEdgesFromMultipleRetrievers() throws Exception {
        //given
        final AccumuloStore accumuloStore = getTestStore("shouldApplyViewToAggregatedEdgesFromMultipleRetrievers");
        addEdgeBasic(accumuloStore);
        AccumuloAllElementsRetriever[] retrievers = getRetrievers(accumuloStore);
        final MergeElementFunction function = new MergeElementFunction().createFunctionWithContext(
                makeContext(
                        //Update View to filter OUT greater than 2.
                        new View.Builder().edge(GROUP_BASIC_EDGE,
                                new ViewElementDefinition.Builder()
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select(PROPERTY_1)
                                                .execute(new IsLessThan(3))
                                                .build())
                                        .build()).build(),
                        EDGE_SCHEMA.clone()));

        //when
        Iterable<Object> iterable = null;
        for (AccumuloAllElementsRetriever elements : retrievers) {
            iterable = function.apply(elements, iterable);
        }

        //then
        assertThat(iterable)
                .asInstanceOf(InstanceOfAssertFactories.iterable(Element.class))
                .isEmpty();
    }

    private static AccumuloAllElementsRetriever[] getRetrievers(final AccumuloStore accumuloStore) throws IteratorSettingException, StoreException {
        return new AccumuloAllElementsRetriever[]{
                new AccumuloAllElementsRetriever(accumuloStore, new GetAllElements.Builder().view(getViewForEdgeBasic()).build(), blankUser()),
                new AccumuloAllElementsRetriever(accumuloStore, new GetAllElements.Builder().view(getViewForEdgeBasic()).build(), blankUser()),
                new AccumuloAllElementsRetriever(accumuloStore, new GetAllElements.Builder().view(getViewForEdgeBasic()).build(), blankUser()),
                new AccumuloAllElementsRetriever(accumuloStore, new GetAllElements.Builder().view(getViewForEdgeBasic()).build(), blankUser()),
                new AccumuloAllElementsRetriever(accumuloStore, new GetAllElements.Builder().view(getViewForEdgeBasic()).build(), blankUser())
        };
    }

    private static AccumuloStore getTestStore(final String instanceName) throws StoreException {
        final AccumuloStore accumuloStore = new MiniAccumuloStore();
        final AccumuloProperties clone = ACCUMULO_PROPERTIES.clone();
        //This line allows different MiniAccumuloStore
        //tableName = NameSpace.GraphId.
        clone.setNamespace(instanceName);
        accumuloStore.initialise(GRAPH_ID_ACCUMULO, EDGE_SCHEMA.clone(), clone);
        return accumuloStore;
    }

    private static HashMap<String, Object> makeContext(final View view, final Schema schema) {
        final HashMap<String, Object> map = new HashMap<>();
        map.put(VIEW, view);
        map.put(SCHEMA, schema);
        map.put(USER, testUser());
        return map;
    }

    @Test
    void shouldRequireUserInFunctionContext() {
        final HashMap<String, Object> context = getTestFunctionContext();
        context.remove(USER);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MergeElementFunction.validate(context))
                .withMessageContaining("context invalid, requires a User");
    }

    @Test
    void shouldRequireSchemaInFunctionContext() {
        final HashMap<String, Object> context = getTestFunctionContext();
        context.remove(SCHEMA);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MergeElementFunction.validate(context))
                .withMessageContaining("context invalid, requires a populated schema");
    }

    @Test
    void shouldRequireSchemaWithGroupsInFunctionContext() {
        final HashMap<String, Object> context = getTestFunctionContext();
        context.put(SCHEMA, new Schema());
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MergeElementFunction.validate(context))
                .withMessageContaining("context invalid, requires a populated schema");
    }

    private static HashMap<String, Object> getTestFunctionContext() {
        final HashMap<String, Object> context = new HashMap<>();
        context.put(USER, testUser());
        context.put(SCHEMA, EDGE_SCHEMA);
        return context;
    }


}
