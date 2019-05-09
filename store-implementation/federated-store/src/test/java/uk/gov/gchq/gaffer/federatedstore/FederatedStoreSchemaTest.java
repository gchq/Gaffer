/*
 * Copyright 2017-2019 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
    public static final HashMapGraphLibrary LIBRARY = new HashMapGraphLibrary();
    public static final String ACC_PROP = "accProp";

    private FederatedStore fStore;
    private static final AccumuloProperties ACCUMULO_PROPERTIES = new AccumuloProperties();
    private static final FederatedStoreProperties FEDERATED_PROPERTIES = new FederatedStoreProperties();
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";

    @Before
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();
        ACCUMULO_PROPERTIES.setStoreClass(MockAccumuloStore.class);
        ACCUMULO_PROPERTIES.setStorePropertiesClass(AccumuloProperties.class);

        FEDERATED_PROPERTIES.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        fStore = new FederatedStore();
        fStore.initialise(TEST_FED_STORE, null, FEDERATED_PROPERTIES);

        fStore.setGraphLibrary(LIBRARY);

        testUser = testUser();
        testContext = new Context(testUser);
    }

    @Test
    public void shouldBeAbleToAddGraphsWithSchemaCollisions() throws Exception {
        LIBRARY.addProperties(ACC_PROP, ACCUMULO_PROPERTIES);
        fStore.setGraphLibrary(LIBRARY);

        String aSchema1ID = "aSchema";
        final Schema aSchema = new Schema.Builder()
                .edge("e1", getProp("prop1"))
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        LIBRARY.addSchema(aSchema1ID, aSchema);

        fStore.execute(OperationChain.wrap(
                new AddGraph.Builder()
                        .graphId("a")
                        .parentPropertiesId(ACC_PROP)
                        .parentSchemaIds(Lists.newArrayList(aSchema1ID))
                        .build()), testContext);

        String bSchema1ID = "bSchema";
        final Schema bSchema = new Schema.Builder()
                .edge("e1", getProp("prop2"))
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        LIBRARY.addSchema(bSchema1ID, bSchema);

        assertFalse(LIBRARY.exists("b"));

        fStore.execute(OperationChain.wrap(new AddGraph.Builder()
                .graphId("b")
                .parentPropertiesId(ACC_PROP)
                .parentSchemaIds(Lists.newArrayList(bSchema1ID))
                .build()), testContext);

        fStore.execute(OperationChain.wrap(new AddGraph.Builder()
                .graphId("c")
                .parentPropertiesId(ACC_PROP)
                .parentSchemaIds(Lists.newArrayList(aSchema1ID))
                .build()), testContext);

        // No exceptions thrown - as all 3 graphs should be able to be added together.
    }

    @Test
    public void shouldGetCorrectDefaultViewForAChosenGraphOperation() throws
            Exception {
        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        library.add("a", new Schema.Builder()
                .edge("e1", getProp("prop1"))
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build(), ACCUMULO_PROPERTIES);

        library.add("b", new Schema.Builder()
                .edge("e1", getProp("prop2"))
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build(), ACCUMULO_PROPERTIES);


        fStore.execute(new AddGraph.Builder()
                .graphId("a")
                .build(), testContext);

        fStore.execute(new AddGraph.Builder()
                .graphId("b")
                .build(), testContext);

        final CloseableIterable<? extends Element> a = fStore.execute(new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        //No view so makes default view, should get only view compatible with graph "a"
                        .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "a")
                        .build())
                .build(), testContext);

        assertNotNull(a);
        assertFalse(a.iterator().hasNext());
    }

    private SchemaEdgeDefinition getProp(final String propName) {
        return new SchemaEdgeDefinition.Builder()
                .source(STRING)
                .destination(STRING)
                .directed(DIRECTED_EITHER)
                .property(propName, STRING)
                .build();
    }
}
