/*
 * Copyright 2017 Crown Copyright
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
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import static org.junit.Assert.assertFalse;

public class FederatedStoreSchemaTest {

    public static final String STRING = "string";
    public static final Schema STRING_SCHEMA = new Schema.Builder()
            .type(STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();
    public static final User TEST_USER = new User("testUser");
    public static final Context TEST_CONTEXT = new Context(TEST_USER);
    public static final String TEST_FED_STORE = "testFedStore";
    public static final HashMapGraphLibrary library = new HashMapGraphLibrary();
    public static final String ACC_PROP = "accProp";


    private FederatedStore fStore;
    public static final AccumuloProperties ACCUMULO_PROPERTIES = new AccumuloProperties();
    public static final StoreProperties FEDERATED_PROPERTIES = new FederatedStoreProperties();

    @Before
    public void setUp() throws Exception {
        ACCUMULO_PROPERTIES.setId(ACC_PROP);
        ACCUMULO_PROPERTIES.setStoreClass(MockAccumuloStore.class);
        ACCUMULO_PROPERTIES.setStorePropertiesClass(AccumuloProperties.class);

        fStore = new FederatedStore();
        fStore.initialise(TEST_FED_STORE, null, FEDERATED_PROPERTIES);

        library.clear();
    }

    @Test
    public void shouldBeAbleToAddGraphsWithSchemaCollisions() throws Exception {
        library.addProperties(ACC_PROP, ACCUMULO_PROPERTIES);
        fStore.setGraphLibrary(library);

        String aSchema1ID = "aSchema";
        final Schema aSchema = new Schema.Builder()
                .edge("e1", getProp("prop1"))
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema(aSchema1ID, aSchema);

        fStore.execute(OperationChain.wrap(
                new AddGraph.Builder()
                        .graphId("a")
                        .parentPropertiesId(ACC_PROP)
                        .parentSchemaIds(Lists.newArrayList(aSchema1ID))
                        .build()), TEST_CONTEXT);

        String bSchema1ID = "bSchema";
        final Schema bSchema = new Schema.Builder()
                .edge("e1", getProp("prop2"))
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema(bSchema1ID, bSchema);

        assertFalse(library.exists("b"));

        fStore.execute(Operation.asOperationChain(new AddGraph.Builder()
                .graphId("b")
                .parentPropertiesId(ACC_PROP)
                .parentSchemaIds(Lists.newArrayList(bSchema1ID))
                .build()), TEST_CONTEXT);

        fStore.execute(Operation.asOperationChain(new AddGraph.Builder()
                .graphId("c")
                .parentPropertiesId(ACC_PROP)
                .parentSchemaIds(Lists.newArrayList(aSchema1ID))
                .build()), TEST_CONTEXT);

        // No exceptions thrown - as all 3 graphs should be able to be added together.
    }

    private SchemaEdgeDefinition getProp(final String propName) {
        return new SchemaEdgeDefinition.Builder()
                .source(STRING)
                .destination(STRING)
                .property(propName, STRING)
                .build();
    }


}