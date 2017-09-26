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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class FederatedStoreGraphVisabilityTest {

    public static final User AddingUser = new User.Builder()
            .userId("addingUser")
            .build();
    public static final User NonAddingUser = new User.Builder()
            .userId("NonAddingUser")
            .build();
    private Graph fedGraph;
    private StoreProperties fedProperties;

    @Before
    public void setUp() throws Exception {
        HashMapGraphLibrary.clear();
        fedProperties = new StoreProperties();
        fedProperties.setStorePropertiesClass(StoreProperties.class);
        fedProperties.setStoreClass(FederatedStore.class);

        final HashMapGraphLibrary library = new HashMapGraphLibrary();

        final Schema aSchema = new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", String.class)
                .build();

        final StoreProperties accProp = new AccumuloProperties();
        accProp.setStoreClass(MockAccumuloStore.class.getName());
        accProp.setStorePropertiesClass(AccumuloProperties.class);

        library.add("a", aSchema, accProp);

        fedGraph = new Builder()
                .config(new GraphConfig.Builder()
                        .graphId("testFedGraph")
                        .library(library)
                        .build())
                .addStoreProperties(fedProperties)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        HashMapGraphLibrary.clear();
    }

    @Test
    public void shouldNotShowHiddenGraphId() throws Exception {
        fedGraph.execute(
                new AddGraph.Builder()
                        .graphId("a")
                        .build(),
                AddingUser);

        final Iterable<? extends String> graphIds = fedGraph.execute(
                new GetAllGraphIds(),
                NonAddingUser);

        assertNotNull("Returned iterator should not be null, it should be empty.", graphIds);
        assertFalse("Showing hidden graphId", graphIds.iterator().hasNext());
    }
}