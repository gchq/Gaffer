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

package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreGraphVisibilityTest {

    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String TEST_STORE_PROPS_ID = "testStorePropsId";
    private static final String TEST_SCHEMA_ID = "testSchemaId";
    private static final String TEST_GRAPH_ID = "testGraphId";
    private static final String TEST_FED_GRAPH_ID = "testFedGraphId";
    private static User addingUser;
    private static User nonAddingUser;
    private static User authNonAddingUser;
    private Graph fedGraph;
    private FederatedStoreProperties fedProperties;
    private HashMapGraphLibrary library;

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "properties/singleUseAccumuloStore.properties"));

    @BeforeEach
    public void setUp() throws Exception {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();

        fedProperties = new FederatedStoreProperties();
        fedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        addingUser = testUser();
        nonAddingUser = blankUser();
        authNonAddingUser = authUser();
        library = new HashMapGraphLibrary();
    }

    @AfterAll
    public static void tearDownCache() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldNotShowHiddenGraphIdWithIDs() throws Exception {

        final Schema aSchema = new Schema.Builder()
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", String.class)
                .build();

        library.add(TEST_GRAPH_ID, TEST_SCHEMA_ID, aSchema, TEST_STORE_PROPS_ID, PROPERTIES);

        fedGraph = new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(TEST_FED_GRAPH_ID)
                        .library(library)
                        .build())
                .addStoreProperties(fedProperties)
                .build();

        fedGraph.execute(
                new AddGraph.Builder()
                        .graphId("g1")
                        .parentPropertiesId(TEST_STORE_PROPS_ID) // <- with ID
                        .parentSchemaIds(Arrays.asList(TEST_SCHEMA_ID)) // <- with ID
                        .build(),
                addingUser);

        fedGraph.execute(
                new AddGraph.Builder()
                        .graphId("g2")
                        .parentPropertiesId(TEST_STORE_PROPS_ID) // <- with ID
                        .parentSchemaIds(Arrays.asList(TEST_SCHEMA_ID)) // <- with ID
                        .graphAuths("auth1")
                        .build(),
                addingUser);


        commonAssertions();
    }

    /*
     * Adhoc test to make sure that the naming of props and schemas without ID's
     * is still retrievable via the name of the graph that is was added to the LIBRARY.
     */
    @Test
    public void shouldNotShowHiddenGraphIdWithoutIDs() throws Exception {
        final Schema aSchema = new Schema.Builder() // <- without ID
                .entity("e1", new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", String.class)
                .build();

        library.add(TEST_GRAPH_ID, aSchema, PROPERTIES);

        fedGraph = new Builder()
                .config(new GraphConfig.Builder()
                        .graphId(TEST_FED_GRAPH_ID)
                        .library(library)
                        .build())
                .addStoreProperties(fedProperties)
                .build();

        fedGraph.execute(
                new AddGraph.Builder()
                        .graphId("g1")
                        .parentPropertiesId(TEST_GRAPH_ID) // <- without ID
                        .parentSchemaIds(Arrays.asList(TEST_GRAPH_ID)) // <- without ID
                        .build(),
                addingUser);

        fedGraph.execute(
                new AddGraph.Builder()
                        .graphId("g2")
                        .parentPropertiesId(TEST_GRAPH_ID) // <- without ID
                        .parentSchemaIds(Arrays.asList(TEST_GRAPH_ID)) // <- without ID
                        .graphAuths("auth1")
                        .build(),
                addingUser);


        commonAssertions();
    }

    private void commonAssertions() throws uk.gov.gchq.gaffer.operation.OperationException {
        Iterable<? extends String> graphIds = fedGraph.execute(
                new GetAllGraphIds(),
                nonAddingUser);


        final HashSet<Object> sets = Sets.newHashSet();
        Iterator<? extends String> iterator = graphIds.iterator();
        while (iterator.hasNext()) {
            sets.add(iterator.next());
        }

        assertNotNull(graphIds, "Returned iterator should not be null, it should be empty.");
        assertEquals(0, sets.size(), "Showing hidden graphId");


        graphIds = fedGraph.execute(
                new GetAllGraphIds(),
                authNonAddingUser);
        iterator = graphIds.iterator();

        sets.clear();
        while (iterator.hasNext()) {
            sets.add(iterator.next());
        }

        assertNotNull(graphIds, "Returned iterator should not be null, it should be empty.");
        assertEquals(1, sets.size(), "Not Showing graphId with correct auth");
        assertThat(sets).contains("g2");


        graphIds = fedGraph.execute(
                new GetAllGraphIds(),
                addingUser);
        iterator = graphIds.iterator();


        sets.clear();
        while (iterator.hasNext()) {
            sets.add(iterator.next());
        }

        assertNotNull(graphIds, "Returned iterator should not be null, it should be empty.");
        assertEquals(2, sets.size(), "Not Showing all graphId for adding user");
        assertThat(sets).contains("g1", "g2");
    }


}
