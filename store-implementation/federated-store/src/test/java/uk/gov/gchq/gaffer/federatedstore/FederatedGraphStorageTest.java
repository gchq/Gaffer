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
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.GRAPH_IDS_NOT_VISIBLE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.TEST_USER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.authUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.blankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedGraphStorageTest {

    public static final String GRAPH_ID = "a";
    public static final String X = "x";
    private FederatedGraphStorage graphStorage;
    private Graph a;
    private Graph b;
    private User nullUser;
    private User testUser;
    private User authUser;
    private FederatedAccess access;
    private User blankUser;
    private SchemaEntityDefinition e1;
    private SchemaEntityDefinition e2;

    @Before
    public void setUp() throws Exception {
        graphStorage = new FederatedGraphStorage();
        e1 = new SchemaEntityDefinition.Builder()
                .vertex("string")
                .build();
        a = new Builder()
                .config(new GraphConfig(GRAPH_ID))
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema.Builder()
                        .entity("e1", e1)
                        .type("string", String.class)
                        .build())
                .build();

        e2 = new SchemaEntityDefinition.Builder()
                .vertex("string2")
                .build();
        final AccumuloProperties accProperties = new AccumuloProperties();
        accProperties.setStoreClass(SingleUseMockAccumuloStore.class);

        b = new Builder()
                .config(new GraphConfig("b"))
                .storeProperties(accProperties)
                .addSchema(new Schema.Builder()
                        .entity("e2", e2)
                        .type("string2", String.class)
                        .build())
                .build();
        nullUser = null;
        testUser = testUser();
        authUser = authUser();
        access = new FederatedAccess(Sets.newHashSet(AUTH_1), TEST_USER);
        blankUser = blankUser();
    }

    @Test
    public void shouldStartWithNoTraits() throws Exception {
        final Set<StoreTrait> traits = graphStorage.getTraits();
        assertEquals(0, traits.size());
    }

    @Test
    public void shouldStartWithNoGraphs() throws Exception {
        final Collection<Graph> graphs = graphStorage.getAll(nullUser);
        assertEquals(0, graphs.size());
    }


    @Test
    public void shouldGetIdForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(testUser);
        assertEquals(1, allIds.size());
        assertEquals(GRAPH_ID, allIds.iterator().next());
    }

    @Test
    public void shouldGetIdForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(authUser);
        assertEquals(1, allIds.size());
        assertEquals(GRAPH_ID, allIds.iterator().next());
    }

    @Test
    public void shouldNotGetIdForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(blankUser);
        assertEquals(0, allIds.size());
        assertFalse(allIds.iterator().hasNext());
    }

    @Test
    public void shouldGetGraphForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(testUser);
        assertEquals(1, allGraphs.size());
        assertEquals(a, allGraphs.iterator().next());
    }

    @Test
    public void shouldGetGraphForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(authUser);
        assertEquals(1, allGraphs.size());
        assertEquals(a, allGraphs.iterator().next());
    }

    @Test
    public void shouldNotGetGraphForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(blankUser);
        assertEquals(0, allGraphs.size());
        assertFalse(allGraphs.iterator().hasNext());
    }

    @Test
    public void shouldGetGraphForAddingUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.get(testUser, Lists.newArrayList(GRAPH_ID));
        assertEquals(1, allGraphs.size());
        assertEquals(a, allGraphs.iterator().next());
    }

    @Test
    public void shouldGetGraphForAuthUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.get(authUser, Lists.newArrayList(GRAPH_ID));
        assertEquals(1, allGraphs.size());
        assertEquals(a, allGraphs.iterator().next());
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        try {
            graphStorage.get(blankUser, Lists.newArrayList(GRAPH_ID));
            fail("exception not thrown");
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(GRAPH_ID)), e.getMessage());
        }
    }

    @Test
    public void shouldNotGetGraphForAddingUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        try {
            graphStorage.get(testUser, Lists.newArrayList(X));
            fail("exception not thrown");
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)), e.getMessage());
        }
    }

    @Test
    public void shouldNotGetGraphForAuthUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        try {
            graphStorage.get(authUser, Lists.newArrayList(X));
            fail("exception not thrown");
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)), e.getMessage());
        }
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        try {
            graphStorage.get(blankUser, Lists.newArrayList(X));
            fail("exception not thrown");
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)), e.getMessage());
        }
    }

    @Test
    public void shouldSchemaShouldChangeWhenAddingGraphB() throws Exception {
        graphStorage.put(a, access);
        final Schema schemaA = graphStorage.getMergedSchema(testUser);
        assertEquals(1, schemaA.getTypes().size());
        assertEquals(String.class, schemaA.getType("string").getClazz());
        assertEquals(e1, schemaA.getElement("e1"));
        graphStorage.put(b, access);
        final Schema schemaAB = graphStorage.getMergedSchema(testUser);
        assertNotEquals(schemaA, schemaAB);
        assertEquals(2, schemaAB.getTypes().size());
        assertEquals(String.class, schemaAB.getType("string").getClazz());
        assertEquals(String.class, schemaAB.getType("string2").getClazz());
        assertEquals(e1, schemaAB.getElement("e1"));
        assertEquals(e2, schemaAB.getElement("e2"));
    }


    @Test
    public void shouldGetSchemaForAddingUser() throws Exception {
        graphStorage.put(a, access);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getMergedSchema(testUser);
        assertNotEquals("Revealing hidden schema", 2, schema.getTypes().size());
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType("string").getClazz());
        assertEquals(e1, schema.getElement("e1"));
    }

    @Test
    public void shouldGetSchemaForAuthUser() throws Exception {
        graphStorage.put(a, access);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getMergedSchema(authUser);
        assertNotEquals("Revealing hidden schema", 2, schema.getTypes().size());
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType("string").getClazz());
        assertEquals(e1, schema.getElement("e1"));
    }

    @Test
    public void shouldNotGetSchemaForBlankUser() throws Exception {
        graphStorage.put(a, access);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getMergedSchema(blankUser);
        assertNotEquals("Revealing hidden schema", 2, schema.getTypes().size());
        assertEquals("Revealing hidden schema", 0, schema.getTypes().size());
    }

    @Test
    public void shouldGetTraitsForAddingUser() throws Exception {
        graphStorage.put(a, new FederatedAccess(Sets.newHashSet(X), X));
        graphStorage.put(b, access);
        final Set<StoreTrait> traits = graphStorage.getTraits(testUser);
        assertNotEquals("Revealing hidden traits", 5, traits.size());
        assertEquals(9, traits.size());
    }

    @Test
    public void shouldGetTraitsForAuthUser() throws Exception {
        graphStorage.put(a, new FederatedAccess(Sets.newHashSet(X), X));
        graphStorage.put(b, access);
        final Set<StoreTrait> traits = graphStorage.getTraits(authUser);
        assertNotEquals("Revealing hidden traits", 5, traits.size());
        assertEquals(9, traits.size());
    }

    @Test
    public void shouldNotGetTraitsForBlankUser() throws Exception {
        graphStorage.put(a, new FederatedAccess(Sets.newHashSet(X), X));
        graphStorage.put(b, access);
        final Set<StoreTrait> traits = graphStorage.getTraits(blankUser);
        assertEquals("Revealing hidden traits", 0, traits.size());
    }

    @Test
    public void shouldRemoveForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID, testUser);
        assertTrue(remove);
    }

    @Test
    public void shouldRemoveForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID, authUser);
        assertTrue(remove);
    }

    @Test
    public void shouldNotRemoveForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID, blankUser);
        assertFalse(remove);
    }
}
