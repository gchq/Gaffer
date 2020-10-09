/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.federatedstore.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedAccess;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.PublicAccessPredefinedFederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.ChangeGraphAccess;
import uk.gov.gchq.gaffer.federatedstore.operation.ChangeGraphId;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public class FederatedAdminIT extends AbstractStoreIT {

    public static final User ADMIN_USER = new User("admin", Collections.EMPTY_SET, Sets.newHashSet("AdminAuth"));
    public static final User NOT_ADMIN_USER = new User("admin", Collections.EMPTY_SET, Sets.newHashSet("NotAdminAuth"));

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties ACCUMULO_PROPERTIES = AccumuloProperties.loadStoreProperties(
            StreamUtil.openStream(currentClass, "properties/singleUseAccumuloStore.properties"));

    @Override
    protected Schema createSchema() {
        final Schema.Builder schemaBuilder = new Schema.Builder(createDefaultSchema());
        schemaBuilder.edges(Collections.EMPTY_MAP);
        schemaBuilder.entities(Collections.EMPTY_MAP);
        return schemaBuilder.build();
    }

    @Before
    public void setUp() throws Exception {
        graph.execute(new RemoveGraph.Builder()
                .graphId(PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_EDGES)
                .build(), user);
        graph.execute(new RemoveGraph.Builder()
                .graphId(PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_ENTITIES)
                .build(), user);
    }

    @Test
    public void shouldRemoveGraphForAdmin() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Boolean removed = graph.execute(new RemoveGraph.Builder()
                .graphId(graphA)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertTrue(removed);
        assertEquals(0, Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).size());

    }

    @Test
    public void shouldNotRemoveGraphForNonAdmin() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Boolean removed = graph.execute(new RemoveGraph.Builder()
                .graphId(graphA)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), NOT_ADMIN_USER);

        //then
        assertFalse(removed);
        assertEquals(1, Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).size());

    }

    @Test
    public void shouldGetAllGraphIdsForAdmin() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Iterable<? extends String> adminGraphIds = graph.execute(new GetAllGraphIds.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertTrue(Lists.newArrayList(adminGraphIds).contains(graphA));
    }

    @Test
    public void shouldNotGetAllGraphIdsForNonAdmin() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Iterable<? extends String> adminGraphIds = graph.execute(new GetAllGraphIds.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), NOT_ADMIN_USER);

        //then
        assertFalse(Lists.newArrayList(adminGraphIds).contains(graphA));
    }

    @Test
    public void shouldGetAllGraphInfoForAdmin() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("authsValueA")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        final FederatedAccess expectedFedAccess = new FederatedAccess.Builder().addingUserId(user.getUserId()).graphAuths("authsValueA").makePrivate().build();

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertNotNull(allGraphsAndAuths);
        assertFalse(allGraphsAndAuths.isEmpty());
        assertEquals(1, allGraphsAndAuths.size());
        assertEquals(graphA, allGraphsAndAuths.keySet().toArray(new String[]{})[0]);
        assertEquals(expectedFedAccess, allGraphsAndAuths.values().toArray(new Object[]{})[0]);

    }

    @Test
    public void shouldNotGetAllGraphInfoForNonAdmin() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder().build(), NOT_ADMIN_USER);

        assertNotNull(allGraphsAndAuths);
        assertTrue(allGraphsAndAuths.isEmpty());
    }

    @Test
    public void shouldNotGetAllGraphInfoForNonAdminWithAdminDeclarationsInOption() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), NOT_ADMIN_USER);

        assertNotNull(allGraphsAndAuths);
        assertTrue(allGraphsAndAuths.isEmpty());
    }

    @Test
    public void shouldNotGetAllGraphInfoForAdminWithoutAdminDeclartionInOptions() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder().build(), ADMIN_USER);

        assertNotNull(allGraphsAndAuths);
        assertTrue(allGraphsAndAuths.isEmpty());
    }

    @Test
    public void shouldGetGraphInfoForSelectedGraphsOnly() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("authsValueA")
                .build(), user);
        final String graphB = "graphB";
        graph.execute(new AddGraph.Builder()
                .graphId(graphB)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("authsValueB")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphB));
        final FederatedAccess expectedFedAccess = new FederatedAccess.Builder().addingUserId(user.getUserId()).graphAuths("authsValueB").makePrivate().build();

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder().option(KEY_OPERATION_OPTIONS_GRAPH_IDS, graphB).build(), user);

        //then
        assertNotNull(allGraphsAndAuths);
        assertFalse(allGraphsAndAuths.isEmpty());
        assertEquals(1, allGraphsAndAuths.size());
        assertEquals(1, allGraphsAndAuths.size());
        assertEquals(graphB, allGraphsAndAuths.keySet().toArray(new String[]{})[0]);
        assertEquals(expectedFedAccess, allGraphsAndAuths.values().toArray(new Object[]{})[0]);
    }

    @Test
    public void shouldChangeGraphUserFromOwnGraphToReplacementUser() throws Exception {
        //given
        final String graphA = "graphA";
        final User replacementUser = new User("replacement");
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser)).contains(graphA));

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(replacementUser.getUserId())
                .build(), user);

        //then
        assertTrue(changed);
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser)).contains(graphA));

    }

    @Test
    public void shouldChangeGraphUserFromSomeoneElseToReplacementUserAsAdminWhenRequestingAdminAccess() throws Exception {
        //given
        final String graphA = "graphA";
        final User replacementUser = new User("replacement");
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser)).contains(graphA));

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(replacementUser.getUserId())
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertTrue(changed);
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser)).contains(graphA));

    }

    @Test
    public void shouldNotChangeGraphUserFromSomeoneElseToReplacementUserAsAdminWhenNotRequestingAdminAccess() throws Exception {
        //given
        final String graphA = "graphA";
        final User replacementUser = new User("replacement");
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser)).contains(graphA));

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(replacementUser.getUserId())
                .build(), ADMIN_USER);

        //then
        assertFalse(changed);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser)).contains(graphA));

    }

    @Test
    public void shouldNotChangeGraphUserFromSomeoneElseToReplacementUserAsNonAdminWhenRequestingAdminAccess() throws Exception {
        //given
        final String graphA = "graphA";
        final User replacementUser = new User("replacement");
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser)).contains(graphA));

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(replacementUser.getUserId())
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), replacementUser);

        //then
        assertFalse(changed);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser)).contains(graphA));
    }

    @Test
    public void shouldChangeGraphIdForOwnGraph() throws Exception {
        //given
        final String graphA = "graphA";
        final String graphB = "graphB";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(graphB)
                .build(), user);

        //then
        assertTrue(changed);
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphB));

    }

    @Test
    public void shouldChangeGraphIdForNonOwnedGraphAsAdminWhenRequestingAdminAccess() throws Exception {
        //given
        final String graphA = "graphA";
        final String graphB = "graphB";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(graphB)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertTrue(changed);
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphB));

    }

    @Test
    public void shouldNotChangeGraphIdForNonOwnedGraphAsAdminWhenNotRequestingAdminAccess() throws Exception {
        //given
        final String graphA = "graphA";
        final String graphB = "graphB";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(graphB)
                .build(), ADMIN_USER);

        //then
        assertFalse(changed);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphB));

    }

    @Test
    public void shouldNotChangeGraphIdForNonOwnedGraphAsNonAdminWhenRequestingAdminAccess() throws Exception {
        //given
        final String graphA = "graphA";
        final String graphB = "graphB";
        final User otherUser = new User("other");
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), otherUser)).contains(graphA));

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(graphB)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), otherUser);

        //then
        assertFalse(changed);
        assertTrue(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user)).contains(graphB));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), otherUser)).contains(graphA));
        assertFalse(Lists.newArrayList(graph.execute(new GetAllGraphIds(), otherUser)).contains(graphB));
    }
}
