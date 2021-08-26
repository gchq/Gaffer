/*
 * Copyright 2020-2021 Crown Copyright
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
import org.apache.accumulo.core.client.Connector;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedAccess;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreCache;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.PublicAccessPredefinedFederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.ChangeGraphAccess;
import uk.gov.gchq.gaffer.federatedstore.operation.ChangeGraphId;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

public class FederatedAdminIT extends AbstractStandaloneFederatedStoreIT {

    public static final User ADMIN_USER = new User("admin", Collections.EMPTY_SET, Sets.newHashSet("AdminAuth"));
    public static final User NOT_ADMIN_USER = new User("admin", Collections.EMPTY_SET, Sets.newHashSet("NotAdminAuth"));

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties ACCUMULO_PROPERTIES = AccumuloProperties.loadStoreProperties(
            StreamUtil.openStream(currentClass, "properties/singleUseAccumuloStore.properties"));

    @Override
    protected Schema createSchema() {
        final Schema.Builder schemaBuilder = new Schema.Builder(AbstractStoreIT.createDefaultSchema());
        schemaBuilder.edges(Collections.EMPTY_MAP);
        schemaBuilder.entities(Collections.EMPTY_MAP);
        return schemaBuilder.build();
    }

    @Override
    public void _setUp() throws Exception {
        graph.execute(new RemoveGraph.Builder()
                .graphId(PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_EDGES)
                .build(), user);
        graph.execute(new RemoveGraph.Builder()
                .graphId(PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_ENTITIES)
                .build(), user);
    }

    @Test
    public void shouldRemoveGraphFromStorage() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Boolean removed = graph.execute(new RemoveGraph.Builder()
                .graphId(graphA)
                .build(), user);

        //then
        assertThat(removed).isTrue();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).isEmpty();

    }

    @Test
    public void shouldRemoveGraphFromCache() throws Exception {
        //given
        FederatedStoreCache federatedStoreCache = new FederatedStoreCache();
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        assertThat(federatedStoreCache.getGraphSerialisableFromCache(graphA)).isNotNull();
        final Boolean removed = graph.execute(new RemoveGraph.Builder()
                .graphId(graphA)
                .build(), user);

        //then
        assertThat(removed).isTrue();
        GraphSerialisable graphSerialisableFromCache = federatedStoreCache.getGraphSerialisableFromCache(graphA);
        assertThat(graphSerialisableFromCache)
                .as(new String(JSONSerialiser.serialise(graphSerialisableFromCache, true)))
                .isNull();
        assertThat(federatedStoreCache.getAllGraphIds()).isEmpty();
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Boolean removed = graph.execute(new RemoveGraph.Builder()
                .graphId(graphA)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertThat(removed).isTrue();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).isEmpty();

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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Boolean removed = graph.execute(new RemoveGraph.Builder()
                .graphId(graphA)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), NOT_ADMIN_USER);

        //then
        assertThat(removed).isFalse();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).hasSize(1);

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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Iterable<? extends String> adminGraphIds = graph.execute(new GetAllGraphIds.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        Assertions.<String>assertThat(adminGraphIds).contains(graphA);
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Iterable<? extends String> adminGraphIds = graph.execute(new GetAllGraphIds.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), NOT_ADMIN_USER);

        //then
        Assertions.<String>assertThat(adminGraphIds).doesNotContain(graphA);
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);
        final FederatedAccess expectedFedAccess = new FederatedAccess.Builder().addingUserId(user.getUserId()).graphAuths("authsValueA").makePrivate().build();

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertThat(allGraphsAndAuths)
                .hasSize(1);
        assertThat(allGraphsAndAuths.keySet().toArray(new String[]{})[0]).isEqualTo(graphA);
        assertThat(allGraphsAndAuths.values().toArray(new Object[]{})[0]).isEqualTo(expectedFedAccess);

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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder().build(), NOT_ADMIN_USER);

        assertThat(allGraphsAndAuths)
                .isEmpty();
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder()
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), NOT_ADMIN_USER);

        assertThat(allGraphsAndAuths)
                .isEmpty();
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder().build(), ADMIN_USER);

        assertThat(allGraphsAndAuths)
                .isEmpty();
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA, graphB);
        final FederatedAccess expectedFedAccess = new FederatedAccess.Builder().addingUserId(user.getUserId()).graphAuths("authsValueB").makePrivate().build();

        //when
        final Map<String, Object> allGraphsAndAuths = graph.execute(new GetAllGraphInfo.Builder().option(KEY_OPERATION_OPTIONS_GRAPH_IDS, graphB).build(), user);

        //then
        assertThat(allGraphsAndAuths)
                .hasSize(1)
                .hasSize(1);
        assertThat(allGraphsAndAuths.keySet().toArray(new String[]{})[0]).isEqualTo(graphB);
        assertThat(allGraphsAndAuths.values().toArray(new Object[]{})[0]).isEqualTo(expectedFedAccess);
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser))).doesNotContain(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(replacementUser.getUserId())
                .build(), user);

        //then
        assertThat(changed).isTrue();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).doesNotContain(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser))).contains(graphA);

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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser))).doesNotContain(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(replacementUser.getUserId())
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertThat(changed).isTrue();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).doesNotContain(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser))).contains(graphA);

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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser))).doesNotContain(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(replacementUser.getUserId())
                .build(), ADMIN_USER);

        //then
        assertThat(changed).isFalse();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser))).doesNotContain(graphA);
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser))).doesNotContain(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(replacementUser.getUserId())
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), replacementUser);

        //then
        assertThat(changed).isFalse();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), replacementUser))).doesNotContain(graphA);
    }

    @Test
    public void shouldChangeGraphIdForOwnGraph() throws Exception {
        //given
        final String graphA = "graphTableA";
        final String graphB = "graphTableB";
        Connector connector = TableUtils.getConnector(ACCUMULO_PROPERTIES.getInstance(),
                ACCUMULO_PROPERTIES.getZookeepers(),
                ACCUMULO_PROPERTIES.getUser(),
                ACCUMULO_PROPERTIES.getPassword());

        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .graphAuths("Auths1")
                .build(), user);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        boolean tableGraphABefore = connector.tableOperations().exists(graphA);
        boolean tableGraphBBefore = connector.tableOperations().exists(graphB);

        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(graphB)
                .build(), user);

        boolean tableGraphAfter = connector.tableOperations().exists(graphA);
        boolean tableGraphBAfter = connector.tableOperations().exists(graphB);

        //then
        assertThat(changed).isTrue();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).doesNotContain(graphA)
                .contains(graphB);
        assertThat(tableGraphABefore).isTrue();
        assertThat(tableGraphBBefore).isFalse();
        assertThat(tableGraphAfter).isFalse();
        assertThat(tableGraphBAfter).isTrue();
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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(graphB)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), ADMIN_USER);

        //then
        assertThat(changed).isTrue();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).doesNotContain(graphA)
                .contains(graphB);

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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(graphB)
                .build(), ADMIN_USER);

        //then
        assertThat(changed).isFalse();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA)
                .doesNotContain(graphB);

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
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), otherUser))).doesNotContain(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(graphB)
                .option(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "true")
                .build(), otherUser);

        //then
        assertThat(changed).isFalse();
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA)
                .doesNotContain(graphB);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), otherUser))).doesNotContain(graphA, graphB);
    }

    @Test
    public void shouldStartWithEmptyCache() throws Exception {
        //given
        FederatedStoreCache federatedStoreCache = new FederatedStoreCache();

        //then
        assertThat(federatedStoreCache.getAllGraphIds()).isEmpty();
    }

    @Test
    public void shouldChangeGraphIdInStorage() throws Exception {
        //given
        String newName = "newName";
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(newName)
                .build(), user);

        //then
        ArrayList<String> graphIds = Lists.newArrayList(graph.execute(new GetAllGraphIds(), user));

        assertThat(changed).isTrue();
        assertThat(graphIds).hasSize(1);
        assertThat(graphIds.toArray()).containsExactly(new String[]{newName});
    }

    @Test
    public void shouldChangeGraphIdInCache() throws Exception {
        //given
        String newName = "newName";
        FederatedStoreCache federatedStoreCache = new FederatedStoreCache();
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphId.Builder()
                .graphId(graphA)
                .newGraphId(newName)
                .build(), user);

        //then
        Set<String> graphIds = federatedStoreCache.getAllGraphIds();

        assertThat(changed).isTrue();
        assertThat(graphIds.toArray())
                .as(graphIds.toString())
                .containsExactly(new String[]{newName});
    }

    @Test
    public void shouldChangeGraphAccessIdInStorage() throws Exception {
        //given
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(NOT_ADMIN_USER.getUserId())
                .build(), user);

        //then
        ArrayList<String> userGraphIds = Lists.newArrayList(graph.execute(new GetAllGraphIds(), user));
        ArrayList<String> otherUserGraphIds = Lists.newArrayList(graph.execute(new GetAllGraphIds(), NOT_ADMIN_USER));

        assertThat(changed).isTrue();
        assertThat(userGraphIds).isEmpty();
        assertThat(otherUserGraphIds).hasSize(1);
        assertThat(otherUserGraphIds.toArray()).containsExactly(new String[]{graphA});
    }

    @Test
    public void shouldChangeGraphAccessIdInCache() throws Exception {
        //given
        FederatedStoreCache federatedStoreCache = new FederatedStoreCache();
        final String graphA = "graphA";
        graph.execute(new AddGraph.Builder()
                .graphId(graphA)
                .schema(new Schema())
                .storeProperties(ACCUMULO_PROPERTIES)
                .build(), user);
        assertThat(Lists.newArrayList(graph.execute(new GetAllGraphIds(), user))).contains(graphA);

        //when
        FederatedAccess before = federatedStoreCache.getAccessFromCache(graphA);
        final Boolean changed = graph.execute(new ChangeGraphAccess.Builder()
                .graphId(graphA)
                .ownerUserId(ADMIN_USER.getUserId())
                .build(), user);
        FederatedAccess after = federatedStoreCache.getAccessFromCache(graphA);

        //then
        assertThat(changed).isTrue();
        assertThat(after).isNotEqualTo(before);
        assertThat(before.getAddingUserId()).isEqualTo(user.getUserId());
        assertThat(after.getAddingUserId()).isEqualTo(ADMIN_USER.getUserId());
    }

}
