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
package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.collect.Sets;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * The tests successfully run in isolation but when run as a suite keep on getting the same strange
 * error (see below.) This has the same start up as FederatedStorePublicAccessTest,
 * FederatedStoreSchemaTest etc which have no issues
 *
 * [INFO] Running uk.gov.gchq.gaffer.federatedstore.AdminGetAllGraphInfoTest
 * client.impl.ServerClient WARN  - There are no tablet servers: check that zookeeper and accumulo are running.
 * core.client.ClientConfiguration WARN  - Found no client.conf in default paths. Using default client configuration values.
 * fate.zookeeper.ZooCache WARN  - Saw (possibly) transient exception communicating with ZooKeeper, will retry
 * org.apache.zookeeper.KeeperException$ConnectionLossException: KeeperErrorCode = ConnectionLoss for /accumulo/instances/instance01
 *    at org.apache.zookeeper.KeeperException.create(KeeperException.java:102)
 *    at org.apache.zookeeper.KeeperException.create(KeeperException.java:54)
 *    at org.apache.zookeeper.ZooKeeper.exists(ZooKeeper.java:1111)
 *    at org.apache.accumulo.fate.zookeeper.ZooCache$2.run(ZooCache.java:394)
 *    at org.apache.accumulo.fate.zookeeper.ZooCache$2.run(ZooCache.java:366)
 *    at org.apache.accumulo.fate.zookeeper.ZooCache$ZooRunnable.retry(ZooCache.java:260)
 *    at org.apache.accumulo.fate.zookeeper.ZooCache.get(ZooCache.java:423)
 *    at org.apache.accumulo.fate.zookeeper.ZooCache.get(ZooCache.java:352)
 *    at org.apache.accumulo.core.client.ZooKeeperInstance.getInstanceID(ZooKeeperInstance.java:198)
 *    at org.apache.accumulo.core.client.ZooKeeperInstance.<init>(ZooKeeperInstance.java:177)
 *    at org.apache.accumulo.core.client.ZooKeeperInstance.<init>(ZooKeeperInstance.java:189)
 *    at org.apache.accumulo.core.client.ZooKeeperInstance.<init>(ZooKeeperInstance.java:92)
 *    at uk.gov.gchq.gaffer.accumulostore.utils.TableUtils.getConnector(TableUtils.java:215)
 *    at uk.gov.gchq.gaffer.accumulostore.AccumuloStore.getConnection(AccumuloStore.java:205)
 *    at uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore.preInitialise(SingleUseMiniAccumuloStore.java:48)
 *    at uk.gov.gchq.gaffer.accumulostore.AccumuloStore.initialise(AccumuloStore.java:159)
 *    at uk.gov.gchq.gaffer.store.Store.createStore(Store.java:260)
 *    at uk.gov.gchq.gaffer.graph.Graph$Builder.updateStore(Graph.java:1066)
 *    at uk.gov.gchq.gaffer.graph.Graph$Builder.build(Graph.java:953)
 *    at uk.gov.gchq.gaffer.graph.GraphSerialisable.getGraph(GraphSerialisable.java:105)
 *    at uk.gov.gchq.gaffer.graph.GraphSerialisable.getGraph(GraphSerialisable.java:86)
 *    at uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.put(FederatedGraphStorage.java:119)
 *    at uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.makeGraphFromCache(FederatedGraphStorage.java:505)
 *    at uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.makeAllGraphsFromCache(FederatedGraphStorage.java:512)
 *    at uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.startCacheServiceLoader(FederatedGraphStorage.java:76)
 *    at uk.gov.gchq.gaffer.federatedstore.FederatedStore.startCacheServiceLoader(FederatedStore.java:487)
 *    at uk.gov.gchq.gaffer.store.Store.initialise(Store.java:278)
 *    at uk.gov.gchq.gaffer.federatedstore.FederatedStore.initialise(FederatedStore.java:144)
 *    at uk.gov.gchq.gaffer.federatedstore.AdminGetAllGraphInfoTest.setUp(AdminGetAllGraphInfoTest.java:80)
 *    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
 *    at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
 *    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
 *    at java.lang.reflect.Method.invoke(Method.java:498)
 *    at org.junit.platform.commons.util.ReflectionUtils.invokeMethod(ReflectionUtils.java:686)
 */
@Disabled
public class AdminGetAllGraphInfoTest {

    private static final String ADMIN_AUTH = "AdminAuth";
    private static final User ADMIN_USER = new User("adminUser", null, Sets.newHashSet(ADMIN_AUTH));
    private static final Class CURRENT_CLASS = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES =
            AccumuloProperties.loadStoreProperties(StreamUtil.openStream(CURRENT_CLASS, "properties/accumuloStore.properties"));
    private static MiniAccumuloClusterManager miniAccumuloClusterManager = null;

    private FederatedAccess access;
    private FederatedStore store;

    @BeforeAll
    public static void setUpStore(@TempDir Path tempDir) throws IOException {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @BeforeEach
    public void setUp() throws Exception {
        access = new FederatedAccess(Sets.newHashSet("authA"), "testuser1", false, FederatedGraphStorage.DEFAULT_DISABLED_BY_DEFAULT);
        store = new FederatedStore();
        final StoreProperties fedProps = new StoreProperties();
        fedProps.set(StoreProperties.ADMIN_AUTH, ADMIN_AUTH);
        store.initialise("testFedStore", null, fedProps);
    }

    @Test
    public void shouldGetAllGraphsAndAuthsAsAdmin() throws Exception {
        final String graph1 = "graph1";

        System.out.println(store.getAllGraphIds(ADMIN_USER, true));
        store.addGraphs(access, new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graph1)
                        .build())
                .schema(new Schema())
                .properties(PROPERTIES)
                .build());

        final Map<String, Object> allGraphsAndAuths = store.getAllGraphsAndAuths(ADMIN_USER, null, true);

        assertNotNull(allGraphsAndAuths);
        assertFalse(allGraphsAndAuths.isEmpty());
        assertEquals(graph1, allGraphsAndAuths.keySet().toArray(new String[]{})[0]);
    }

    @Test
    public void shouldNotGetAllGraphsAndAuthsAsAdmin() throws Exception {
        final String graph1 = "graph1";

        System.out.println(store.getAllGraphIds(ADMIN_USER, true));
        store.addGraphs(access, new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(graph1)
                        .build())
                .schema(new Schema())
                .properties(PROPERTIES)
                .build());

        final Map<String, Object> allGraphsAndAuths = store.getAllGraphsAndAuths(new User(), null, true);

        assertNotNull(allGraphsAndAuths);
        assertTrue(allGraphsAndAuths.isEmpty());
    }
}
