/*
 * Copyright 2023-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.util;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.user.User;

public final class GafferPopTestUtil {
    public static final String USER_ID = "user01";
    public static final String AUTH_1 = "auth1";
    public static final String AUTH_2 = "auth2";

    private static final FederatedStoreProperties FEDERATED_STORE_PROPERTIES = FederatedStoreProperties
        .loadStoreProperties("/federatedStore/fed-store.properties");
    private static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties
    .loadStoreProperties("/tinkerpop/map-store.properties");
    private static final AccumuloProperties ACCUMULO_STORE_PROPERTIES =
        AccumuloProperties.loadStoreProperties("/gaffer/store.properties");

    private GafferPopTestUtil() {

    }
    public static final Configuration TEST_CONFIGURATION_1 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.GRAPH, GafferPopGraph.class.getName());
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
        }
    };

    public static final Configuration TEST_CONFIGURATION_2 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
            this.setProperty(GafferPopGraph.GRAPH_ID, "Graph1");
            this.setProperty(GafferPopGraph.STORE_PROPERTIES, GafferPopTestUtil.class.getClassLoader().getResource("gaffer/store.properties").getPath());
            this.setProperty(GafferPopGraph.GET_ELEMENTS_LIMIT, 1);
            this.setProperty(GafferPopGraph.HAS_STEP_FILTER_STAGE, "POST_TRANSFORM");
        }
    };

    public static final Configuration TEST_CONFIGURATION_3 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
        }
    };

    public static Graph getGafferGraph(Class<?> clazz, StoreProperties properties) {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .storeProperties(properties)
                .addSchemas(StreamUtil.openStreams(clazz, "/gaffer/schema"))
                .build();
    }

    public static User getTestUser(String... auths) {
        return new User.Builder()
                .userId(USER_ID)
                .dataAuths(auths)
                .build();
    }

    public enum StoreType {
        MAP, ACCUMULO, FEDERATED;
    }

    public static StoreProperties getStoreProperties(StoreType storeType) {
        switch (storeType) {
            case MAP:
                return MAP_STORE_PROPERTIES;
            case ACCUMULO:
                return ACCUMULO_STORE_PROPERTIES;
            case FEDERATED:
                return FEDERATED_STORE_PROPERTIES;
            default:
                throw new IllegalArgumentException("Unknown StoreType: " + storeType);
        }
    }
}
