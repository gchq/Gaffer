/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.traffic;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.proxystore.ProxyStore;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.service.v2.RestApiV2TestClient;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.traffic.listeners.DataLoader;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;

/**
 * Runs {@link RoadTrafficTestQueries} against a GAFFER REST API that is linked to a store that the example Road Traffic
 * data set has been loaded into.
 */
public class RoadTrafficRestApiSTs extends RoadTrafficTestQueries {

    public static final String STORE_TYPE_PROPERTY = "store.type";
    public static final String STORE_TYPE_DEFAULT = "accumulo";

    protected static final RestApiTestClient CLIENT = new RestApiV2TestClient();

    @TempDir
    public static File testFolder;

    @BeforeAll
    public static void prepareRestApi() throws IOException {
        // Spin up the REST API
        CLIENT.startServer();

        // Connect it to a Gaffer store, as specified in the 'store.type' property
        CLIENT.reinitialiseGraph(
                testFolder,
                Schema.fromJson(StreamUtil.schemas(ElementGroup.class)),
                StoreProperties.loadStoreProperties(StreamUtil.openStream(RoadTrafficRestApiSTs.class, System.getProperty(STORE_TYPE_PROPERTY, STORE_TYPE_DEFAULT) + StreamUtil.STORE_PROPERTIES))
        );

        // Load Road Traffic data into the store
        final DataLoader loader = new DataLoader();
        loader.contextInitialized(null);
    }

    @BeforeEach
    @Override
    public void prepareProxy() {
        ProxyProperties props = new ProxyProperties(System.getProperties());
        props.setStoreClass(ProxyStore.class);
        props.setStorePropertiesClass(props.getClass());

        this.graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(props)
                .build();

        this.user = new User();
    }

}
