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

package uk.gov.gchq.gaffer.traffic;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
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

import java.io.IOException;
import java.net.URL;

/**
 * Spins up the Gaffer REST API and loads the example Road Traffic data set into the store specified by the 'store.type'
 * property and then runs the {@link RoadTrafficTestQueries} against it.
 */
public class RoadTrafficRestApiITs extends RoadTrafficTestQueries {

    public static final String STORE_TYPE_PROPERTY = "store.type";
    public static final String STORE_TYPE_DEFAULT = "accumulo";

    protected static final RestApiTestClient CLIENT = new RestApiV2TestClient();

    @ClassRule
    public static final TemporaryFolder TEST_FOLDER = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void prepareRestApi() throws IOException {
        // Spin up the REST API
        CLIENT.startServer();

        // Connect it to a Gaffer store, as specified in the 'store.type' property
        CLIENT.reinitialiseGraph(
                TEST_FOLDER,
                Schema.fromJson(StreamUtil.schemas(ElementGroup.class)),
                StoreProperties.loadStoreProperties(StreamUtil.openStream(RoadTrafficRestApiITs.class, System.getProperty(STORE_TYPE_PROPERTY, STORE_TYPE_DEFAULT) + StreamUtil.STORE_PROPERTIES))
        );

        // Load Road Traffic data into the store
        final DataLoader loader = new DataLoader();
        loader.contextInitialized(null);
    }

    @AfterClass
    public static void after() {
        CLIENT.stopServer();
    }

    @Override
    public void prepareProxy() throws IOException {
        // Create a proxy store that will proxy all queries to the REST API that has been spun up
        ProxyProperties props = new ProxyProperties(System.getProperties());
        props.setStoreClass(ProxyStore.class);
        props.setStorePropertiesClass(props.getClass());

        final URL restURL = new URL(CLIENT.getRoot());
        props.setGafferHost(restURL.getHost());
        props.setGafferPort(restURL.getPort());
        props.setGafferContextRoot(CLIENT.getPath());

        this.graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(props)
                .build();

        this.user = new User();
    }

}
