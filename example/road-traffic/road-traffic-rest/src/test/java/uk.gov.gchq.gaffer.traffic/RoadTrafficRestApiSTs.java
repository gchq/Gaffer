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

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.proxystore.ProxyStore;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StorePropertiesUtil;
import uk.gov.gchq.gaffer.user.User;

/**
 * Runs {@link RoadTrafficTestQueries} against a GAFFER REST API that is linked to a store that the example Road Traffic
 * data set has been loaded into.
 */
public class RoadTrafficRestApiSTs extends RoadTrafficTestQueries {

    @Override
    public void prepareProxy() {
        StoreProperties props = new StoreProperties(System.getProperties());
        StorePropertiesUtil.setStoreClass(props, ProxyStore.class);

        this.graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(props)
                .build();

        this.user = new User();
    }

}
