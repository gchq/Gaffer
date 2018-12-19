/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore.integration;

import org.junit.Before;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;

public class MapStoreITs extends AbstractStoreITs {
    private static final MapStoreProperties STORE_PROPERTIES =
            MapStoreProperties.loadStoreProperties(StreamUtil.storeProps(MapStoreITs.class));

    public MapStoreITs() {
        super(STORE_PROPERTIES);
    }

    protected MapStoreITs(final StoreProperties storeProperties) {
        super(storeProperties);
    }

    @Before
    public void clearMap() {
        MapStore.resetStaticMap();
    }
}
