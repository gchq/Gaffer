/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.proxystore.integration;

import org.junit.AfterClass;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.proxystore.SingleUseMockAccumuloProxyStore;
import uk.gov.gchq.gaffer.store.StoreProperties;

public class ProxyStoreITs extends AbstractStoreITs {
    private static final StoreProperties STORE_PROPERTIES = StoreProperties.loadStoreProperties(StreamUtil.openStream(ProxyStoreITs.class, "/mock-proxy-store.properties"));

    public ProxyStoreITs() {
        super(STORE_PROPERTIES);
    }

    @AfterClass
    public static void afterClass() {
        System.out.println("stop service");
        SingleUseMockAccumuloProxyStore.cleanUp();
    }
}
