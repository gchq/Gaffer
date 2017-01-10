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

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.proxystore.ProxyStoreTest;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.application.ApplicationResourceConfig;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.net.URI;

@Ignore
public class ProxyStoreITs extends AbstractStoreITs {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyStoreTest.class);
    private static final StoreProperties STORE_PROPERTIES = StoreProperties.loadStoreProperties(StreamUtil.openStream(ProxyStoreITs.class, "/proxy-store.properties"));

    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(ProxyStoreTest.class));

    private static final String REST_URI = "http://localhost:8080/rest/v1";
    private static HttpServer server;

    public ProxyStoreITs() {
        super(STORE_PROPERTIES);
    }

    @BeforeClass
    public static void beforeClass() throws IOException, InterruptedException, StoreException {
        // start REST
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(REST_URI), new ApplicationResourceConfig());

        // set properties for REST service
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, "/home/user/projects/gaffer/store-implementation/proxy-store/src/test/resources/store.properties");
        System.setProperty(SystemProperty.SCHEMA_PATHS, "/home/user/projects/gaffer/store-implementation/proxy-store/src/test/resources/schema");
    }
}
