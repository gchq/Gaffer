///*
// * Copyright 2016 Crown Copyright
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package uk.gov.gchq.gaffer.rest;
//
//import org.glassfish.grizzly.http.server.HttpServer;
//import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
//import org.glassfish.jersey.server.ResourceConfig;
//import org.junit.BeforeClass;
//import org.junit.Ignore;
//import uk.gov.gchq.gaffer.commonutil.StreamUtil;
//import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
//import uk.gov.gchq.gaffer.rest.service.SimpleGraphConfigurationService;
//import uk.gov.gchq.gaffer.rest.service.SimpleOperationService;
//import uk.gov.gchq.gaffer.rest.service.StatusService;
//import uk.gov.gchq.gaffer.store.StoreProperties;
//import java.net.URI;
//import java.util.logging.ConsoleHandler;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//import static org.glassfish.hk2.utilities.ServiceLocatorUtilities.bind;
//
//@Ignore
//public class ProxyStoreITs extends AbstractStoreITs {
//    private static final StoreProperties STORE_PROPERTIES = StoreProperties.loadStoreProperties(StreamUtil
//            .storeProps(ProxyStoreITs.class));
//
//    private static HttpServer server;
//
//    public ProxyStoreITs() {
//        super(STORE_PROPERTIES);
//    }
//
//    @BeforeClass
//    public static void beforeClass() {
//        // start REST
//        server = GrizzlyHttpServerFactory.createHttpServer(URI.create("http://localhost:8080/rest/v1"), new ApplicationConfig());
//
//        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, "/home/user/projects/gaffer/store-implementation/proxy-store/src/test/resources/store.properties");
//        System.setProperty(SystemProperty.SCHEMA_PATHS, "/home/user/projects/gaffer/store-implementation/proxy-store/src/test/resources/proxy-schema.json");
//
//        Logger l = Logger.getLogger("org.glassfish.grizzly.http.server.HttpHandler");
//        l.setLevel(Level.FINE);
//        l.setUseParentHandlers(false);
//        ConsoleHandler ch = new ConsoleHandler();
//        ch.setLevel(Level.ALL);
//        l.addHandler(ch);
//    }
//
//    public static class ApplicationConfig extends ResourceConfig {
//
//        public ApplicationConfig() {
//            register(SimpleOperationService.class);
//            register(SimpleGraphConfigurationService.class);
//            register(StatusService.class);
//        }
//    }
//}
