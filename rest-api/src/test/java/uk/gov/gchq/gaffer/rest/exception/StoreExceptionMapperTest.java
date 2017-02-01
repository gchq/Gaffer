/*
<<<<<<< Updated upstream:rest-api/src/test/java/uk/gov/gchq/gaffer/rest/exception/StoreExceptionMapperTest.java
 * Copyright 2017 Crown Copyright
=======
 * Copyright 2016 Crown Copyright
>>>>>>> Stashed changes:integration-test/src/test/java/uk/gov/gchq/gaffer/integration/impl/RestApiIT.java
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

<<<<<<< Updated upstream:rest-api/src/test/java/uk/gov/gchq/gaffer/rest/exception/StoreExceptionMapperTest.java
package uk.gov.gchq.gaffer.rest.exception;

import org.junit.Test;

public class StoreExceptionMapperTest {

    @Test
    public void storeExceptionShouldResultInInternalServerError() {

=======
package uk.gov.gchq.gaffer.integration.impl;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.rest.application.ApplicationResourceConfig;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

public class RestApiIT extends AbstractStoreIT {

    private final static Client client = ClientBuilder.newClient();
    private static HttpServer server;

    @BeforeClass
    public static void beforeClass() throws IOException, InterruptedException, StoreException {
        // start REST
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create("http://localhost:8080/rest/v1"), new ApplicationResourceConfig());


//        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, "/home/user/projects/gaffer/rest-api/src/test/resources/mockaccumulostore.properties");
//        System.setProperty(SystemProperty.SCHEMA_PATHS, "/home/user/projects/gaffer/rest-api/src/test/resources/example-schema.json");

        Logger l = Logger.getLogger("org.glassfish.grizzly.http.server.HttpHandler");
        l.setLevel(Level.FINE);
        l.setUseParentHandlers(false);
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.ALL);
        l.addHandler(ch);
    }

    @AfterClass
    public static void afterClass() {
        server.shutdownNow();
    }

    @Override
    @Before
    public void setup() throws Exception {
        assumeTrue("Skipping test as no store properties have been defined.", null != super.getStoreProperties());

        final String originalMethodName = name.getMethodName().endsWith("]")
                ? name.getMethodName()
                      .substring(0, name.getMethodName().indexOf("["))
                : name.getMethodName();
        final Method testMethod = this.getClass().getMethod(originalMethodName);
        final Collection<StoreTrait> requiredTraits = new ArrayList<>();

        for (final Annotation annotation : testMethod.getDeclaredAnnotations()) {
            if (annotation.annotationType().equals(TraitRequirement.class)) {
                final TraitRequirement traitRequirement = (TraitRequirement) annotation;
                requiredTraits.addAll(Arrays.asList(traitRequirement.value()));
            }
        }

        for (final StoreTrait requiredTrait : requiredTraits) {
            assumeTrue("Skipping test as the store does not implement all required traits.", graph
                    .hasTrait(requiredTrait));
        }

        addDefaultElements();
    }

    @Test
    public void shouldReturnOkStatusMessage() {
        // Given
        final Response response = client.target("http://localhost:8080/rest/v1")
                                        .path("status")
                                        .request()
                                        .get();

        // When
        final Map<String, String> statusMessage = response.readEntity(Map.class);

        System.out.println(statusMessage.get("description"));

        // Then
        assertEquals("The system is working normally.", statusMessage.get("description"));
    }

    @Test
    public void shouldRetrieveSchema() {
        // Given
        final Response response = client.target("http://localhost:8080/rest/v1")
                                        .path("graph/schema")
                                        .request()
                                        .get();

        // When
        final Schema schema = response.readEntity(Schema.class);

        System.out.println(schema);

        // Then
        assertNotNull(schema);
    }

    @Override
    public void addDefaultElements() throws OperationException {
        client.target("http://localhost:8080/rest/v1")
              .path("graph/addElements")
              .request()
              .put(Entity.json(new AddElements.Builder()
                      .elements((Iterable) getEntities().values())
                      .build()));

        client.target("http://localhost:8080/rest/v1")
              .path("graph/addElements")
              .request()
              .put(Entity.json(new AddElements.Builder()
                      .elements((Iterable) getEdges().values())
                      .build()));
>>>>>>> Stashed changes:integration-test/src/test/java/uk/gov/gchq/gaffer/integration/impl/RestApiIT.java
    }

}
