/*
 * Copyright 2017-2023 Crown Copyright
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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.core.exception.GafferWrappedErrorRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.proxystore.ProxyStore;
import uk.gov.gchq.gaffer.proxystore.SingleUseMapProxyStore;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.service.v2.RestApiV2TestClient;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProxyStoreBasicIT {

    private static final RestApiTestClient CLIENT = new RestApiV2TestClient();

    public static final User USER = new User();
    public static final Element[] DEFAULT_ELEMENTS = new Element[] {
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("1")
                    .property(TestPropertyNames.PROP_1, 1)
                    .property(TestPropertyNames.PROP_2, 2)
                    .property(TestPropertyNames.PROP_3, 3)
                    .property(TestPropertyNames.PROP_4, 4)
                    .property(TestPropertyNames.COUNT, 1)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("2")
                    .property(TestPropertyNames.PROP_1, 1)
                    .property(TestPropertyNames.PROP_2, 2)
                    .property(TestPropertyNames.PROP_3, 3)
                    .property(TestPropertyNames.PROP_4, 4)
                    .property(TestPropertyNames.COUNT, 1)
                    .build(),
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(TestPropertyNames.PROP_1, 1)
                    .property(TestPropertyNames.PROP_2, 2)
                    .property(TestPropertyNames.PROP_3, 3)
                    .property(TestPropertyNames.PROP_4, 4)
                    .property(TestPropertyNames.COUNT, 1)
                    .build()
    };

    private Store store;
    private Graph graph;

    @BeforeAll
    public static void beforeAll() throws Exception {
        SingleUseMapProxyStore.cleanUp();
        CLIENT.startServer();
    }

    @AfterAll
    public static void afterAll() {
        CLIENT.stopServer();
    }

    @BeforeEach
    public void before(@TempDir File testFolder) throws IOException {
        CLIENT.reinitialiseGraph(testFolder, StreamUtil.SCHEMA, "map-store.properties");

        // setup ProxyStore
        store = new ProxyStore.Builder()
                .graphId("graph1")
                .host("localhost")
                .port(8080)
                .contextRoot("rest/v2")
                .build();
        graph = new Graph.Builder()
                .store(store)
                .build();
    }

    @Test
    public void shouldAddElementsAndGetAllElements() throws Exception {
        // Given
        addDefaultElements();

        // When - Get
        final Iterable<? extends Element> results = graph.execute(new GetAllElements(), USER);

        // Then
        assertThat(results).hasSize(DEFAULT_ELEMENTS.length);
        assertThat(results).asInstanceOf(InstanceOfAssertFactories.iterable(Element.class)).contains(DEFAULT_ELEMENTS);
    }

    @Test
    public void shouldAddElementsAndGetRelatedElements() throws Exception {
        // Given
        addDefaultElements();

        // When
        final GetElements getElements = new GetElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .input(new EntitySeed("1"))
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, USER);

        // Then
        assertThat(results).hasSize(1);
        assertThat(results).asInstanceOf(InstanceOfAssertFactories.iterable(Element.class)).contains(DEFAULT_ELEMENTS[0]);
    }

    @Test
    public void shouldAddElementsViaAJob() throws Exception {
        // Add elements
        final AddElements add = new AddElements.Builder()
                .input(DEFAULT_ELEMENTS)
                .build();
        JobDetail jobDetail = graph.executeJob(new OperationChain<>(add), USER);

        // Wait until the job status is not RUNNING
        while (JobStatus.RUNNING.equals(jobDetail.getStatus())) {
            jobDetail = graph.execute(new GetJobDetails.Builder()
                    .jobId(jobDetail.getJobId())
                    .build(), USER);
            Thread.sleep(100);
        }

        // Get elements
        final GetElements getElements = new GetElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .edge(TestGroups.EDGE)
                        .build())
                .input(new EntitySeed("1"))
                .build();
        final Iterable<? extends Element> results = graph.execute(getElements, USER);

        // Then
        assertThat(results).hasSize(2);
        assertThat(results).asInstanceOf(InstanceOfAssertFactories.iterable(Element.class)).contains(DEFAULT_ELEMENTS[0], DEFAULT_ELEMENTS[2]);
    }

    @Test
    public void shouldCatchAndThrowUsefulErrorMessages() throws Exception {
        // Given
        addDefaultElements();

        // When / Then
        final GafferWrappedErrorRuntimeException actual = assertThrows(GafferWrappedErrorRuntimeException.class,
                () -> graph.execute(
                        new OperationChain.Builder()
                                .first(new GetAllElements())
                                .then(new Limit<>(1, false))
                                .then(new ToList<>())
                                .build(),
                        USER));
        assertThat(actual.getError()).isEqualTo(new Error.ErrorBuilder()
                .simpleMessage("Limit of 1 exceeded.")
                .status(Status.INTERNAL_SERVER_ERROR)
                .build());
    }

    @Test
    public void shouldHaveAllOfDelegateStoreTraitsApartFromVisibility() throws OperationException {
        // Given
        final Set<StoreTrait> expectedTraits = new HashSet<>(MapStore.TRAITS);
        expectedTraits.remove(StoreTrait.VISIBILITY);

        // When
        final Set<StoreTrait> storeTraits = graph.execute(new GetTraits.Builder().currentTraits(false).build(), new Context());

        // Then
        assertThat(storeTraits).isEqualTo(expectedTraits);
    }

    private void addDefaultElements() throws OperationException {
        final AddElements add = new AddElements.Builder()
                .input(DEFAULT_ELEMENTS)
                .build();
        graph.execute(add, USER);
    }

    @Test
    public void shouldNotErrorWithNonNullOptionsMapAndNullHandlerOption() throws Exception {
        final AddElements add = new AddElements.Builder()
                .input(DEFAULT_ELEMENTS)
                .option("Anything", "Value") // any value to create a optionsMap
                .build();
        graph.execute(add, USER);
    }

    @Test
    public void shouldGetOriginalSchemaUsingMethodsAndOperation() throws OperationException {
        // Given
        Schema storeSchema = Schema.fromJson(StreamUtil.openStream(this.getClass(), StreamUtil.SCHEMA));

        // When - Get
        final Schema returnedSchemaFromGraphMethod = graph.getSchema(); // Indirectly runs getOriginalSchema
        final Schema returnedSchemaFromStoreMethod = store.getOriginalSchema();
        final Schema returnedSchemaFromOperation = graph.execute(new GetSchema(), USER);

        // Then
        assertThat(returnedSchemaFromGraphMethod).isEqualTo(storeSchema);
        assertThat(returnedSchemaFromStoreMethod).isEqualTo(storeSchema);
        assertThat(returnedSchemaFromOperation).isEqualTo(storeSchema);
    }

    @Test
    public void shouldGetInternalOptimisedSchemaUsingMethodAndOperation() throws OperationException {
        // Given
        Schema storeSchema = Schema.fromJson(StreamUtil.openStream(this.getClass(), StreamUtil.SCHEMA));
        Schema optimisedStoreSchema = new SchemaOptimiser().optimise(storeSchema, true);

        // When - Get
        final Schema returnedSchemaFromMethod = store.getSchema();
        GetSchema getCompactSchema = new GetSchema.Builder().compact(true).build();
        final Schema returnedSchemaFromOperation = graph.execute(getCompactSchema, USER);

        // Then
        assertThat(returnedSchemaFromMethod).isEqualTo(optimisedStoreSchema);
        assertThat(returnedSchemaFromOperation).isEqualTo(optimisedStoreSchema);
    }
}
