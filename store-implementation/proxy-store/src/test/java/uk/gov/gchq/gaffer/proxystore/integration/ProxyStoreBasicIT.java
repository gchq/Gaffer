/*
 * Copyright 2016-2020 Crown Copyright
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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
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
import uk.gov.gchq.gaffer.proxystore.integration.factory.TestGraphFactory;
import uk.gov.gchq.gaffer.rest.GafferWebApplication;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = GafferWebApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("proxy")
public class ProxyStoreBasicIT {
    private Graph graph;

    @Autowired
    private GraphFactory factory;

    @LocalServerPort
    private int localServerPort;

    public static final User USER = new User();
    public static final List<Element> DEFAULT_ELEMENTS = Lists.newArrayList(new Entity.Builder()
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
                .build());


    @BeforeEach
    public void before() throws IOException {
        // Reset Map store
        if (factory instanceof TestGraphFactory) {
            ((TestGraphFactory) factory).reset();
        } else {
            throw new RuntimeException("Expected Factory to be injected");
        }
        // setup ProxyStore
        graph = new Graph.Builder()
                .store(new ProxyStore.Builder()
                        .graphId("graph1")
                        .host("localhost")
                        .port(localServerPort)
                        .contextRoot("rest")
                        .build())
                .build();
    }

    @Test
    public void shouldAddElementsAndGetAllElements() throws Exception {
        // Given
        addDefaultElements();


        // When - Get
        final CloseableIterable<? extends Element> results = graph.execute(new GetAllElements(), USER);

        // Then
        assertEquals(DEFAULT_ELEMENTS.size(), Iterables.size(results));
        assertTrue(Lists.newArrayList(results).containsAll(DEFAULT_ELEMENTS));
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
        CloseableIterable<? extends Element> results = graph.execute(getElements, USER);

        // Then
        assertEquals(1, Iterables.size(results));
        assertTrue(Lists.newArrayList(results).contains(DEFAULT_ELEMENTS.get(0)));
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
        CloseableIterable<? extends Element> results = graph.execute(getElements, USER);

        // Then
        assertEquals(2, Iterables.size(results));
        assertTrue(Lists.newArrayList(results).contains(DEFAULT_ELEMENTS.get(0)));
        assertTrue(Lists.newArrayList(results).contains(DEFAULT_ELEMENTS.get(2)));
    }

    @Test
    public void shouldCatchAndThrowUsefulErrorMessages() throws Exception {
        // Given
        addDefaultElements();

        // When / Then

        GafferWrappedErrorRuntimeException e = assertThrows(GafferWrappedErrorRuntimeException.class, () -> {
            graph.execute(
                new OperationChain.Builder()
                    .first(new GetAllElements())
                    .then(new Limit<>(1, false))
                    .then(new ToList<>())
                    .build(), USER);
        });

        assertEquals(new Error.ErrorBuilder()
            .simpleMessage("Limit of 1 exceeded.")
            .status(Status.INTERNAL_SERVER_ERROR)
            .build(), e.getError());
    }

    @Test
    public void shouldHaveAllOfDelegateStoreTraitsApartFromVisibility() {
        // Given
        final Set<StoreTrait> expectedTraits = new HashSet<>(MapStore.TRAITS);
        expectedTraits.remove(StoreTrait.VISIBILITY);

        // When
        final Set<StoreTrait> storeTraits = graph.getStoreTraits();

        // Then
        assertEquals(expectedTraits, storeTraits);
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
                .option("Anything", "Value") //any value to create a optionsMap
                .build();
        graph.execute(add, USER);
    }
}
