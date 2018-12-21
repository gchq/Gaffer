/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.JsonToElementGenerator;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.Repeat;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAsElementsFromEndpoint;
import uk.gov.gchq.gaffer.operation.impl.job.CancelScheduledJob;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.store.Context;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class JobSchedulerIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Before
    public void checkNoElementsInGraph() throws Exception {
        CloseableIterable<? extends Element> results = graph.execute(new GetAllElements(), new Context());
        Assert.assertFalse(results.iterator().hasNext());
    }

    @Test
    public void shouldCancelScheduledJob() throws Exception {
        // Given
        final Entity inputEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex")
                .build();
        final Repeat repeat = new Repeat(2, 2, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new AddElements.Builder()
                        .input(inputEntity)
                        .build())
                .build();

        final Context context = new Context(user, repeat);

        // When
        JobDetail parentJobDetail = graph.executeJob(opChain, context);
        CloseableIterable<? extends Element> resultsAfterNoAdd = graph.execute(new GetAllElements(), new Context(user));

        // Then
        assertFalse(resultsAfterNoAdd.iterator().hasNext());
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());

        Thread.sleep(3000);

        // When
        CloseableIterable<? extends Element> results = graph.execute(new GetAllElements(), new Context(user));

        // Then
        ElementUtil.assertElementEquals(Collections.singletonList(inputEntity), results);

        // When
        graph.execute(new CancelScheduledJob.Builder().jobId(parentJobDetail.getJobId()).build(), new Context(user));

        Thread.sleep(2000);

        parentJobDetail = graph.execute(new GetJobDetails.Builder().jobId(parentJobDetail.getJobId()).build(), new Context(user));

        assertEquals(JobStatus.CANCELLED, parentJobDetail.getStatus());

        CloseableIterable<JobDetail> allJobs = graph.execute(new GetAllJobDetails.Builder().build(), new Context(user));

        int childJobsRun = 0;
        for (JobDetail job : allJobs) {
            if (null != job.getParentJobId() && job.getParentJobId().equals(parentJobDetail.getJobId())) {
                childJobsRun += 1;
            }
        }
        assertEquals(1, childJobsRun);
    }

    @Test
    public void shouldRunScheduledJob() throws Exception {
        final int port = 1080;
        ClientAndServer mockServer = ClientAndServer.startClientAndServer(port);
        final String ENDPOINT_BASE_PATH = "http://127.0.0.1:";
        final String ENDPOINT_PATH = "/jsonEndpoint";
        final String endpointString = ENDPOINT_BASE_PATH + port + ENDPOINT_PATH;
        final Entity firstEndpointEntity = new Entity.Builder().group(TestGroups.ENTITY).vertex(VERTEX_PREFIXES[0]).build();
        final Entity secondEndpointEntity = new Entity.Builder().group(TestGroups.ENTITY).vertex(VERTEX_PREFIXES[1]).build();
        final Repeat repeat = new Repeat(3, 3, TimeUnit.SECONDS);

        final GetAsElementsFromEndpoint getAsElementsFromEndpoint = new GetAsElementsFromEndpoint.Builder()
                .endpoint(endpointString)
                .generator(JsonToElementGenerator.class)
                .build();

        mockServer.when(request()
                .withMethod("GET")
                .withPath(ENDPOINT_PATH))
                .respond(response()
                        .withStatusCode(200)
                        .withBody("[\n" +
                                "  {\n" +
                                "    \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\",\n" +
                                "    \"group\": \"" + firstEndpointEntity.getGroup() + "\",\n" +
                                "    \"vertex\": \"" + firstEndpointEntity.getVertex() + "\"\n" +
                                "  }\n" +
                                "]"));

        // setup and schedule job
        final OperationChain opChain = new OperationChain.Builder().first(getAsElementsFromEndpoint).then(new AddElements()).build();
        graph.executeJob(opChain, new Context(user, repeat));

        // Check nothing in graph at the moment
        CloseableIterable<? extends Element> resultsAfterNoAdd = graph.execute(new GetAllElements(), new Context(user));
        assertFalse(resultsAfterNoAdd.iterator().hasNext());

        Thread.sleep(5000);

        // Check it has been run once with the first Elements
        CloseableIterable<? extends Element> resultsAfterOneAdd = graph.execute(new GetAllElements(), new Context(user));
        ElementUtil.assertElementEquals(Collections.singletonList(firstEndpointEntity), resultsAfterOneAdd);

        // Update the endpoint
        mockServer.when(request()
                .withMethod("GET")
                .withPath(ENDPOINT_PATH))
                .respond(response()
                        .withStatusCode(200)
                        .withBody("[\n" +
                                "  {\n" +
                                "    \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\",\n" +
                                "    \"group\": \"" + secondEndpointEntity.getGroup() + "\",\n" +
                                "    \"vertex\": \"" + secondEndpointEntity.getVertex() + "\"\n" +
                                "  }\n" +
                                "]"));

        // Check it has been run twice and now has all elements
        Thread.sleep(4000);
        CloseableIterable<? extends Element> resultsAfterTwoAdd = graph.execute(new GetAllElements(), new Context(user));
        ElementUtil.assertElementEquals(Arrays.asList(firstEndpointEntity, secondEndpointEntity), resultsAfterTwoAdd);
    }

    @Test
    public void shouldRunSimpleScheduledJobThatRunsOnce() throws Exception {
        // Given
        final Entity inputEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex")
                .build();
        final Repeat repeat = new Repeat(2, 100, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new AddElements.Builder()
                        .input(inputEntity)
                        .build())
                .build();

        final Context context = new Context(user, repeat);

        // When
        JobDetail parentJobDetail = graph.executeJob(opChain, context);
        CloseableIterable<? extends Element> resultsAfterNoAdd = graph.execute(new GetAllElements(), new Context(user));

        // Then
        assertFalse(resultsAfterNoAdd.iterator().hasNext());
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());

        // zzzzzzzzzzzzzzzz
        Thread.sleep(3000);

        // When
        CloseableIterable<? extends Element> results = graph.execute(new GetAllElements(), new Context(user));

        // Then
        ElementUtil.assertElementEquals(Collections.singletonList(inputEntity), results);
    }

    @Test
    public void shouldThrowExceptionOnIncorrectlyConfiguredJob() throws Exception {
        // Given
        final Repeat repeat = new Repeat(1, 30, TimeUnit.SECONDS);
        // Incorrectly configured Job
        final OperationChain opChain = new OperationChain.Builder().first(new Limit<>()).build();
        final Context context = new Context(user, repeat);

        // When
        JobDetail parentJobDetail = graph.executeJob(opChain, context);

        // Then
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());

        Thread.sleep(2000);

        // Given / When
        CloseableIterable<JobDetail> jobDetailList = graph.execute(new GetAllJobDetails(), context);

        // Then - assert the child job
        for (JobDetail resultJobDetail : jobDetailList) {
            if (null != resultJobDetail.getParentJobId() && resultJobDetail.getParentJobId().equals(parentJobDetail.getJobId())) {
                // assert the job status is failed
                assertTrue(resultJobDetail.getStatus().equals(JobStatus.FAILED));
            }
        }
    }

    @Test
    public void shouldSeeFinishedChildScheduledJob() throws Exception {
        // Given
        final Repeat repeat = new Repeat(0, 30, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain.Builder().first(new Limit<>(3)).build();
        final Context context = new Context(user, repeat);

        // When
        JobDetail parentJobDetail = graph.executeJob(opChain, context);

        // Then
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());

        Thread.sleep(2000);

        // Given / When
        CloseableIterable<JobDetail> jobDetailList = graph.execute(new GetAllJobDetails(), context);

        // Then - assert the child job
        for (JobDetail resultJobDetail : jobDetailList) {
            if (null != resultJobDetail.getParentJobId() && resultJobDetail.getParentJobId().equals(parentJobDetail.getJobId())) {
                // assert the job status is finished
                assertTrue(resultJobDetail.getStatus().equals(JobStatus.FINISHED));
                // assert the start time of the child is within 1 second after (within 50 ms) of the parents
                final long startTimeDifference = resultJobDetail.getStartTime() - parentJobDetail.getStartTime() - 1000;
                assertTrue((startTimeDifference < 50));
            }
        }
    }
}
