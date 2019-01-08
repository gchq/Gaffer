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

import org.junit.Test;
import org.mockserver.integration.ClientAndServer;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.JsonToElementGenerator;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.jobtracker.Job;
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
    final int port = 40034;
    ClientAndServer mockServer = ClientAndServer.startClientAndServer(port);

    @Test
    public void shouldRunScheduledJob() throws Exception {
        final String ENDPOINT_BASE_PATH = "http://127.0.0.1:";
        final String ENDPOINT_PATH = "/jsonEndpoint";
        final String endpointString = ENDPOINT_BASE_PATH + port + ENDPOINT_PATH;
        final Entity firstEndpointEntity = new Entity.Builder().group(TestGroups.ENTITY).vertex(VERTEX_PREFIXES[0]).build();
        final Entity secondEndpointEntity = new Entity.Builder().group(TestGroups.ENTITY).vertex(VERTEX_PREFIXES[1]).build();
        final Repeat repeat = new Repeat(0, 1, TimeUnit.SECONDS);

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

        graph.executeJob(new Job(repeat, opChain), new Context(user));

        // Check nothing in graph at the moment
        CloseableIterable<? extends Element> resultsAfterNoAdd = graph.execute(new GetAllElements(), new Context(user));
        assertFalse(resultsAfterNoAdd.iterator().hasNext());

        // sleep because the job is scheduled
        Thread.sleep(200);

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

        // sleep because the job is scheduled
        Thread.sleep(1800);

        // Check it has been run twice and now has all elements
        CloseableIterable<? extends Element> resultsAfterTwoAdd = graph.execute(new GetAllElements(), new Context(user));
        ElementUtil.assertElementEquals(Arrays.asList(firstEndpointEntity, secondEndpointEntity), resultsAfterTwoAdd);
    }

    @Test
    public void shouldRunJobAsNormalWithNullRepeat() throws Exception {
        final String ENDPOINT_BASE_PATH = "http://127.0.0.1:";
        final String ENDPOINT_PATH = "/jsonEndpoint2";
        final String endpointString = ENDPOINT_BASE_PATH + port + ENDPOINT_PATH;
        final Entity firstEndpointEntity = new Entity.Builder().group(TestGroups.ENTITY).vertex(VERTEX_PREFIXES[0]).build();

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

        JobDetail executingJobDetail = graph.executeJob(new Job(null, opChain), new Context(user));

        Thread.sleep(300);

        // Check it has been run and added elements
        CloseableIterable<? extends Element> resultsAfterOneAdd = graph.execute(new GetAllElements(), new Context(user));
        ElementUtil.assertElementEquals(Collections.singletonList(firstEndpointEntity), resultsAfterOneAdd);

        // Check the job has been FINISHED
        final CloseableIterable<JobDetail> jobDetails = graph.execute(new GetAllJobDetails(), new Context(user));
        for (final JobDetail jobDetail : jobDetails) {
            if (jobDetail.getJobId().equals(executingJobDetail.getJobId())) {
                assertTrue(jobDetail.getStatus().equals(JobStatus.FINISHED));
            }
        }

        mockServer.stop();
    }

    @Test
    public void shouldCancelScheduledJob() throws Exception {
        // Given
        final Entity inputEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex")
                .build();
        final Repeat repeat = new Repeat(0, 1, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new AddElements.Builder()
                        .input(inputEntity)
                        .build())
                .build();

        final Job job = new Job(repeat, opChain);

        // When
        JobDetail parentJobDetail = graph.executeJob(job, new Context(user));

        // sleep because the job is scheduled
        Thread.sleep(200);

        // When
        CloseableIterable<? extends Element> results = graph.execute(new GetAllElements(), new Context(user));

        // Then
        ElementUtil.assertElementEquals(Collections.singletonList(inputEntity), results);

        // When
        graph.execute(new CancelScheduledJob.Builder().jobId(parentJobDetail.getJobId()).build(), new Context(user));

        // sleep because the job is scheduled
        Thread.sleep(1800);

        parentJobDetail = graph.execute(new GetJobDetails.Builder().jobId(parentJobDetail.getJobId()).build(), new Context(user));

        assertEquals(JobStatus.CANCELLED, parentJobDetail.getStatus());

        CloseableIterable<JobDetail> allJobs = graph.execute(new GetAllJobDetails.Builder().build(), new Context(user));

        int childJobsRun = 0;
        for (JobDetail job2 : allJobs) {
            if (null != job2.getParentJobId() && job2.getParentJobId().equals(parentJobDetail.getJobId())) {
                childJobsRun += 1;
            }
        }
        assertEquals(1, childJobsRun);
    }

    @Test
    public void shouldRunSimpleScheduledJobThatRunsOnce() throws Exception {
        // Given
        final Entity inputEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex")
                .build();
        final Repeat repeat = new Repeat(1, 100, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new AddElements.Builder()
                        .input(inputEntity)
                        .build())
                .build();

        final Context context = new Context(user);

        // When
        JobDetail parentJobDetail = graph.executeJob(new Job(repeat, opChain), context);
        CloseableIterable<? extends Element> resultsAfterNoAdd = graph.execute(new GetAllElements(), new Context(user));

        // Then
        assertFalse(resultsAfterNoAdd.iterator().hasNext());
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());

        // sleep because the job is scheduled
        Thread.sleep(1200);

        // When
        CloseableIterable<? extends Element> results = graph.execute(new GetAllElements(), new Context(user));

        // Then
        ElementUtil.assertElementEquals(Collections.singletonList(inputEntity), results);
    }

    @Test
    public void shouldThrowExceptionOnIncorrectlyConfiguredJob() throws Exception {
        // Given
        final Repeat repeat = new Repeat(0, 100, TimeUnit.SECONDS);
        // Incorrectly configured Job
        final OperationChain opChain = new OperationChain.Builder().first(new Limit<>()).build();
        final Context context = new Context(user);

        // When
        JobDetail parentJobDetail = graph.executeJob(new Job(repeat, opChain), context);

        // Then
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());

        // sleep because the job is scheduled
        Thread.sleep(200);

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
        final Context context = new Context(user);

        // When
        JobDetail parentJobDetail = graph.executeJob(new Job(repeat, opChain), context);

        // Then
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());

        Thread.sleep(200);

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
