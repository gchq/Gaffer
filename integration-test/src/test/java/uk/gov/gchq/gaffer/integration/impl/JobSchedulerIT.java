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

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.JsonToElementGenerator;
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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.verify.VerificationTimes.exactly;

public class JobSchedulerIT extends AbstractStoreIT {
    private static final int PORT = 40038;
    private static ClientAndServer mockServer = ClientAndServer.startClientAndServer(PORT);
    final String ENDPOINT_BASE_PATH = "http://127.0.0.1:";

    @AfterClass
    public static void tearDownServer() {
        if (null != mockServer) {
            mockServer.stop();
            mockServer = null;
        }
    }

    @Test
    public void shouldRunScheduledJob() throws Exception {
        // Given
        final String ENDPOINT_PATH = "/jsonEndpoint";
        final String endpointString = ENDPOINT_BASE_PATH + PORT + ENDPOINT_PATH;
        final Repeat repeat = new Repeat(0, 1, TimeUnit.SECONDS);

        final GetAsElementsFromEndpoint getAsElementsFromEndpoint = new GetAsElementsFromEndpoint.Builder()
                .endpoint(endpointString)
                .generator(JsonToElementGenerator.class)
                .build();

        HttpRequest request = request()
                .withMethod("GET")
                .withPath(ENDPOINT_PATH);

        mockServer.when(request)
                .respond(response()
                        .withStatusCode(200)
                        .withBody("[ ]"));

        // setup and schedule job
        final OperationChain opChain = new OperationChain.Builder()
                .first(getAsElementsFromEndpoint)
                .build();

        // When
        graph.executeJob(new Job(repeat, opChain), new Context(user));

        // sleep because the job is scheduled
        Thread.sleep(600);

        // Then - check mockServer request has been recieved once
        mockServer.verify(request, exactly(1));

        // sleep because the job is scheduled
        Thread.sleep(600);

        // Then - check mockServer request has been recieved twice now
        mockServer.verify(request, exactly(2));
    }

    @Test
    public void shouldRunJobAsNormalWithNullRepeat() throws Exception {
        // Given
        final String ENDPOINT_PATH = "/jsonEndpoint2";
        final String endpointString = ENDPOINT_BASE_PATH + PORT + ENDPOINT_PATH;

        final GetAsElementsFromEndpoint getAsElementsFromEndpoint = new GetAsElementsFromEndpoint.Builder()
                .endpoint(endpointString)
                .generator(JsonToElementGenerator.class)
                .build();

        final HttpRequest request = request()
                .withMethod("GET")
                .withPath(ENDPOINT_PATH);

        mockServer.when(request)
                .respond(response()
                        .withStatusCode(200)
                        .withBody("[ ]"));

        // setup and schedule job
        final OperationChain opChain = new OperationChain.Builder()
                .first(getAsElementsFromEndpoint)
                .build();

        // When
        JobDetail executingJobDetail = graph.executeJob(new Job(null, opChain), new Context(user));

        // Sleep because the job is scheduled
        Thread.sleep(300);

        // Then - check it has been run and added elements
        mockServer.verify(request, exactly(1));

        // Check the job has been FINISHED
        final CloseableIterable<JobDetail> jobDetails = graph.execute(new GetAllJobDetails(), new Context(user));
        for (final JobDetail jobDetail : jobDetails) {
            if (jobDetail.getJobId().equals(executingJobDetail.getJobId())) {
                assertTrue(jobDetail.getStatus().equals(JobStatus.FINISHED));
            }
        }

        // Check it has been run and added elements
        mockServer.verify(request, exactly(1));
    }

    @Test
    public void shouldCancelScheduledJob() throws Exception {
        // Given
        final Repeat repeat = new Repeat(0, 1, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new Limit.Builder<>().resultLimit(3).build())
                .build();

        final Job job = new Job(repeat, opChain);

        // When
        JobDetail parentJobDetail = graph.executeJob(job, new Context(user));

        // sleep because the job is scheduled
        Thread.sleep(200);

        // When
        graph.execute(new CancelScheduledJob.Builder().jobId(parentJobDetail.getJobId()).build(), new Context(user));

        // sleep because the job is scheduled
        Thread.sleep(300);

        parentJobDetail = graph.execute(new GetJobDetails.Builder().jobId(parentJobDetail.getJobId()).build(), new Context(user));

        // Then
        assertEquals(JobStatus.CANCELLED, parentJobDetail.getStatus());

        CloseableIterable<JobDetail> allJobs = graph.execute(new GetAllJobDetails.Builder().build(), new Context(user));

        // Then
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
        final Repeat repeat = new Repeat(0, 100, TimeUnit.SECONDS);
        final OperationChain opChain = new OperationChain.Builder()
                .first(new AddElements.Builder()
                        .input(inputEntity)
                        .build())
                .build();

        final Context context = new Context(user);

        // When
        JobDetail parentJobDetail = graph.executeJob(new Job(repeat, opChain), context);

        // Then
        assertEquals(JobStatus.SCHEDULED_PARENT, parentJobDetail.getStatus());

        // sleep because the job is scheduled
        Thread.sleep(1200);

        // When
        CloseableIterable<? extends Element> results = graph.execute(new GetAllElements(), new Context(user));

        // Then
        assertEquals(1, Lists.newArrayList(results).size());
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
