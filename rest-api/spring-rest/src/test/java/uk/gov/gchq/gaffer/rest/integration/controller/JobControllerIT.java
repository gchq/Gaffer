/*
 * Copyright 2019-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.integration.controller;

import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;

import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.Repeat;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.job.CancelScheduledJob;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.cache.util.CacheProperties.CACHE_SERVICE_CLASS;

public class JobControllerIT extends AbstractRestApiIT {

    @Autowired
    private GraphFactory graphFactory; // This will be a Mock (see application-test.properties)

    @Before
    public void setupGraph() {
        StoreProperties properties = new MapStoreProperties();
        properties.setJobTrackerEnabled(true);
        properties.set(CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());

        Graph graph = new Graph.Builder()
                .config(new GraphConfig("myGraph"))
                .storeProperties(properties)
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(graph);
    }

    @Test
    public void shouldCorrectlyDoAndThenCancelScheduledJob() throws IOException, InterruptedException {
        // When
        final Repeat repeat = new Repeat(1, 2, TimeUnit.SECONDS);
        Job job = new Job(repeat, new OperationChain.Builder().first(new GetAllElements()).build());
        final ResponseEntity<JobDetail> jobSchedulingResponse = post("/graph/jobs/schedule", job, JobDetail.class);

        JobDetail jobDetailParent = jobSchedulingResponse.getBody();

        // Then
        assertEquals(201, jobSchedulingResponse.getStatusCode().value());
        String parentJobId = jobDetailParent.getJobId();

        // Wait for first scheduled to run
        Thread.sleep(1500);

        final ResponseEntity<List> getAllJobDetailsResponse = post("/graph/operations/execute",
                new GetAllJobDetails(), List.class);

        Iterable<JobDetail> jobDetails = deserialiseJobDetailIterable(getAllJobDetailsResponse.getBody());

        for (JobDetail jobDetail : jobDetails) {
            if (null != jobDetail.getParentJobId() && jobDetail.getParentJobId().equals(parentJobId)) {
                assertEquals(JobStatus.FINISHED, jobDetail.getStatus());
            }
            if (jobDetail.getJobId().equals(parentJobId)) {
                assertEquals(JobStatus.SCHEDULED_PARENT, jobDetail.getStatus());
            }
        }

        post("/graph/operations/execute",
                new CancelScheduledJob.Builder().jobId(parentJobId).build(),
                Set.class);

        final Iterable<JobDetail> cancelledJobDetails = deserialiseJobDetailIterable(post("/graph/operations/execute",
                new GetAllJobDetails(),
                List.class).getBody());

        for (JobDetail jobDetail : cancelledJobDetails) {
            if (parentJobId.equals(jobDetail.getJobId())) {
                assertEquals(JobStatus.CANCELLED, jobDetail.getStatus());
            }
        }
    }

    private Iterable<JobDetail> deserialiseJobDetailIterable(final Iterable body) {
        try {
            return JSONSerialiser.deserialise(JSONSerialiser.serialise(body), new TypeReferenceImpl.JobDetailIterable());
        } catch (SerialisationException e) {
            throw new RuntimeException(e);
        }
    }
}
