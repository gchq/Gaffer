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
package uk.gov.gchq.gaffer.jobtracker.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JcsJobTrackerIT {
    private Store store;
    private Graph graph;

    @Before
    public void setup() throws Exception {
        final StoreProperties storeProps = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        store = Class.forName(storeProps.getStoreClass()).asSubclass(Store.class).newInstance();
        store.initialise(new Schema(), storeProps);
        graph = new Graph.Builder()
                .store(store)
                .build();
        clearJobTracker();
    }

    @After
    public void after() throws Exception {
        clearJobTracker();
    }

    @Test
    public void shouldAddJobIdToJobTrackerWhenExecuteJob() throws OperationException, IOException, InterruptedException {
        // Given
        final OperationChain<CloseableIterable<Edge>> opChain = new OperationChain<>(new GetAllEdges());
        final User user = new User("user01");

        // When
        JobDetail jobDetails = graph.executeJob(opChain, user);
        final String jobId = jobDetails.getJobId();
        final JobDetail expectedJobDetail = new JobDetail(jobId, user.getUserId(), opChain, JobStatus.FINISHED, null);
        expectedJobDetail.setStartTime(jobDetails.getStartTime());

        int count = 0;
        while (JobStatus.RUNNING == jobDetails.getStatus() && ++count < 20) {
            Thread.sleep(100);
            jobDetails = graph.execute(new GetJobDetails.Builder()
                    .jobId(jobId)
                    .build(), user);
        }
        expectedJobDetail.setEndTime(jobDetails.getEndTime());

        // Then
        assertEquals(expectedJobDetail, jobDetails);
    }

    @Test
    public void shouldAddJobIdToJobTrackerWhenExecute() throws OperationException, IOException, InterruptedException {
        // Given
        final OperationChain<JobDetail> opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new GetJobDetails())
                .build();
        final User user = new User("user01");

        // When
        final JobDetail jobDetails = graph.execute(opChain, user);
        final String jobId = jobDetails.getJobId();
        final JobDetail expectedJobDetail = new JobDetail(jobId, user.getUserId(), opChain, JobStatus.RUNNING, null);
        expectedJobDetail.setStartTime(jobDetails.getStartTime());
        expectedJobDetail.setEndTime(jobDetails.getEndTime());

        // Then
        assertEquals(expectedJobDetail, jobDetails);
    }

    private void clearJobTracker() throws Exception {
        if (null != store) {
            store.getJobTracker().clear();
        }
    }
}
