/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.job;

import org.junit.Test;

import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobTracker;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetJobDetailsHandlerTest {

    @Test
    public void shouldThrowExceptionIfJobTrackerIsNotConfigured() {
        // Given
        final GetJobDetailsHandler handler = new GetJobDetailsHandler();
        final GetJobDetails operation = mock(GetJobDetails.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        given(store.getJobTracker()).willReturn(null);

        // When / Then
        try {
            handler.doOperation(operation, new Context(user), store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldGetJobDetailsByDelegatingToJobTrackerWithOperationJobId() throws OperationException {
        // Given
        final String jobId = "jobId";
        final GetJobDetailsHandler handler = new GetJobDetailsHandler();
        final GetJobDetails operation = new GetJobDetails.Builder()
                .jobId(jobId)
                .build();
        final Store store = mock(Store.class);
        final JobTracker jobTracker = mock(JobTracker.class);
        final User user = mock(User.class);
        final JobDetail jobsDetail = mock(JobDetail.class);
        final Context context = new Context(user);

        given(store.getJobTracker()).willReturn(jobTracker);
        given(jobTracker.getJob(jobId, user)).willReturn(jobsDetail);

        // When
        final JobDetail result = handler.doOperation(operation, context, store);

        // Then
        assertSame(jobsDetail, result);
    }

    @Test
    public void shouldGetJobDetailsByDelegatingToJobTrackerWithContextJobId() throws OperationException {
        // Given
        final GetJobDetailsHandler handler = new GetJobDetailsHandler();
        final GetJobDetails operation = new GetJobDetails();
        final Store store = mock(Store.class);
        final JobTracker jobTracker = mock(JobTracker.class);
        final User user = mock(User.class);
        final JobDetail jobsDetail = mock(JobDetail.class);
        final Context context = new Context.Builder()
                .user(user)
                .build();
        final String jobId = context.getJobId();

        given(store.getJobTracker()).willReturn(jobTracker);
        given(jobTracker.getJob(jobId, user)).willReturn(jobsDetail);

        // When
        final JobDetail result = handler.doOperation(operation, context, store);

        // Then
        assertSame(jobsDetail, result);
    }
}
