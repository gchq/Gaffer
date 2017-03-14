/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.jobtracker;


import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class JcsJobTrackerTest {
    private JcsJobTracker jobTracker;

    @Before
    public void setUp() throws Exception {
        jobTracker = new JcsJobTracker();
        jobTracker.initialise(null);
        jobTracker.clear();
    }

    @After
    public void teardown() throws Exception {
        jobTracker.clear();
    }

    @Test
    public void shouldAddAndGetJob() {
        // Given
        final User user = mock(User.class);
        final OperationChain<?> opChain = mock(OperationChain.class);
        given(opChain.toString()).willReturn("op chain to string");
        final JobDetail job = new JobDetail("jobId1", "userId1", opChain, JobStatus.RUNNING, "description");

        // When
        jobTracker.addOrUpdateJob(job, user);
        final JobDetail resultJob = jobTracker.getJob(job.getJobId(), user);

        // Then
        assertEquals(job, resultJob);
    }

    @Test
    public void shouldGetAllJobs() {
        // Given
        final User user = mock(User.class);
        final OperationChain<?> opChain = mock(OperationChain.class);
        given(opChain.toString()).willReturn("op chain to string");
        final JobDetail job1 = new JobDetail("jobId1", "userId1", opChain, JobStatus.RUNNING, "description");
        final JobDetail job2 = new JobDetail("jobId2", "userId2", opChain, JobStatus.RUNNING, "description");

        // When
        jobTracker.addOrUpdateJob(job1, user);
        jobTracker.addOrUpdateJob(job2, user);
        final List<JobDetail> jobDetails = Lists.newArrayList(jobTracker.getAllJobs(user));

        // Then
        final List<JobDetail> expectedJobDetails = Lists.newArrayList(job1, job2);
        final Comparator<JobDetail> jobDetailComparator = (o1, o2) -> o1.hashCode() - o2.hashCode();
        jobDetails.sort(jobDetailComparator);
        expectedJobDetails.sort(jobDetailComparator);
        assertEquals(expectedJobDetails, jobDetails);
    }

    @Test
    public void shouldOverwriteJob() {
        // Given
        final User user = mock(User.class);
        final OperationChain<?> opChain = mock(OperationChain.class);
        given(opChain.toString()).willReturn("op chain to string");
        final String jobId = "jobId1";
        final JobDetail job1 = new JobDetail(jobId, "userId1", opChain, JobStatus.RUNNING, "description1");
        final JobDetail job2 = new JobDetail(jobId, "userId1", opChain, JobStatus.FINISHED, "description2");

        // When
        jobTracker.addOrUpdateJob(job1, user);
        jobTracker.addOrUpdateJob(job2, user);

        // Then
        final JobDetail resultJob = jobTracker.getJob(jobId, user);
        assertEquals(job2, resultJob);
    }

    @Test
    public void shouldValidateJobDetailAndThrowExceptionIfNull() {
        // Given
        final User user = mock(User.class);
        final JobDetail job = null;

        // When / Then
        try {
            jobTracker.addOrUpdateJob(job, user);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldValidateJobDetailAndThrowExceptionIfMissingJobId() {
        // Given
        final User user = mock(User.class);
        final OperationChain<?> opChain = mock(OperationChain.class);
        given(opChain.toString()).willReturn("op chain to string");
        final JobDetail job = new JobDetail("", "userId1", opChain, JobStatus.RUNNING, "description");

        // When / Then
        try {
            jobTracker.addOrUpdateJob(job, user);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }
}
