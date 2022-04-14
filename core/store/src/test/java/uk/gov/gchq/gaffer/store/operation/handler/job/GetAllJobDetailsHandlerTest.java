/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobTracker;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class GetAllJobDetailsHandlerTest {

    @Test
    public void shouldThrowExceptionIfJobTrackerIsNotConfigured(@Mock final Store store, @Mock final User user,
                                                                @Mock final GetAllJobDetails operation) {
        // Given
        final GetAllJobDetailsHandler handler = new GetAllJobDetailsHandler();

        given(store.getJobTracker()).willReturn(null);

        // When / Then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> handler.doOperation(operation, new Context(user), store)).extracting("message")
                .isNotNull();
    }

    @Test
    public void shouldGetAllJobDetailsByDelegatingToJobTracker(@Mock final Store store, @Mock final User user,
                                                               @Mock final GetAllJobDetails operation, @Mock final JobTracker jobTracker,
                                                               @Mock final Iterable<JobDetail> jobsDetails)
            throws OperationException {
        // Given
        final GetAllJobDetailsHandler handler = new GetAllJobDetailsHandler();

        given(store.getJobTracker()).willReturn(jobTracker);
        given(jobTracker.getAllJobs(user)).willReturn(jobsDetails);

        // When
        final Iterable<JobDetail> results = handler.doOperation(operation, new Context(user), store);

        // Then
        assertThat(results).isSameAs(jobsDetails);
    }
}
