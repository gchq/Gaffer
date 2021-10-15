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

import uk.gov.gchq.gaffer.jobtracker.JobTracker;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.job.CancelScheduledJob;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class CancelScheduledJobHandlerTest {

    @Test
    public void shouldThrowExceptionWithNoJobId() {
        // Given
        CancelScheduledJob operation = new CancelScheduledJob.Builder()
                .jobId(null)
                .build();
        CancelScheduledJobHandler handler = new CancelScheduledJobHandler();
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        given(store.getJobTracker()).willReturn(new JobTracker());

        // When / Then
        assertThatExceptionOfType(OperationException.class).isThrownBy(() -> handler.doOperation(operation, new Context(user), store)).withMessage("job id must be specified");
    }

    @Test
    public void shouldThrowExceptionIfJobTrackerIsNotConfigured() {
        // Given
        final CancelScheduledJobHandler handler = new CancelScheduledJobHandler();
        final CancelScheduledJob operation = mock(CancelScheduledJob.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        given(store.getJobTracker()).willReturn(null);

        // When / Then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> handler.doOperation(operation, new Context(user), store)).withMessage("JobTracker not enabled");
    }
}
