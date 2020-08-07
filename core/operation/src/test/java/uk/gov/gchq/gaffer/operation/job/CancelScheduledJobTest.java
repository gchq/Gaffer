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

package uk.gov.gchq.gaffer.operation.job;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.job.CancelScheduledJob;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class CancelScheduledJobTest extends OperationTest<CancelScheduledJob> {
    private final String testJobId = "testJobId";

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        CancelScheduledJob op = new CancelScheduledJob.Builder()
                .jobId(testJobId)
                .option("testOp", "testOpVal")
                .build();

        // Then
        assertEquals(testJobId, op.getJobId());
        assertEquals(Collections.singletonMap("testOp", "testOpVal"), op.getOptions());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        CancelScheduledJob op = new CancelScheduledJob.Builder()
                .jobId(testJobId)
                .option("testOp", "testOpVal")
                .build();

        // When
        CancelScheduledJob clonedOp = op.shallowClone();

        // Then
        assertNotSame(clonedOp, op);
        assertEquals(clonedOp.getJobId(), op.getJobId());
        assertEquals(clonedOp.getOptions(), op.getOptions());
    }

    @Override
    protected CancelScheduledJob getTestObject() {
        return new CancelScheduledJob();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Collections.singleton("jobId");
    }
}
