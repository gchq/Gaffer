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

package uk.gov.gchq.gaffer.operation.job;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;

import static org.junit.Assert.assertEquals;


public class GetJobDetailsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetJobDetails operation = new GetJobDetails.Builder()
                .jobId("jobId")
                .build();

        // When
        byte[] json = serialiser.serialise(operation, true);
        final GetJobDetails deserialisedOp = serialiser.deserialise(json, GetJobDetails.class);

        // Then
        assertEquals("jobId", deserialisedOp.getJobId());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final GetJobDetails op = new GetJobDetails.Builder()
                .jobId("jobId")
                .build();

        // Then
        assertEquals("jobId", op.getJobId());
    }
}
