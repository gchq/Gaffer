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
import uk.gov.gchq.gaffer.operation.export.Export;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;


public class GetJobResultsTest extends OperationTest<GetJobResults> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GetJobResults operation = new GetJobResults.Builder()
                .jobId("jobId")
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(operation, true);
        final GetJobResults deserialisedOp = JSONSerialiser.deserialise(json, GetJobResults.class);

        // Then
        assertEquals("jobId", deserialisedOp.getJobId());
    }

    @Test
    public void shouldReturnNullIfSetKey() {
        // When
        final GetJobResults jobResults = new GetJobResults.Builder()
                .key(Export.DEFAULT_KEY)
                .build();

        // Then
        assertThat(jobResults.getKey(), is(nullValue()));
    }


    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final GetJobResults op = new GetJobResults.Builder()
                .jobId("jobId")
                .build();

        // Then
        assertEquals("jobId", op.getJobId());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final GetJobResults getJobResults = new GetJobResults.Builder()
                .jobId("id1")
                .build();

        // When
        final GetJobResults clone = getJobResults.shallowClone();

        // Then
        assertNotSame(getJobResults, clone);
        assertNotNull(clone);
        assertEquals(getJobResults.getJobId(), clone.getJobId());
    }

    @Override
    protected GetJobResults getTestObject() {
        return new GetJobResults();
    }
}
