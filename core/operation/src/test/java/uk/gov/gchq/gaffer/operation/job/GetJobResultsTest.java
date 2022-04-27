/*
 * Copyright 2016-2020 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.export.Export;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;

import static org.assertj.core.api.Assertions.assertThat;

public class GetJobResultsTest extends OperationTest<GetJobResults> {

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GetJobResults operation = new GetJobResults.Builder()
                .jobId("jobId")
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(operation, true);
        final GetJobResults deserialisedOp = JSONSerialiser.deserialise(json, GetJobResults.class);

        // Then
        assertThat(deserialisedOp.getJobId()).isEqualTo("jobId");
    }

    @Test
    public void shouldReturnNullIfSetKey() {
        // When
        final GetJobResults jobResults = new GetJobResults.Builder()
                .key(Export.DEFAULT_KEY)
                .build();

        // Then
        assertThat(jobResults.getKey()).isNull();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final GetJobResults op = new GetJobResults.Builder()
                .jobId("jobId")
                .build();

        // Then
        assertThat(op.getJobId()).isEqualTo("jobId");
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final GetJobResults getJobResults = new GetJobResults.Builder()
                .jobId("id1")
                .build();

        // When
        final GetJobResults clone = getJobResults.shallowClone();

        // Then
        assertThat(clone)
                .isNotSameAs(getJobResults)
                .isNotNull();
        assertThat(clone.getJobId()).isEqualTo(getJobResults.getJobId());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertThat(outputClass).isEqualTo(Iterable.class);
    }

    @Override
    protected GetJobResults getTestObject() {
        return new GetJobResults();
    }
}
