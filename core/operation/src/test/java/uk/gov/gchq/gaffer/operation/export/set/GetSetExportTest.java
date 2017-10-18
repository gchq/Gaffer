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

package uk.gov.gchq.gaffer.operation.export.set;

import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;


public class GetSetExportTest extends OperationTest<GetSetExport> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GetSetExport operation = new GetSetExport.Builder()
                .key("key")
                .jobId("jobId")
                .start(0)
                .end(5)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(operation, true);
        final GetSetExport deserialisedOp = JSONSerialiser.deserialise(json, GetSetExport.class);

        // Then
        assertEquals("key", deserialisedOp.getKey());
        assertEquals("jobId", deserialisedOp.getJobId());
        assertEquals(0, deserialisedOp.getStart());
        assertEquals(5, (int) deserialisedOp.getEnd());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final GetSetExport operation = new GetSetExport.Builder()
                .key("key")
                .jobId("jobId")
                .start(0)
                .end(5)
                .build();

        // Then
        assertEquals("key", operation.getKey());
        assertEquals("jobId", operation.getJobId());
        assertEquals(0, operation.getStart());
        assertEquals(5, (int) operation.getEnd());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String key = "key";
        final String jobId = "jobId";
        final int start = 0;
        final int end = 5;
        final GetSetExport getSetExport = new GetSetExport.Builder()
                .key(key)
                .jobId(jobId)
                .start(start)
                .end(end)
                .build();

        // When
        GetSetExport clone = getSetExport.shallowClone();

        // Then
        assertNotSame(getSetExport, clone);
        assertEquals(key, clone.getKey());
        assertEquals(jobId, clone.getJobId());
        assertEquals(start, clone.getStart());
        assertEquals(end, (int) clone.getEnd());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Override
    protected GetSetExport getTestObject() {
        return new GetSetExport();
    }
}
