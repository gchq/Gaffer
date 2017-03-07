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
import uk.gov.gchq.gaffer.operation.impl.export.GetExport;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;

import static org.junit.Assert.assertEquals;


public class GetSetExportTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();
    private static final String INVALID_KEY = "/invalidKey";

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetExport operation = new GetSetExport.Builder()
                .key("key")
                .jobId("jobId")
                .start(0)
                .end(5)
                .build();

        // When
        byte[] json = serialiser.serialise(operation, true);
        final GetSetExport deserialisedOp = serialiser.deserialise(json, GetSetExport.class);

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
}
