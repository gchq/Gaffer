/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.time.serialisation;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RBMBackedTimestampSetSerialiserTest {
    private static final RBMBackedTimestampSetSerialiser RBM_BACKED_TIMESTAMP_SET_SERIALISER
            = new RBMBackedTimestampSetSerialiser();

    @Test
    public void testSerialiser() throws SerialisationException {
        // Given
        final RBMBackedTimestampSet rbmBackedTimestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        rbmBackedTimestampSet.add(Instant.ofEpochMilli(1000L));
        rbmBackedTimestampSet.add(Instant.ofEpochMilli(1000000L));

        // When
        final byte[] serialised = RBM_BACKED_TIMESTAMP_SET_SERIALISER.serialise(rbmBackedTimestampSet);
        final RBMBackedTimestampSet deserialised = RBM_BACKED_TIMESTAMP_SET_SERIALISER.deserialise(serialised);

        // Then
        assertEquals(rbmBackedTimestampSet, deserialised);
    }

    @Test
    public void testCanHandle() throws SerialisationException {
        assertTrue(RBM_BACKED_TIMESTAMP_SET_SERIALISER.canHandle(RBMBackedTimestampSet.class));
        assertFalse(RBM_BACKED_TIMESTAMP_SET_SERIALISER.canHandle(String.class));
    }
}
