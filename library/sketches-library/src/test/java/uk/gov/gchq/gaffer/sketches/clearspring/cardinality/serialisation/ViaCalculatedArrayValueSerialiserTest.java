/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class ViaCalculatedArrayValueSerialiserTest<OUTPUT, VALUE> extends ViaCalculatedValueSerialiserTest<OUTPUT, VALUE[]> {

    @Override
    protected void serialiseFirst(final Pair<OUTPUT, byte[]> pair) throws SerialisationException {
        final byte[] serialised = serialiser.serialise(pair.getFirst());
        final VALUE[] estimateFirstValue = useTestValue(pair.getFirst());
        final VALUE[] estimateFirstValueDeserialised = useTestValue(serialiser.deserialise(serialised));
        assertArrayEquals(estimateFirstValue, estimateFirstValueDeserialised);
        final VALUE[] estimateSecondValueDeserialised = useTestValue(serialiser.deserialise(pair.getSecond()));
        assertArrayEquals(estimateFirstValue, estimateSecondValueDeserialised);
    }

    @Override
    protected void testSerialiser(final OUTPUT object) {
        try {

            int countOfNullPointer = 0;

            VALUE[] originalValue;
            try {
                originalValue = useTestValue(object);
                countOfNullPointer++;
            } catch (final NullPointerException e) {
                originalValue = null;
            }

            VALUE[] deserialisedValue;
            try {
                final OUTPUT objectDeserialised = serialiser.deserialise(serialiser.serialise(object));
                deserialisedValue = useTestValue(objectDeserialised);
                countOfNullPointer++;
            } catch (final NullPointerException e) {
                deserialisedValue = null;
            }

            assertArrayEquals(originalValue, deserialisedValue);
            assertTrue("The values are equal, however one of them was assigned null via NullPointerException the other wasn't.", countOfNullPointer != 1);


        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
        }
    }
}
