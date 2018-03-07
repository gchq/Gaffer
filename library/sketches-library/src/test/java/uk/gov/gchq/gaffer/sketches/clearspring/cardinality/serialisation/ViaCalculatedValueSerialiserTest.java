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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class ViaCalculatedValueSerialiserTest<OUTPUT, VALUE> extends ToBytesSerialisationTest<OUTPUT> {
    protected abstract OUTPUT getExampleOutput();

    protected abstract VALUE getTestValue(final OUTPUT object);

    @Test
    public void testSerialiseAndDeserialise() {
        testSerialiser(getExampleOutput());
        testSerialiser(getEmptyExampleOutput());
    }

    protected abstract OUTPUT getEmptyExampleOutput();

    protected VALUE useTestValue(final OUTPUT object) {
        try {
            return getTestValue(object);
        } catch (final NullPointerException e) {
            throw new NullPointerException("The supplied object to getTestValue() was not suitable.");
        }
    }

    @Override
    protected void deserialiseSecond(final Pair<OUTPUT, byte[]> pair) throws SerialisationException {
        //Nothing serialiseFirst does it all.
    }

    @Override
    protected void serialiseFirst(final Pair<OUTPUT, byte[]> pair) throws SerialisationException {
        final byte[] serialised = serialiser.serialise(pair.getFirst());
        final VALUE firstValue = useTestValue(pair.getFirst());
        final VALUE firstValueDeserialised = useTestValue(serialiser.deserialise(serialised));
        assertEquals(firstValue, firstValueDeserialised);
        final VALUE secondValueDeserialised = useTestValue(serialiser.deserialise(pair.getSecond()));
        assertEquals(firstValue, secondValueDeserialised);
    }

    protected void testSerialiser(final OUTPUT object) {
        try {
            VALUE originalValue = useTestValue(object);

            final OUTPUT objectDeserialised = serialiser.deserialise(serialiser.serialise(object));
            VALUE deserialisedValue = useTestValue(objectDeserialised);

            assertEquals(originalValue, deserialisedValue);
        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
        }
    }
}
